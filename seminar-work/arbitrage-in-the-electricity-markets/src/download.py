from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from re import search
from time import sleep
from typing import Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xlrd

BASE_URL = "https://docs.misoenergy.org/marketreports/"
RATES_URL = "https://cdn.misoenergy.org/Market%20Settlements%20Rates%20Factors101742.xls"

type Market = Literal["day_ahead", "real_time"]
type Component = Literal["LMP", "MCC"]
type Report = Literal["day_ahead", "real_time", "virtual_volume", "constraint_factor"]


class MISO:
    """Public MISO data downloads normalized into one Parquet file per call."""

    @staticmethod
    def _days(start: str | date, end: str | date) -> list[date]:
        """Build an inclusive range of operating dates.

        :param start: First date, as an ISO string or a date.
        :param end: Last date, as an ISO string or a date.
        :return: Consecutive dates; reversed ranges raise ValueError.
        """
        first = date.fromisoformat(start) if isinstance(start, str) else start
        last = date.fromisoformat(end) if isinstance(end, str) else end
        if first > last:
            raise ValueError("start must not be after end")
        return [first + timedelta(days=n) for n in range((last - first).days + 1)]

    @staticmethod
    def _fetch(url: str) -> bytes:
        """Read a public file into memory, retrying transient network failures.

        :param url: HTTPS report URL.
        :return: Response bytes; raw files are never written to disk.
        :raises HTTPError: The server rejects the request or the file is unavailable.
        """
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            },
        )
        for attempt in range(3):
            try:
                with urlopen(request, timeout=60) as response:
                    return bytes(response.read())
            except HTTPError as error:
                if error.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise
            except (URLError, TimeoutError, ConnectionError):
                if attempt == 2:
                    raise
            sleep(2**attempt)
        raise RuntimeError(f"Download failed: {url}")

    @staticmethod
    def _reports(days: list[date], kind: Report, archive_dir: Path | None = None) -> Iterator[tuple[date, bytes]]:
        """Yield daily reports from monthly archives or public daily files.

        :param days: Consecutive, nonempty operating dates.
        :param kind: DA/RT price, virtual-volume or constraint-factor report.
        :param archive_dir: Optional existing directory of MISO monthly archives.
            Archives there are read without modification. Downloads stay in memory.
        :return: Operating date and file bytes for every requested day.
        """
        months = sorted({day.replace(day=1) for day in days})
        for month in months:
            selected = [day for day in days if day.replace(day=1) == month]
            stems = {
                "day_ahead": "da_lmp" if month < date(2015, 3, 1) else "da_expost_lmp",
                "real_time": "rt_lmp_final",
                "virtual_volume": "da_pr",
                "constraint_factor": "ccf_co",
            }
            stem = stems[kind]
            extension = "xls" if kind == "virtual_volume" else "csv"
            name = f"{month:%Y%m}_{stem}_{extension}.zip"
            local = archive_dir / name if archive_dir is not None else None
            try:
                body = local.read_bytes() if local is not None and local.is_file() else MISO._fetch(BASE_URL + name)
            except HTTPError as error:
                if error.code != 404:
                    raise
                urls = [f"{BASE_URL}{day:%Y%m%d}_{stem}.{extension}" for day in selected]
                with ThreadPoolExecutor(max_workers=4) as pool:
                    yield from zip(selected, pool.map(MISO._fetch, urls), strict=True)
                continue
            with ZipFile(BytesIO(body)) as archive:
                members = {Path(member).name: member for member in archive.namelist()}
                for day in selected:
                    filename = f"{day:%Y%m%d}_{stem}.{extension}"
                    yield day, archive.read(members[filename])

    @staticmethod
    def _save(frames: Iterator[pd.DataFrame], output: str | Path) -> None:
        """Write normalized batches to one Parquet file without retaining earlier batches.

        :param frames: Nonempty iterator of dataframes with a consistent schema.
        :param output: New .parquet file in an existing directory.
        :return: None. An interrupted write is removed; existing files are protected.
        """
        path = Path(output)
        if path.suffix != ".parquet":
            raise ValueError("output must have the .parquet extension")
        first = pa.Table.from_pandas(next(frames), preserve_index=False)
        schema = first.schema
        if "node" in schema.names:
            types = schema.types
            types[schema.names.index("node")] = pa.dictionary(pa.int32(), pa.string())
            metadata: dict[bytes | str, bytes | str] = {key: value for key, value in (schema.metadata or {}).items()}
            schema = pa.schema(zip(schema.names, types, strict=True), metadata=metadata)
        with path.open("xb") as stream:
            try:
                with pq.ParquetWriter(stream, schema, compression="zstd") as writer:
                    writer.write_table(first.cast(schema))
                    for frame in frames:
                        table = pa.Table.from_pandas(frame, preserve_index=False)
                        writer.write_table(table.cast(schema))
            except BaseException:
                stream.close()
                path.unlink()
                raise

    @staticmethod
    def _excel(body: bytes, sheet_name: str | int = 0) -> pd.DataFrame:
        """Read an XLS sheet, converting Excel date cells to Python datetimes.

        :param body: In-memory XLS workbook.
        :param sheet_name: Worksheet name or zero-based position.
        :return: Cell values in a dataframe without an inferred header.
        """
        with xlrd.open_workbook(file_contents=body) as book:
            sheet = book.sheet_by_name(sheet_name) if isinstance(sheet_name, str) else book.sheet_by_index(sheet_name)
            rows = [
                [
                    xlrd.xldate_as_datetime(float(cell.value), book.datemode) if cell.ctype == xlrd.XL_CELL_DATE else cell.value
                    for cell in sheet.row(row)
                ]
                for row in range(sheet.nrows)
            ]
        return pd.DataFrame(rows)

    @staticmethod
    def _lmp(body: bytes, day: date, nodes: tuple[str, ...] | None, component: Component = "LMP") -> pd.DataFrame:
        """Normalize one MISO hourly LMP report, preserving negative prices.

        :param body: CSV bytes including the four-line MISO preamble.
        :param day: Expected operating date, checked against the report contents.
        :param nodes: Exact node names to retain, or None for every published node.
        :param component: LMP, or its marginal congestion component MCC.
        :return: timestamp, node and lmp_usd_mwh or mcc_usd_mwh; hour starts in
            fixed EST (UTC-05:00), without daylight-saving adjustments.
        """
        header = b"\n".join(body.splitlines()[:4])
        match = search(rb"\b(\d{1,2}/\d{1,2}/\d{4})\b", header)
        if match is None:
            raise ValueError("Missing operating date in the LMP report")
        reported = pd.Timestamp(match[1].decode()).date()
        if reported != day:
            raise ValueError(f"Expected {day}; report contains {reported}")
        raw = pd.read_csv(BytesIO(body), skiprows=4)
        raw.dropna(how="all", inplace=True)
        raw.columns = raw.columns.str.strip()
        hours = [f"HE {hour}" for hour in range(1, 25)]
        if "Value" in raw.columns:
            raw = raw.loc[raw["Value"].eq(component)]
        elif component != "LMP":
            raise ValueError(f"The report does not publish {component} on {day}")
        frame = raw[["Node", *hours]].copy()
        frame["Node"] = frame["Node"].str.strip()
        if nodes is not None:
            frame = frame.loc[frame["Node"].isin(nodes)]
            if set(frame["Node"]) != set(nodes):
                raise ValueError(f"Requested nodes are unavailable on {day}")
        if frame.empty or frame["Node"].duplicated().any():
            raise ValueError(f"Empty or duplicate LMP nodes on {day}")
        frame[hours] = frame[hours].astype(float)
        if frame.isna().any().any() or frame[hours].isin([float("inf"), float("-inf")]).any().any():
            raise ValueError(f"Missing or non-finite LMP values on {day}")
        value = f"{component.lower()}_usd_mwh"
        result = frame.melt(id_vars="Node", var_name="hour", value_name=value)
        offsets = result["hour"].str.removeprefix("HE ").astype(int) - 1
        result["timestamp"] = pd.Timestamp(day) + pd.to_timedelta(offsets, unit="h")
        result["timestamp"] = result["timestamp"].dt.tz_localize("Etc/GMT+5")
        result.rename(columns={"Node": "node"}, inplace=True)
        result["node"] = result["node"].astype("category")
        return result[["timestamp", "node", value]]

    @staticmethod
    def download_prices(
        start: str | date,
        end: str | date,
        market: Market,
        output: str | Path,
        *,
        nodes: tuple[str, ...] | None = None,
        component: Component = "LMP",
        archive_dir: Path | None = None,
    ) -> None:
        """Download hourly nodal prices and save only the three necessary columns.

        Historical DA prices use the pre-ELMP report; from March 2015 the source is
        DA ExPost. RT prices always use the final hourly report. Missing reports,
        nodes or prices raise an error instead of silently producing a partial file.
        Daily batches are appended to one file; RAM use does not grow with the period.

        :param start: First operating date, inclusive; date or YYYY-MM-DD.
        :param end: Last operating date, inclusive; date or YYYY-MM-DD.
        :param market: "day_ahead" or "real_time".
        :param output: New .parquet file in an existing directory.
        :param nodes: Exact MISO node names, or None to retain every node.
        :param component: LMP or MCC; MCC is the congestion price used by FTRs.
        :param archive_dir: Optional directory of existing monthly MISO archives.
        :return: None. Columns: timestamp (fixed EST), node and lmp_usd_mwh or mcc_usd_mwh.
        """
        if market not in {"day_ahead", "real_time"}:
            raise ValueError("market must be day_ahead or real_time")
        if component not in {"LMP", "MCC"}:
            raise ValueError("component must be LMP or MCC")
        if Path(output).exists():
            raise FileExistsError(output)
        frames = (MISO._lmp(body, day, nodes, component) for day, body in MISO._reports(MISO._days(start, end), market, archive_dir))
        MISO._save(frames, output)

    @staticmethod
    def _virtual_volume(body: bytes, day: date) -> dict[str, date | float]:
        """Extract daily virtual demand and supply totals by their Excel labels.

        :param body: Day-Ahead Pricing XLS report bytes.
        :param day: Expected operating date, checked against Market Date.
        :return: Date and two daily energy totals in MWh.
        """
        frame = MISO._excel(body)
        labels = frame.astype(str).map(str.strip)
        dates = labels.iloc[:, 0].loc[labels.iloc[:, 0].str.startswith("Market Date:")]
        if len(dates) != 1 or pd.Timestamp(dates.iloc[0].split(":", 1)[1].strip()).date() != day:
            raise ValueError(f"Market date mismatch in virtual volumes for {day}")
        result: dict[str, date | float] = {"date": day}
        for label, field in [("Demand Virtual", "virtual_demand_mwh"), ("Supply Virtual", "virtual_supply_mwh")]:
            rows, columns = (labels.to_numpy(dtype=str) == label).nonzero()
            if len(rows) != 1:
                raise ValueError(f"Missing or repeated {label} label on {day}")
            row, column = int(rows[0]), int(columns[0])
            if labels.iat[row + 1, 0] != "Energy Cleared (MWh)":
                raise ValueError(f"Unexpected virtual volume units on {day}")
            cell = frame.iat[row + 1, column]
            if not isinstance(cell, (int, float)):
                raise ValueError(f"Non-numeric virtual volume on {day}")
            value = float(cell)
            if not 0 <= value < float("inf"):
                raise ValueError(f"Invalid virtual volume on {day}")
            result[field] = value
        return result

    @staticmethod
    def download_virtual_volumes(start: str | date, end: str | date, output: str | Path, *, archive_dir: Path | None = None) -> None:
        """Download daily cleared virtual energy without downloading individual bids.

        :param start: First operating date, inclusive; date or YYYY-MM-DD.
        :param end: Last operating date, inclusive; date or YYYY-MM-DD.
        :param output: New .parquet file in an existing directory.
        :param archive_dir: Optional directory of existing Day-Ahead Pricing ZIPs.
        :return: None. The file contains date and daily virtual demand/supply in MWh.
        :raises ValueError: Requested reports omit virtual totals, as modern reports do.
        """
        if Path(output).exists():
            raise FileExistsError(output)
        records = [MISO._virtual_volume(body, day) for day, body in MISO._reports(MISO._days(start, end), "virtual_volume", archive_dir)]
        result = pd.DataFrame(records)
        result["date"] = pd.to_datetime(result["date"])
        MISO._save(iter([result]), output)

    @staticmethod
    def download_administrative_rates(start: str | date, end: str | date, output: str | Path) -> None:
        """Download historical administrative rates applicable to the date range.

        The workbook contains revised rates, not a history of what traders knew.
        RSG, congestion charges and capital costs are not included. The last change
        before start is retained so that the first requested day has a valid rate.

        :param start: First operating date, inclusive; date or YYYY-MM-DD.
        :param end: Last operating date, inclusive; date or YYYY-MM-DD.
        :param output: New .parquet file in an existing directory.
        :return: None. Columns: effective_date, schedule_17_usd_mwh,
            schedule_24_usd_mwh, transaction_usd_bid (per submitted hourly bid).
        """
        days = MISO._days(start, end)
        if Path(output).exists():
            raise FileExistsError(output)
        body = MISO._fetch(RATES_URL)
        fields = {
            "ENERGY_MKT_RATE": "schedule_17_usd_mwh",
            "SCHD_24_ALC_RATE": "schedule_24_usd_mwh",
            "ADMIN_TXN_RATE": "transaction_usd_bid",
        }
        parts: list[pd.DataFrame] = []
        for name in ["Pre_ASM_Rates", "Post_ASM_Rates"]:
            sheet = MISO._excel(body, name)
            selected = sheet.loc[sheet[1].isin(fields)].set_index(1).iloc[:, 1:].T
            selected = selected.rename(columns=fields).astype(float)
            selected["effective_date"] = pd.to_datetime(sheet.iloc[5, 2:].to_numpy())
            parts.append(selected)
        rates = pd.concat(parts, ignore_index=True).sort_values("effective_date")
        rates = rates.drop_duplicates("effective_date", keep="last")
        if pd.Timestamp(days[0]) < rates["effective_date"].min() or pd.Timestamp(days[-1]).to_period("M") > rates[
            "effective_date"
        ].max().to_period("M"):
            raise ValueError("Requested dates extend beyond published administrative rates")
        before = rates.loc[rates["effective_date"].le(pd.Timestamp(days[0]))].tail(1)
        within = rates.loc[rates["effective_date"].between(pd.Timestamp(days[0]), pd.Timestamp(days[-1]), inclusive="right")]
        result = pd.concat([before, within], ignore_index=True)
        if result.isna().any().any():
            raise ValueError("Missing administrative rates")
        MISO._save(iter([result[["effective_date", *fields.values()]]]), output)

    @staticmethod
    def download_rsg_rates(start: str | date, end: str | date, output: str | Path, *, publication: date) -> None:
        """Download hourly DDC and active-constraint CMC settlement rates.

        The publication contains revised observations for a rolling history.
        Requested DDC hours must be complete. Blank CMC cells are inactive hours;
        only nonzero CMC rates are retained. CMC rates still need node-specific
        contribution factors before they can be applied to virtual positions.

        :param start: First operating date, inclusive.
        :param end: Last operating date, inclusive.
        :param output: New .parquet path in an existing directory.
        :param publication: Publication date of the SRW RSG workbook.
        :return: None. Columns: timestamp, constraint, rate_type, rate_usd_mwh.
        """
        days = MISO._days(start, end)
        if Path(output).exists():
            raise FileExistsError(output)
        body = MISO._fetch(f"{BASE_URL}{publication:%Y%m%d}_ms_rsg_srw.xlsx")
        parts: list[pd.DataFrame] = []
        for sheet, label in [("MISO DDC rate", "MISO_DDC_RATE"), ("ATC CMC rate", "ATC_CMC_RATE")]:
            with pd.ExcelFile(BytesIO(body), engine="openpyxl") as workbook:
                raw = workbook.parse(sheet, header=1)
            raw = raw.loc[raw["BILL_DETERMINANT"].eq(label)].copy()
            raw.rename(columns={"OPERATING DATE": "date", "CONSTRAINT NAME": "constraint"}, inplace=True)
            if "constraint" not in raw.columns:
                raw["constraint"] = ""
            raw = raw.loc[raw["date"].between(pd.Timestamp(days[0]), pd.Timestamp(days[-1]))]
            hours = [f"HE{hour}" for hour in range(1, 25)]
            frame = raw.melt(id_vars=["date", "constraint"], value_vars=hours, var_name="hour", value_name="rate_usd_mwh")
            offsets = frame["hour"].str.removeprefix("HE").astype(int) - 1
            frame["timestamp"] = (frame["date"] + pd.to_timedelta(offsets, unit="h")).dt.tz_localize("Etc/GMT+5")
            frame["constraint"] = frame["constraint"].str.strip()
            frame["rate_type"] = "ddc" if label == "MISO_DDC_RATE" else "cmc"
            if label == "ATC_CMC_RATE":
                frame = frame.loc[frame["rate_usd_mwh"].notna() & frame["rate_usd_mwh"].ne(0)]
            parts.append(frame[["timestamp", "constraint", "rate_type", "rate_usd_mwh"]])
        result = pd.concat(parts, ignore_index=True).sort_values(["timestamp", "rate_type", "constraint"])
        ddc = result.loc[result["rate_type"].eq("ddc")]
        expected = pd.date_range(days[0], periods=24 * len(days), freq="h", tz="Etc/GMT+5")
        if not pd.DatetimeIndex(ddc["timestamp"]).equals(expected) or result.isna().any().any():
            raise ValueError("The publication does not contain complete requested RSG rates")
        if result.duplicated(["timestamp", "constraint", "rate_type"]).any():
            raise ValueError("Duplicate RSG rate observations")
        MISO._save(iter([result]), output)

    @staticmethod
    def _constraint_factors(body: bytes, day: date) -> pd.DataFrame:
        """Normalize nonzero node-constraint contribution factors for one day.

        :param body: Public CCF CSV bytes with its four-line preamble.
        :param day: Expected operating date.
        :return: timestamp, constraint, node and dimensionless factor.
        """
        raw = pd.read_csv(BytesIO(body), skiprows=4)
        raw.dropna(how="all", inplace=True)
        raw.columns = raw.columns.str.strip()
        raw = raw.loc[raw["NODE NAME"].notna()]
        raw = raw.loc[~raw["OPERATING DATE"].astype(str).str.startswith("MISO ")]
        dates = pd.to_datetime(raw["OPERATING DATE"])
        if not dates.eq(pd.Timestamp(day)).all():
            raise ValueError(f"Missing or mismatched constraint factors on {day}")
        if raw.empty:
            match = search(rb"Market Date:\s*(\d{1,2}/\d{1,2}/\d{4})", body)
            if match is None or pd.Timestamp(match[1].decode()).date() != day:
                raise ValueError(f"Missing market date in the empty CCF report for {day}")
        raw.rename(columns={"CONSTRAINT NAME": "constraint", "NODE NAME": "node"}, inplace=True)
        hours = [f"HOUR{hour}" for hour in range(1, 25)]
        frame = raw.melt(id_vars=["constraint", "node"], value_vars=hours, var_name="hour", value_name="factor")
        frame["factor"] = frame["factor"].astype(float)
        if frame["factor"].isna().any() or frame["factor"].abs().gt(1).any():
            raise ValueError(f"Invalid constraint factors on {day}")
        frame = frame.loc[frame["factor"].ne(0)].copy()
        frame["constraint"] = frame["constraint"].str.strip().astype("string")
        frame["node"] = frame["node"].str.strip().astype("category")
        offsets = frame["hour"].str.removeprefix("HOUR").astype(int) - 1
        frame["timestamp"] = (pd.Timestamp(day) + pd.to_timedelta(offsets, unit="h")).dt.tz_localize("Etc/GMT+5")
        return frame[["timestamp", "constraint", "node", "factor"]]

    @staticmethod
    def download_constraint_factors(
        start: str | date, end: str | date, output: str | Path, *, operating_days: tuple[date, ...] | None = None
    ) -> None:
        """Download CCF observations into one Parquet without retaining raw CSVs.

        :param start: First operating date, inclusive.
        :param end: Last operating date, inclusive.
        :param output: New .parquet path in an existing directory.
        :param operating_days: Optional subset, such as days with nonzero CMC rates.
        :return: None. Columns: timestamp, constraint, node, factor; zero factors omitted.
        """
        days = MISO._days(start, end)
        if Path(output).exists():
            raise FileExistsError(output)
        if operating_days is not None:
            if not operating_days or not set(operating_days).issubset(days):
                raise ValueError("operating_days must be a nonempty subset of the period")
            days = sorted(set(operating_days))
        frames = (MISO._constraint_factors(body, day) for day, body in MISO._reports(days, "constraint_factor"))
        MISO._save(frames, output)

    @staticmethod
    def _ftr_awards(body: bytes, start: date, end: date, nodes: tuple[str, ...] | None) -> pd.DataFrame:
        """Select purchased point-to-point FTR obligations from an auction CSV.

        :param body: MarketResults CSV bytes.
        :param start: First date intersecting a retained contract's delivery term.
        :param end: Last date intersecting a retained contract's delivery term.
        :param nodes: Optional allowed source and sink nodes, matched exactly.
        :return: Contract identifier, participant, path, term, class, MW and price.
        """
        raw = pd.read_csv(BytesIO(body))
        raw = raw.loc[raw["Category"].eq("PTP") & raw["HedgeType"].eq("OBL") & raw["Type"].eq("BUY")]
        fields = {
            "FTRID": "ftr_id",
            "MarketParticipant": "participant",
            "Source": "source",
            "Sink": "sink",
            "StartDate": "start_date",
            "EndDate": "end_date",
            "Class": "class",
            "MW": "mw",
            "ClearingPrice": "auction_price_usd_mw",
        }
        frame = raw[list(fields)].rename(columns=fields)
        for column in ["start_date", "end_date"]:
            frame[column] = pd.to_datetime(frame[column], format="%m/%d/%Y")
        frame = frame.loc[frame["start_date"].le(pd.Timestamp(end)) & frame["end_date"].ge(pd.Timestamp(start))]
        if nodes is not None:
            frame = frame.loc[frame["source"].isin(nodes) & frame["sink"].isin(nodes)]
        if frame.isna().any().any() or frame["mw"].le(0).any():
            raise ValueError("Missing FTR fields or nonpositive awarded quantities")
        if not frame["class"].isin(["Peak", "Off-peak"]).all():
            raise ValueError("Unknown FTR time-of-use class")
        return frame.reset_index(drop=True)

    @staticmethod
    def download_ftr_awards(
        start: str | date,
        end: str | date,
        output: str | Path,
        *,
        auctions: tuple[date, ...],
        nodes: tuple[str, ...] | None = None,
        round_number: int = 1,
    ) -> None:
        """Download annual-auction FTR obligations with overlapping delivery terms.

        Each retained row is a purchased point-to-point obligation. The clearing
        price is USD per MW for the entire contract term, not USD/MWh. Negative
        auction prices are valid. Original delivery dates are never truncated.

        :param start: First delivery date, inclusive.
        :param end: Last delivery date, inclusive.
        :param output: New .parquet path in an existing directory.
        :param auctions: Publication dates used in MISO annual-results filenames.
        :param nodes: Optional exact node names; both endpoints must be included.
        :param round_number: Annual auction round, 1, 2 or 3.
        :return: None. One file of awards, with auction date and round identifiers.
        """
        days = MISO._days(start, end)
        if Path(output).exists():
            raise FileExistsError(output)
        if not auctions or round_number not in {1, 2, 3}:
            raise ValueError("Provide auction dates and a round from 1 to 3")
        parts: list[pd.DataFrame] = []
        for auction in sorted(set(auctions)):
            url = f"{BASE_URL}{auction:%Y%m%d}_ftr_annual_results_round_{round_number}.zip"
            with ZipFile(BytesIO(MISO._fetch(url))) as archive:
                members = [n for n in archive.namelist() if Path(n).name.startswith("MarketResults_") and n.endswith(".csv")]
                if not members:
                    raise ValueError(f"No supported MarketResults CSVs in {url}")
                for name in members:
                    frame = MISO._ftr_awards(archive.read(name), days[0], days[-1], nodes)
                    frame["auction_date"] = pd.Timestamp(auction)
                    frame["round"] = round_number
                    parts.append(frame)
        result = pd.concat(parts, ignore_index=True)
        if result.empty or result.duplicated(["ftr_id", "auction_date", "round"]).any():
            raise ValueError("No matching awards or duplicate FTR identifiers")
        MISO._save(iter([result]), output)
