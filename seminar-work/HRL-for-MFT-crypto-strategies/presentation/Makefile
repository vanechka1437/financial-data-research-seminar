XELATEX  ?= xelatex
BUILD    := .latex-build
ARTICLES := ru.pdf en.pdf
FLAGS    := -interaction=nonstopmode -halt-on-error -file-line-error

.PHONY: all slides articles

all: articles

# The final presentation PDF is retained without its build sources.
slides: macrohft_hse.pdf

articles: $(ARTICLES)

ifeq ($(OS),Windows_NT)
SHELL := cmd.exe
.SHELLFLAGS := /C

$(ARTICLES): %.pdf: %.tex
	@if not exist "$(BUILD)\$*" mkdir "$(BUILD)\$*"
	$(XELATEX) $(FLAGS) -output-directory="$(BUILD)\$*" "$<"
	$(XELATEX) $(FLAGS) -output-directory="$(BUILD)\$*" "$<"
	@copy /Y "$(BUILD)\$*\$@" "$@" >NUL
else
$(ARTICLES): %.pdf: %.tex
	@mkdir -p "$(BUILD)/$*"
	$(XELATEX) $(FLAGS) -output-directory="$(BUILD)/$*" "$<"
	$(XELATEX) $(FLAGS) -output-directory="$(BUILD)/$*" "$<"
	@cp "$(BUILD)/$*/$@" "$@"
endif
