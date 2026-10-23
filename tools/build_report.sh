#!/usr/bin/env bash
# Refresh the report's tables from the measurements and (if pandoc is installed) rebuild the PDF.
#   tools/build_report.sh [group_x group_y]        default group numbers 095 120
set -euo pipefail
cd "$(dirname "$0")/.."
X=${1:-095}; Y=${2:-120}
MD="FinalReport_Group_${X}_${Y}.md"; PDF="FinalReport_Group_${X}_${Y}.pdf"
[ -f "$MD" ] || { echo "missing $MD"; exit 1; }
[ -f Results/raw_metrics.csv ] || { echo "missing Results/raw_metrics.csv"; exit 1; }
python3 tools/report_tables.py --csv Results/raw_metrics.csv --report "$MD"
if ! command -v pandoc >/dev/null 2>&1; then
  echo "pandoc is not installed: the tables are updated in $MD. Convert it to PDF with any tool that can embed the PNGs"
  echo "in Results/ (or install pandoc + a LaTeX engine and re-run this script)."; exit 0
fi
build() { pandoc "$MD" -o "$PDF" --pdf-engine=xelatex --resource-path=. -V geometry:margin=1in -V fontsize=10pt -V colorlinks=true "$@"; }
if build 2>/tmp/build_report.err; then :;
else
  # minimal TeX installs lack lmodern.sty: give pandoc an empty stand-in and a font that is almost always present
  stub=$(mktemp -d); echo '\ProvidesPackage{lmodern}' > "$stub/lmodern.sty"
  TEXINPUTS="$stub:" build -V mainfont="DejaVu Serif" -V monofont="DejaVu Sans Mono"
fi
echo "built $PDF ($(pdfinfo "$PDF" 2>/dev/null | awk '/Pages/{print $2}') pages)"
