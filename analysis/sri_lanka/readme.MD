Here’s a concise README you can drop at the repo root.

---

# Sri Lanka WER Processing

Build a reproducible dataset from Sri Lanka’s **Weekly Epidemiological Reports (WERs)**: extract weekly district disease counts (e.g., dengue, leptospirosis), link to population, station weather, land cover, and ERA5, and output **analysis-ready panels** (plus optional figures).

## What to run

* **Full pipeline** (crawl + extract + join):

  1. `analysis/sri_lanka/initial_processing.R`
  2. `analysis/sri_lanka/post_processing.R`

* **Just explore data quickly** (skip crawling/extraction):
  Run **`analysis/sri_lanka/post_processing.R`** directly after cloning.
  It reads the pre-extracted outputs in `analysis/sri_lanka/outputs/`.

## Requirements

R packages: `data.table`, `stringr`, `lubridate`, `pdftools`, `xml2`, `rvest`,
`sf`, `terra`, `exactextractr`, `ggplot2`, `scales`, `ragg`
Optional (only if parsing population PDF): `tabulizer`, `tabulizerjars`, working **Java (JDK)**.

Environment variables (recommended via `~/.Renviron`):

```
CHI_LOCAL_WORK="C:/path/to/workdir"              # temp/outputs
CHI_GITHUB_ROOT="C:/path/to/CHI-Data"            # repo root
# Optional: bypass Java by providing a tidy population CSV
CHI_POP_CSV=".../analysis/sri_lanka/inputs/population_by_district_in_census_years.csv"
# Windows only if needed:
# JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot"
```

## Outputs

* `analysis/sri_lanka/outputs/sri_lanka_WER_index_of_pdfs.csv` (WER index)
* `analysis/sri_lanka/outputs/disease_counts_v4.txt` (extracted counts)
* `outputs/lep_analysis_panel.csv` (final merged panel)
* Figures (if generated) → `analysis/sri_lanka/outputs/figures/`

## Quick start

```r
# from R
source("analysis/sri_lanka/post_processing.R")
# opens & merges pre-extracted outputs so you can explore immediately
```

---

## Project structure (key paths)

```text
CHI-Data/
├─ analysis/
│  ├─ madagascar/                      # separate project; leave as-is
│  └─ sri_lanka/
│     ├─ initial_processing.R          # 1) crawl WER PDFs, extract counts
│     ├─ post_processing.R             # 2) join pop/weather/landcover/ERA5 → panel
│     ├─ helpers.R                     # parsing, normalization, disease maps
│     ├─ inputs/
│     │  ├─ Mid-year_population_by_district_and_sex_2024.pdf
│     │  ├─ population_by_district_in_census_years.csv   # (CSV fallback for pop)
│     │  └─ station_data/
│     │     ├─ SriLanka_Weather_Dataset.csv
│     │     ├─ SriLanka_Weather_Dataset_V1.csv
│     │     └─ readme.txt
│     ├─ outputs/
│     │  ├─ disease_counts_v4.txt
│     │  ├─ sri_lanka_WER_index_of_pdfs.csv
│     │  ├─ srilanka_district_daily_era5_areawt.csv
│     │  ├─ srilanka_district_weekly_era5_areawt.csv
│     │  └─ figures/
│     └─ SriLanka_Landcover_2018/
│        └─ SriLanka_Landcover_2018.tif
├─ helpers/
│  ├─ helpers.R                         # shared utilities
│  └─ plot_helpers.R
├─ data/
│  └─ readme.MD
├─ docs/
│  └─ sri_lanka/
│     └─ readme.MD.txt
```

---

### Notes

* If `tabulizer`/Java is painful on your machine, set `CHI_POP_CSV` to use the provided population CSV and run `post_processing.R`.
* District naming is normalized internally; if you add new sources, map any novel aliases in `analysis/sri_lanka/helpers.R`.

If you want this README auto-copied to `analysis/sri_lanka/readme.MD` with a shorter, subfolder-specific version, I can draft that too.
