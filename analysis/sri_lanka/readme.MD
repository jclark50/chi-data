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



# README — Sri Lanka WER ▸ Climate–Health Pipeline

## Overview

This repo builds an analysis-ready dataset linking Sri Lanka’s Weekly Epidemiological Reports (WER) to gridded climate covariates. It:

1. Indexes and parses WER PDFs to extract district-level weekly cases.
2. Joins mid-year population to compute per-capita rates.
3. Maps daily weather station data and ERA5 reanalysis to districts.
4. Adds land cover proportions by district.
5. Produces both **daily** and **weekly** climate features aligned to epidemiological weeks for modeling.

> If you only want to explore/plot the prepared data, you can start directly at `post_processing.R` after cloning.

## Quick start

### Prereqs

* R ≥ 4.3 with packages: `data.table`, `stringr`, `lubridate`, `rvest`, `xml2`, `pdftools`, `sf`, `terra`, `exactextractr`, `arrow`, `ggplot2`, `scales`, `ragg`.
* For optional PDF table extraction from the population PDF: `tabulapdf` + `tabulizerjars` (requires Java + `rJava`).
* GDAL/GEOS/PROJ available for `sf/terra` (on macOS: `brew install gdal`).

### Environment variables (recommended)

Add to `~/.Renviron` (or set in the session):

```
CHI_LOCAL_WORK="C:/Users/<you>/Desktop/srilanka"
CHI_GITHUB_ROOT="C:/Users/<you>/R_Projects/CHI-Data"
JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot"  # if using tabulizer
```

### Run the pipeline

* Full ingest → extract → enrich:

  1. `analysis/sri_lanka/initial_processing.R`

     * Crawls WER site, indexes PDFs, extracts weekly district counts, saves:

       * `analysis/sri_lanka/outputs/sri_lanka_WER_index_of_pdfs.csv`
       * `analysis/sri_lanka/outputs/disease_counts_v4.txt`
* Ready-to-analyze enrichment (can be run standalone):
  2\. `analysis/sri_lanka/post_processing.R`

  * Reads case counts, ingests population PDF, adds weather station aggregates, land cover proportions, and joins **daily** and **weekly** ERA5 features.
  * Outputs:

    * `analysis/sri_lanka/outputs/figures/*.png` (if you enable plotting)
    * `analysis/sri_lanka/outputs/srilanka_district_daily_era5_areawt.csv`
    * `analysis/sri_lanka/outputs/srilanka_district_weekly_era5_areawt.csv`
    * `analysis/sri_lanka/outputs/lep_analysis_panel.csv` (health + features)

> **Shortcut for exploration:** clone the repo and run `post_processing.R`. It reads previously created WER outputs (or bundled inputs), builds features, and writes analysis files for quick modeling.

### Inputs (under `analysis/sri_lanka/inputs/`)

* `Mid-year_population_by_district_and_sex_2024.pdf`
* Weather station CSVs under `inputs/station_data/`
* Land cover GeoTIFF `SriLanka_Landcover_2018.tif`
* ERA5 daily/district file produced by the ERA5 pipeline (`srilanka_district_daily_era5_areawt.csv`) or created by the included ERA5 scripts.

### Typical outputs (under `analysis/sri_lanka/outputs/`)

* `sri_lanka_WER_index_of_pdfs.csv` — catalog of WER PDFs with inferred week dates.
* `disease_counts_v4.txt` — long table of (district × week × disease) counts.
* `srilanka_district_daily_era5_areawt.csv` — district×day, area-weighted daily climate metrics.
* `srilanka_district_weekly_era5_areawt.csv` — district×epi-week climate features (sums, means, lags, rolling, anomalies).
* Optional figures: national trend, top districts, seasonality, heatmaps.

### Troubleshooting

* **Java/rJava/tabulizer**: If you see “JAVA\_HOME cannot be determined…”, confirm a JDK is installed and `JAVA_HOME` points to it (not to JRE). On Windows, install Adoptium Temurin (17+), then restart R/RStudio.
* **sf/terra CRS issues**: Make sure GDAL/GEOS/PROJ are installed; verify `st_crs()` is defined and transformations succeed.

---

## Project structure (key items)

```
CHI-Data/
├─ analysis/
│  ├─ sri_lanka/
│  │  ├─ inputs/
│  │  │  ├─ Mid-year_population_by_district_and_sex_2024.pdf
│  │  │  ├─ population_by_district_in_census_years.csv
│  │  │  └─ station_data/
│  │  │     ├─ SriLanka_Weather_Dataset.csv
│  │  │     └─ archive.zip, readme.txt, ...
│  │  ├─ outputs/
│  │  │  ├─ figures/
│  │  │  │  ├─ P1_national_trend.png
│  │  │  │  ├─ P2_top15_latest_week.png
│  │  │  │  ├─ P3_top6_last52_facets.png
│  │  │  │  ├─ P4_seasonality_boxplots.png
│  │  │  │  └─ P6_yoy_change_top15.png
│  │  │  ├─ disease_counts_v4.txt
│  │  │  ├─ sri_lanka_WER_index_of_pdfs.csv
│  │  │  ├─ srilanka_district_daily_era5_areawt.csv
│  │  │  └─ srilanka_district_weekly_era5_areawt.csv
│  │  ├─ helpers.R
│  │  ├─ initial_processing.R        # WER crawl + extraction
│  │  ├─ post_processing.R           # join pop, weather, land cover, ERA5; build features
│  │  ├─ sri_lanka_era5-hourly_to_weekly-district_level.R (ERA5 pipeline pieces)
│  │  └─ sri_lanka_modeling.R (optional modeling)
│  └─ madagascar/ ... (separate project)
├─ helpers/
│  ├─ helpers.R
│  └─ plot_helpers.R
├─ data/ (optional shared data)
└─ CHI-Data.Rproj
```

