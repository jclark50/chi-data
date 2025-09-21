population data: https://data.gov.lk/dataset/population-district-census-years
weather portal: https://weather.meteo.gov.lk/


Here’s a **concise GitHub README draft** that tells someone what to run and why, with an option to start directly from `post_processing.R` if they just want to explore data:

---

# Sri Lanka WER Processing

This repository contains scripts to build a reproducible dataset from the **Weekly Epidemiological Reports (WERs)** published by the Sri Lanka Ministry of Health.

The workflow extracts disease counts (e.g., dengue, leptospirosis) from PDFs, links them with population, weather, land cover, and ERA5 climate data, and produces **analysis-ready panels and plots**.

---

## Getting Started

1. **Clone the repository**

   ```bash
   git clone https://github.com/jclark50/chi-data.git
   cd chi-data
   ```

2. **Install R dependencies**
   Packages used include:
   `data.table`, `stringr`, `lubridate`, `pdftools`, `tabulapdf`, `tabulizerjars`, `sf`, `terra`, `exactextractr`, `ggplot2`, `scales`, `ragg`.

   Make sure you also have a working Java installation for PDF table extraction (`tabulapdf`).

---

## Workflow

* **`initial_processing.R`**
  Full end-to-end pipeline:

  * Indexes WER PDFs
  * Extracts weekly district-level disease counts
  * Joins with population, weather station, land cover, and ERA5 aggregates
  * Saves intermediate outputs for downstream use

* **`post_processing.R`**
  Loads processed outputs and focuses on:

  * Data cleaning
  * Exploratory analysis
  * Plot generation and figure export

---

## Quick Start

If you **just want to explore the data/plots** without running the full extraction pipeline:

* Clone this repo
* Run **`post_processing.R`**
* This will start from the already-extracted outputs saved in `analysis/sri_lanka/outputs/`

---

## Outputs

* `outputs/sri_lanka_WER_index_of_pdfs.csv` → reproducible index of all WERs
* `outputs/disease_counts_v4.txt` → extracted disease tables
* Figures are written to `analysis/sri_lanka/outputs/figures/`

---

Would you like me to also add a **“Project Structure” tree diagram** (like a mini file map with `/analysis/sri_lanka`, `/helpers`, `/outputs`) so newcomers can immediately see where things live?
