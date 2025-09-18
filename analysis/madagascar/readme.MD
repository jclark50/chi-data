Perfect — here’s a **pair of full drafts**. You can drop them straight into your repo:

---

# 📄 **Repo-side Technical Guide** (`analysis/madagascar/README.md`)

````markdown
# Madagascar ERA5 Analysis & Disease/Workability Pipeline

This directory contains the end-to-end workflow for processing ERA5-Land climate data for northeast Madagascar villages (Sarahandrano and Mandena). The goal is to assess climate change impacts on crop disease risk (Phytophthora, Anthracnose) and labor/workability.

GOOGLE DRIVE LINK: https://drive.google.com/drive/folders/1TsTExu4fv4cIeY6rgXawh1uwjDUKlN45?usp=sharing


---

## Setup

1. **Dependencies**:  
   Install required R packages:  
   ```r
   install.packages(c("arrow","dplyr","data.table","lubridate","here"))
````

2. **Environment variables**:
   Define data locations in your `.Renviron` (project or user):

   ```bash
   ERA5_MADAGASCAR=C:/Users/you/Google Drive/CHI-Data/era5_madagascar
   ERA5_OUT=C:/Users/you/Desktop/madagascar_outputs
   ERA5_STREAM=C:/Users/you/Desktop/madagascar_outputs/stream
   ```

   Reload R or run `readRenviron("~/.Renviron")` after editing.

3. **Helpers**:
   Shared functions are in:

   * `helpers/helpers.R` (core calculations, S-curve workability, etc.)
   * `helpers/plot_helpers.R` (plot theming utilities).
     Both are sourced inside the scripts.

---

## Scripts

### 1. `era5_madagascar.R`

Runs the full **ETL pipeline**:

* Reads ERA5 parquet by year (hourly).
* Maps villages to grid cells with nearest (1 cell) and IDW (4 cells).
* Computes **hourly village-weighted series** (ta, td, wbgt, wind, rh, vpd, ssrd).
* Aggregates to **daily** metrics:

  * Mean/max/min temp, diurnal range, RH, VPD.
  * Radiation sums (MJ m^-2).
  * WBGT exceedance hours (≥28, ≥31).
  * Hot hours ≥35 °C.
  * Workability % (all/day/night).
  * Crop disease hours (Phytophthora, Anthracnose).
* Writes to Parquet in partitioned directories:

  * `stream/hourly_agg/`
  * `stream/daily_agg/`

### 2. `madagascar_plots.R`

Takes `daily_agg` and generates compact outputs:

* **Yearly lines** (Phytophthora hours, workability).
* **Monthly climatology** of workability.
* **Seasonal timing** (center-of-mass DOY, peak 30-day).
* **Risk-day summaries** (per month, per spell).
* Outputs tables (`T_*.csv`) and figures (`P_*.png`) in a dated folder under `stream/deliverables_YYYYMMDD_HHMM/`.

---

## Outputs

* **Tables**: CSV summaries (`T_yearly_*.csv`, `T_monthly_*.csv`, etc.)
* **Figures**: PNG plots (`P1_...png`, `P2_...png`, etc.)
* **Intermediate**: Parquet in `stream/` for reproducibility.

---

## Notes

* Use `scheme="nearest1"` for primary results; IDW is for robustness.
* Partial years with <95% valid days are excluded automatically.
* Some years (e.g. 2023) may have row deficits — verify ERA5 source completeness before interpreting anomalies.

````

---

# 📑 **Methods-Style Document** (`docs/methods_madagascar.md`)  

```markdown
# Methods: ERA5-Based Climate, Disease, and Workability Analysis in Northeast Madagascar

This document describes the data sources, preprocessing, metrics, and outputs for the Madagascar climate-health analysis.

---

## Data Sources

- **ERA5-Land hourly reanalysis** (ECMWF, 0.1° resolution).  
- Years analyzed: 1980–2024.  
- Variables: 2 m air temperature (ta), dewpoint (td), WBGT (derived if available), 2 m wind speed, surface solar radiation downwards (ssrd).  
- Study sites:  
  - Sarahandrano (lat –14.607, lon 49.649, elev. ~1347 m).  
  - Mandena (lat –14.477, lon 49.811, elev. ~1348 m).  

---

## Preprocessing

1. **Grid mapping**  
   - Villages mapped to ERA5 grid indices.  
   - Two weighting schemes:
     - *Nearest1*: single nearest cell.  
     - *IDW4*: inverse-distance weights over 4 nearest cells.  

2. **Hourly processing**  
   - Derived metrics computed lazily in Arrow:  
     - Relative humidity (RH) from ta/td.  
     - Vapor pressure deficit (VPD).  
     - Radiation converted from J m^-2 → MJ m^-2.  

3. **Daily aggregation**  
   - Means: tmean, RH, VPD, workability %.  
   - Extremes: tmax, tmin, diurnal temp range (DTR).  
   - Sums: solar radiation, hours exceeding thresholds.  

---

## Disease Metrics

- **Phytophthora** (“palm hours”): ta 20–30 °C and (VPD ≤ 0.7 or RH ≥ 95%).  
- **Anthracnose** (“anth hours”): ta 22–32 °C and (VPD ≤ 0.9 or RH ≥ 93%).  
- Metrics computed hourly, then summed to daily totals.

---

## Workability Metric

- Based on **WBGT (°C)** using a logistic S-curve:  
  - Midpoint = 30 °C.  
  - Steepness = 0.6.  
- Produces % workability per hour.  
- Aggregated to day/night/all-day averages.  
- Used to assess trends in labor capacity under heat stress.

---

## Seasonal Framing

- Season-year defined **Aug → Jul** to capture wet-season alignment with disease onset.  
- Metrics:  
  - **Onset/offset**: based on sustained 7-day sums of ≥24 favorable hours, with quiet windows.  
  - **Center of mass DOY**: weighted average of disease hours across the season.  
  - **Peak 30-day window**: DOY with max 30-day rolling sum.

---

## Outputs

- **Hourly & daily Parquet** (analysis-ready, stored under `stream/`).  
- **CSV summaries**:
  - Yearly (disease hours, workability, extremes).  
  - Monthly averages (risk days, levels).  
  - Streaks (longest consecutive risk days).  
- **Figures**:
  - Yearly disease/workability lines.  
  - Seasonal timing plots.  
  - Risk-day barplots.  
  - Spell-length distributions.  

---

## Usage

- Run `era5_madagascar.R` to regenerate hourly/daily Parquet.  
- Run `madagascar_plots.R` to produce figures and tables.  
- Re-run with updated `ERA5_MADAGASCAR` and `ERA5_STREAM` paths for portability.  

---

## Notes and Caveats

- **Coverage gaps**: Some years (e.g., 2023) have incomplete rows — requires upstream ERA5 validation.  
- **Village collapsing**: Outputs often averaged across the two sites; single-village results remain available.  
- **Weighting choice**: Report nearest-cell results; IDW available as sensitivity check.
````

---

👉 Do you want me to also **add inline references** (e.g., ERA5 citation, phytophthora disease papers) so it doubles as a publication-ready methods appendix? Or keep it internal-only for now?
