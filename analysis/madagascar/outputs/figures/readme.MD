# Madagascar – Figures

This folder contains figures produced by the Madagascar climate/workability
pipeline (ERA5-Land → village-weighted hourly → daily summaries → plots).
Images are **base R** outputs unless noted. See the repo’s main README for
how to re-run the pipeline.

> Conventions  
> • `idw4` = inverse-distance weighting over up to 4 grid centers  
> • `nearest1` = nearest grid center only  
> • Village names: **Mandena**, **Sarahandrano**

## What’s here

### A. Time series (annual, with OLS trend line)
- `annual_timeseries_ols_idw4 • Mandena.png`  
  Annual climate metrics for Mandena using **idw4** mapping.
- `annual_timeseries_ols_idw4 • Sarahandrano.png`  
  Annual climate metrics for Sarahandrano using **idw4**.
- `annual_timeseries_ols_nearest1 • Mandena.png`  
  Same as above using **nearest1**.
- `annual_timeseries_ols_nearest1 • Sarahandrano.png`  
  Same as above using **nearest1**.

### B. Period contrasts (2018–22 vs earlier 5-yr blocks)
- `bars_delta_rh_mean_idw4 • Mandena.png`  
  Change in **relative humidity** by month, Mandena (**idw4**), vs 2013–17, 2008–12, 2003–07 (with CIs).
- `bars_delta_rh_mean_idw4 • Sarahandrano.png`  
  Same for Sarahandrano (**idw4**).
- `bars_delta_rh_mean_nearest1 • Mandena.png`  
  Same using **nearest1**.
- `bars_delta_rh_mean_nearest1 • Sarahandrano.png`  
  Same using **nearest1**.
- `bars_delta_vpd_vs_baselines_idw4 • Mandena.png`  
  Change in **vapor pressure deficit (VPD)** by month, Mandena (**idw4**), with bootstrap CIs.
- `bars_delta_vpd_vs_baselines_idw4 • Sarahandrano.png`  
  Same for Sarahandrano (**idw4**).
- `bars_delta_vpd_vs_baselines_nearest1 • Mandena.png`  
  Same using **nearest1**.
- `bars_delta_vpd_vs_baselines_nearest1 • Sarahandrano.png`  
  Same using **nearest1**.

### C. Heatmaps of Δ temperature (2018–22 minus baseline)
- `heatmap_delta_tmean_c_idw4 • Mandena.png`  
  Monthly Δ **mean temperature** (°C), Mandena (**idw4**), baseline = all three blocks combined.
- `heatmap_delta_tmean_c_idw4 • Sarahandrano.png`  
  Same for Sarahandrano (**idw4**).
- `heatmap_delta_tmean_c_nearest1 • Mandena.png`  
  Same using **nearest1**.
- `heatmap_delta_tmean_c_nearest1 • Sarahandrano.png`  
  Same using **nearest1**.
- `heatmap_delta_tmean_c_vs_2013-2017_idw4 • Mandena.png`  
  Monthly Δ vs **2013–2017** only (Mandena, **idw4**).
- `heatmap_delta_tmean_c_vs_2013-2017_idw4 • Sarahandrano.png`  
  Monthly Δ vs **2013–2017** (Sarahandrano, **idw4**).
- `heatmap_delta_tmean_c_vs_2013-2017_nearest1 • Mandena.png`  
  Same using **nearest1**.
- `heatmap_delta_tmean_c_vs_2013-2017_nearest1 • Sarahandrano.png`  
  Same using **nearest1**.

### D. Mapping / grid-cell context
- `map_era5_cells_nearest_vs_idw.png`  
  ERA5-Land grid centers used by **nearest1** and neighbors blended by **idw4**.  
  Shows both villages fall within the same central grid cell and the neighbors
  used for idw4 (important limitation).

### E. Workability & disease-related diagnostics
- `P1_palm_hours_yearlines_byvillage.png`  
  Annual sum of **Phytophthora-favorable hours** (rule-based), by village.
- `P10_loss_area_yearly_byvillage.png`  
  Annual **loss-area** = Σ(100 − workability%) — a risk-weighted burden.
- `P11_max_block70_histogram_byvillage.png`  
  Histogram of **max contiguous hours ≥70% workability** per day.
- `P12_shift_window_workability_monthly_byvillage.png`  
  Average workability by **morning/midday/afternoon/night** windows across months.
- `P2_workability_all_yearlines_byvillage.png`  
  Annual mean **all-hours workability (%)** by village.
- `P4_schedulability_share_days_ge4h70_by_month.png`  
  Share of days per month with **≥4 contiguous hours** at **≥70%** workability.
- `P5_degree_hours_monthly_byvillage.png`  
  Monthly **degree-hours** above **28/31 °C (WBGT)** by village.
- `P8_workability_percentiles_monthly_byvillage.png`  
  Monthly **P25/P50/P75** workability by village.

### F. Bundled exports
- `FigurePack_20250918_1554.pdf`  
  All key figures combined for sharing/QA.
- `FigurePack_QA_20250918_1554.csv`  
  Provenance listing (page order and captions) for the PDF.

## Repro & notes
- Figures are produced by the analysis scripts in this repo under `analysis/madagascar`.
- If filenames differ, they reflect the mapping scheme (idw4 vs nearest1) and village.
- **Glossary (one-liners):**  
  • *Workability (%):* comfort/workable conditions score per hour.  
  • *VPD (kPa):* “thirstiness” of air; higher = drier feeling.  
  • *Degree-hours:* heat dose above a threshold.  
  • *WBGT:* heat stress index factoring temp/humidity/sun/wind.

