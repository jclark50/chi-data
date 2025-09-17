# README — Madagascar climate × vanilla disease/workability analysis

*Last updated: {{today}}*

## 1) What this repo/script does

This analysis builds village-level climate summaries for two sites in NE Madagascar (Mandena, Sarahandrano) from ERA5-Land, then derives:

* Hourly/daily climate metrics (Tmean/Tmax/Tmin, RH, VPD, SSRD, WBGT).
* **Workability** scores (0–100%) by hour/day using an S-curve on WBGT.
* **Vanilla disease risk** indicators (Phytophthora and Anthracnose) from simple, literature-informed temperature–moisture rules.
* Seasonal timing diagnostics (Aug→Jul “season”, center-of-mass DOY, 30-day peak DOY).
* “Hours → **days**” translations: risk days, 7-day rolling totals, and **spells** (consecutive risk days).
* Publication-ready PNGs and compact CSV tables.

It is designed to be fast and memory-safe on large ERA5 parquet by using **Arrow** lazily, collecting only the columns needed.

---

## 2) Inputs & directories

### ERA5 processed parquet (already on disk)

Hourly ERA5-Land single-level variables, pre-processed to parquet with columns like:

* `validTime` (epoch seconds), `lat_idx_x4`, `lon_idx_x4`
* `ta_scaled10_degC`, `td_scaled10_degC`, `wbgt_scaled10_degC`, `wind2m_scaled10_ms1`
* `ssrd` (J m⁻² per hour)

> Grid is 0.25° (\~28 km at the equator), stored as integer indices (`*_idx_x4`).

### Streamed, aggregated outputs (produced by the pipeline)

Two supported layouts for the **daily** and **hourly** aggregates:

* **Layout A (no year partition):**
  `.../daily_agg/scheme=<nearest1|idw4>/village=<name>/part-*.parquet`

* **Layout B (recommended; with year partition):**
  `.../daily_agg/year=<YYYY>/scheme=<nearest1|idw4>/village=<name>/part-*.parquet`

> The plotting/summary script supports both. If you see a “cannot find `/year=.../` path” error, you’re on Layout A—either keep it (don’t filter by year via path) or run the one-time repartition step documented below.

### Village points

Two point locations (WGS84):

* **Sarahandrano**: `(-14.60735, 49.64937)`
* **Mandena**: `(-14.47744, 49.81175)`

Weights are built either as:

* **nearest1**: single nearest ERA5 grid center; or
* **idw4**: inverse-distance-squared over the 4 nearest grid centers.

---

## 3) Software & packages

* R ≥ 4.2
* `arrow`, `data.table`, `dplyr`, `lubridate`
* `sf` (for weights construction; not needed for the plotting-only script)
* `future.apply` (optional; for parallel year processing)
* `berryFunctions` (optional; to auto-open saved PNGs)

Install once:

```r
install.packages(c("arrow","data.table","dplyr","lubridate","sf","future.apply","berryFunctions"))
```

---

## 4) Key configuration knobs

At the top of the plotting/summary script you’ll see:

* `stream_dir` — root folder containing `daily_agg` and (optionally) `hourly_agg`.
* `scheme_focus` — `"nearest1"` or `"idw4"`.
* `years_keep` — analysis window (default `2000:2022`).
* `season_start_month` — start month of season (default `8` for Aug→Jul).
* `riskday_min_hours` — day counted as “risk day” if `palm_hours ≥ this` (default `4`).
* `sustained_7d_thresh` — 7-day sum threshold (e.g., 24 h) to flag sustained periods.

---

## 5) End-to-end workflow (overview)

1. **Map villages → ERA5 grid**
   Build weights per village:

   * `nearest1`: weight = 1 for the single nearest cell.
   * `idw4`: 4 nearest cells with weights ∝ 1/d².

2. **Hourly assemble (per year)**

   * Convert time to local (UTC+3); add `date`, `hour`.
   * Rescale encoded variables (e.g., `ta_scaled10_degC / 10`).
   * Compute **RH**, **VPD**, and **ssrd\_MJ = ssrd / 1e6**.

3. **Village-weighted hourly series**
   Merge with weights; compute weighted means of ta, td, wbgt, rh, vpd, ssrd\_MJ.

4. **Daily aggregation**

   * `tmean_c`, `tmax_c`, `tmin_c`, `dtr_c`
   * `rh_mean`, `vpd_kpa`, `ssrd_MJ_day = sum(ssrd_MJ)`
   * **Heat counters** (integer hours): `wbgt28_h`, `wbgt31_h`, `hot35_h`
   * **Workability**: apply S-curve to hourly WBGT, average by day:

     * Day (09–16), Night (22–06), All hours

5. **Disease rules (hourly → daily)**

   * **Phytophthora** (humid & mild): `20 ≤ Ta ≤ 30` and `(VPD ≤ 0.7  | RH ≥ 95%)`
   * **Anthracnose** (slightly warmer): `22 ≤ Ta ≤ 32` and `(VPD ≤ 0.9  | RH ≥ 93%)`
   * Daily “hours” = count of hourly hits.

6. **Seasonal timing (Aug→Jul)**

   * `season_doy` within the Aug→Jul year.
   * **Center-of-mass DOY** from daily `palm_hours`.
   * **Peak 30-day DOY** from rolling 30-day sums.

7. **Hours → days → spells**

   * **Risk day** if `palm_hours ≥ riskday_min_hours` (default 4 h).
   * **Spells** = consecutive risk days (length distribution).
   * **7-day rolling totals** for sustained periods (e.g., ≥24 h/week).

8. **Outputs**

   * **PNGs** with large fonts, readable axes (base R only).
   * **CSVs** with small, human-readable tables.

---

## 6) Running the plotting/summary script

* Point `stream_dir` at your streamed daily parquet root.
* Run the single block titled **“Madagascar — plots & tables from streamed daily parquet”**.
* It creates `deliverables_YYYYMMDD_HHMM/` under `stream_dir`, containing:

### Figures (PNG)

* `P1_palm_hours_yearlines_mean.png` — Phytophthora-favorable hours per year (village average).
* `P2_workability_all_yearlines_mean.png` — Workability (%, all hours) by year (avg).
* `P3_workability_monthly_day_clim_mean.png` — Daytime workability monthly climatology.
* `P4_season_timing_mean.png` — Center-of-mass & 30-day peak DOY (Aug→Jul season).
* `P5_riskdays_per_month_mean.png` — Average **risk days per month** (hours→days).
* `P6_risk_spells_length_hist.png` — Histogram of **spell lengths** (consecutive risk days).

### Tables (CSV)

* `T_yearly_palm_hours_mean.csv` — Year, mean Phytophthora hours.
* `T_yearly_workability_all_mean.csv` — Year, mean workability (%).
* `T_monthly_workability_day_clim_mean.csv` — Month, mean daytime workability (%).
* `T_season_timing_mean.csv` — Season-year, center-of-mass DOY, 30-day peak DOY.
* `T_riskdays_per_month_mean.csv` — Month, average risk days.
* `risk_spells_length_counts.csv` — Spell length vs. count.

> If you prefer per-village plots, set `collapse` off and split by `village`.

---

## 7) Interpreting the outputs (plain language)

* **Workability (%):** share of hours when outdoor labor is reasonably safe/feasible given heat stress (WBGT). We use a smooth S-curve (midpoint 30 °C WBGT) so scores fade from \~100% in cool conditions toward 0% in extreme heat.
* **Phytophthora “hours”:** count of hours per day/year where temperature and humidity support pathogen activity/survival. The “risk days” plots translate this to **days** (≥4 risky hours) to make it easier to reason about agricultural schedules and advisories.
* **Seasonal timing:**

  * **Center-of-mass** DOY is like the “average timing” of the risky period within Aug→Jul;
  * **Peak 30-day** DOY points to the most concentrated month-long window each season.
* **Spells:** consecutive days that meet the daily risk criterion. More and longer spells are operationally meaningful (spray timing, field hygiene, harvest planning).

---

## 8) Common pitfalls & fixes

* **Arrow “int32 truncation” or mixed types (e.g., `wbgt28_h`)**
  Some partitions may store floats while others store ints. When reading, **upcast** to `float64()` inside Arrow before `collect()`. If needed, **repair** counters by recomputing from hourly and rewriting as integers.

* **“Cannot find path … `/year=YYYY/…`”**
  You’re on **Layout A** (no year partition). Either:

  * Keep Layout A and compute `year := year(date)` after reading, or
  * Run the one-time **repartition** to add `/year/` folders and reopen.

* **Huge RAM use**
  Keep operations **lazy** until the last step; select only the columns you need; collect in chunks by year if necessary.

* **Duplicate village lines**
  With these two points, ERA5 grid hits almost the same cells; averaging them to a single line is fine for high-level comms. If you need more local contrast, use `idw4` or pull higher-res data later.

---

## 9) Parameter defaults (documented)

* Workability S-curve: `steepness = 0.6`, `midpoint = 30` °C (WBGT).
  *Change only with a clear justification; this is a communicable default.*
* Risk day threshold: `riskday_min_hours = 4`.
  *Lower values inflate counts; higher values catch only tighter windows.*
* Sustained period: `sustained_7d_thresh = 24` h / 7 days.
  *Tune if collaborators define “sustained” differently.*
* Season start: `season_start_month = 8` (Aug→Jul) to avoid wet-season split across Jan.

---

## 10) Extending this work

* **Village set:** add more points; the weighting tools handle any number of sites.
* **Compare schemes:** re-run figures for `nearest1` vs. `idw4` (or change K).
* **Sri Lanka / other observatories:** reuse the same pipeline; swap `stream_dir`, village points, and season start; keep the plotting block.
* **Survey alignment:** map survey question periods (e.g., 2018–2022) to our **Prev5 vs Recent5** windows and export side-by-side tables for perception vs. ERA5-derived change.

---

## 11) Repartition helper (optional, one-time)

```r
daily_src <- arrow::open_dataset(file.path(stream_dir, "daily_agg"),
                                 format = "parquet", unify_schemas = TRUE)
daily_target <- file.path(stream_dir, "daily_agg_by_year")
if (dir.exists(daily_target)) unlink(daily_target, recursive = TRUE)

daily_src |>
  dplyr::mutate(
    year = lubridate::year(date),
    wbgt28_h = arrow::cast(wbgt28_h, arrow::float64()),
    wbgt31_h = arrow::cast(wbgt31_h, arrow::float64()),
    hot35_h  = arrow::cast(hot35_h,  arrow::float64())
  ) |>
  arrow::write_dataset(
    path = daily_target,
    format = "parquet",
    partitioning = c("year","scheme","village")
  )
```

---

## 12) Data dictionary (CSVs)

* **T\_yearly\_palm\_hours\_mean.csv**
  `year`, `val` (mean Phytophthora-favorable hours across villages)

* **T\_yearly\_workability\_all\_mean.csv**
  `year`, `val` (mean workability %, all hours, across villages)

* **T\_monthly\_workability\_day\_clim\_mean.csv**
  `month (1–12)`, `val` (mean workability %, 09–16, across years & villages)

* **T\_season\_timing\_mean.csv**
  `season_year` (Aug→Jul label), `com_doy`, `peak30_doy` (days 1–365)

* **T\_riskdays\_per\_month\_mean.csv**
  `month`, `risk_days` (avg risk days per month across years & villages)

* **risk\_spells\_length\_counts.csv**
  `len` (days), `N` (count of spells 2000–2022, villages collapsed)

---

## 13) Contact & notes

* This analysis supports CHI Objective 1 ▸ Priority 1 (data infrastructure + harmonized analytics across observatories).
* Contains derived climate indicators; **do not** distribute raw ERA5 tiles outside existing licenses/DUAs.
* For questions or to extend to other sites, ping Jordan (Data Science contractor, CHI).

*That’s it. Run the plotting block, and you’ll get a dated `deliverables_*` folder with PNGs + CSVs ready to drop into your weekly notes or a 1–2-pager.*
