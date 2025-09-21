# =============================================================================
# DGHI / CHI - Madagascar Climate & Disease Pipeline
# -----------------------------------------------------------------------------
# WHAT
#   End-to-end rebuild of a reproducible pipeline using ERA5/ERA5-Land parquet
#   to produce village-weighted hourly climate variables, daily aggregates,
#   disease "risk hours," and robust workability metrics. Outputs are written
#   as partitioned Parquet to feed analyses and figures.
#
# WHY
#   . Provide consistent, auditable climate exposures for CHI's Madagascar work.
#   . Support comparisons across 5-year periods and long-term trends.
#   . Deliver interpretable workability metrics for communication and planning.
#
# HOW (high-level flow)
#   1) Ingest hourly ERA5* parquet by year (Arrow dataset; lazy scan + collect).
#   2) Convert UTC timestamps to local time (Indian/Antananarivo), derive hour,
#      and compute ta/td/wbgt/wind/rh/vpd/ssrd_MJ (hourly).
#   3) Map village points to ERA5 grid centers and build weights:
#        - scheme = "nearest1": single nearest 0.25° center (weight=1)
#        - scheme = "idw4": inverse-distance weights over 4 nearest centers
#      (Weights are computed on grid **centers**; polygons are for visualization.)
#   4) Hourly village-weighted aggregation (per scheme × village × date × hour).
#   5) Daily rollups:
#        - Baseline met: tmean/tmax/tmin/DTR, RH, VPD, SSRD (sum).
#        - Heat counts: hours with WBGT ??? 28/31, hours with T ??? 35 °C.
#        - Disease risk hours (rule-of-thumb thermal-humidity windows).
#        - Workability metrics:
#            · Distribution: P05/25/50/75/95, IQR, Expected Shortfall (worst 20%).
#            · Threshold dose: degree-hours over 28/31; hours in Safe/Caution/High.
#            · Schedulability: max contiguous ???70/80% blocks; count of 2h/4h blocks.
#            · Shift windows: 06-10 / 10-14 / 14-18 / 22-06 means.
#            · Earliest start time to secure a ???4h window at ???70%.
#            · Work:rest-implied effective minutes (Light/Moderate/Heavy).
#            · Loss area: ???(100 ??? workability%) per day (risk-weighted deficit).
#        - Workability S-curve midpoint is set to the year's 95th-percentile WBGT
#          (adaptive). Swap for a fixed midpoint if you need strict comparability.
#   6) Write partitioned Parquet:
#        - hourly_agg/YYYY.parquet  (village-weighted hourly)
#        - daily_agg/YYYY.parquet   (enriched daily metrics)
#
# INPUTS (assumptions)
#   . ERA5 parquet directory with year subfolders at ERA5_MADAGASCAR (env var).
#   . Per-hour columns (rename here if different):
#       validTime, ssrd, ta_scaled10_degC, td_scaled10_degC, wbgt_scaled10_degC,
#       wind2m_scaled10_ms1, lat_idx_x4, lon_idx_x4  (indices at 0.25°: idx/4)
#   . Villages data.table: village, lon, lat, elev_m.
#
# OUTPUTS
#   . Parquet files under ERA5_OUT/. and ERA5_STREAM/. (env vars).
#   . Hourly and daily tables partitioned by scheme/village/year.
#
# CONFIGURATION (set in ~/.Renviron or project .Renviron)
#   ERA5_MADAGASCAR=G:/My Drive/Duke/DGHI/data/era5/
#   ERA5_OUT=C:/./madagascar
#   ERA5_STREAM=C:/./madagascar/stream
#   (You can also Sys.setenv(...) inside the script for quick tests.)
#
# KEY FUNCTIONS
#   . make_weights_centers(): builds "nearest1" and "idw4" center weights.
#   . read_one_year_raw(): Arrow read + derive hourly variables (UTC???local).
#   . process_year(): village-weighted hourly; daily rollups; write Parquet.
#
# DESIGN NOTES / TRADEOFFS
#   . Weighting is performed on grid **centers**; this preserves ERA5 sampling.
#   . Derived metrics (e.g., WBGT, workability) are computed AFTER weighting the
#     primitive hourly variables-avoids mixing nonlinear transforms with spatial
#     interpolation.
#   . The adaptive WBGT midpoint makes the S-curve reflect each year's climate.
#     Use a fixed midpoint for strict cross-year comparability.
#   . "nearest1" is crisp and interpretable; "idw4" reduces jumps across grid
#     lines. Consider bilinear or area-average variants for sensitivity analyses.
#
# PERFORMANCE / SCALING
#   . Arrow dataset scanning avoids loading full years when not needed.
#   . Year-by-year processing limits memory and enables parallelization (outer
#     loop) if desired.
#
# QA / SANITY CHECKS
#   . Ensure each year has ???95% of days before using in trend/period analyses.
#   . Inspect daily_agg fields for NAs; WBGT availability may vary by year.
#   . Spot-check village cells and weights with the provided mapping utility.
#
# HOW TO RUN
#   1) Set env vars in .Renviron (paths above), reopen R session.
#   2) Edit village coordinates if needed.
#   3) Source helpers (helpers.R) and run the script.
#   4) Adjust years_all to your analysis window and rerun.
#
# REPRODUCIBILITY
#   . All parameters (paths, years, time zone, thresholds) are defined up top.
#   . Outputs are deterministic given inputs and config; version control this file
#     and helpers.R alongside the data dictionary for ERA5 parquet.
#
# CONTACT
#   DGHI Climate & Health Initiative - Jordan Clark
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(lubridate)
  library(here)
})



##########################################
# # 0) CONFIG
##########################################

# ---- Load helper functions ----
source(here("helpers", "helpers.R"))

# ---- Configurable paths via .Renviron ----
# In your ~/.Renviron or project .Renviron file, set:
#   ERA5_MADAGASCAR=C:/Users/jordan/Google Drive/CHI-Data/era5_madagascar
#   ERA5_STREAM=C:/Users/jordan/Google Drive/CHI-Data/madagascar_streamed
#
# To check what R sees:
# Sys.getenv("ERA5_MADAGASCAR")
Sys.setenv("ERA5_MADAGASCAR" = "G:/My Drive/Duke/DGHI/data/era5/madagascar/")
era5_root <- Sys.getenv("ERA5_MADAGASCAR", unset = NA)
if (is.na(era5_root)) {
  stop("Environment variable ERA5_MADAGASCAR not set. Please edit your .Renviron.")
  # usethis::edit_r_environ("project")
  # Sys.setenv("ERA5_MADAGASCAR" = "G:/My Drive/Duke/DGHI/data/era5/")
}

Sys.setenv("ERA5_OUT" = "C:/Users/jordan/Desktop/madagascar")
Sys.setenv("ERA5_STREAM" = "C:/Users/jordan/Desktop/madagascar/stream")


out_root <- Sys.getenv("ERA5_OUT", unset = "C:/data/chi_outputs/madagascar")
dir.create(out_root, recursive = TRUE, showWarnings = FALSE)


stream_dir <- Sys.getenv("ERA5_STREAM", unset = file.path(out_root, "streamed"))
dir.create(stream_dir, recursive = TRUE, showWarnings = FALSE)

########################
########################

# ---- Analysis settings ----
years_all   <- 1980:2024
tz_local      <- "Indian/Antananarivo"
tz_offset_sec <- 3L * 3600L



villages <- data.table(
  village = c("Sarahandrano","Mandena"),
  lon     = c(49.6493707, 49.8117457),
  lat     = c(-14.6073478, -14.4774428),
  elev_m  = c(1347, 1348)
)

##########################################
# 1) VARIABLE MAP (EDIT HERE IF COLUMN NAMES DIFFER)
##########################################
# ERA5 parquet per-hour columns expected in each year folder:
#  - validTime: integer seconds since epoch (UTC) or timestamp
#  - ssrd:      surface solar radiation downwards (J m^-2 per hour)
#  - ta_scaled10_degC:  air temp *10 (°C * 10)
#  - td_scaled10_degC:  dewpoint *10 (°C * 10)
#  - wbgt_scaled10_degC: WBGT *10 (°C * 10)   (if absent, set wbgt = NA)
#  - wind2m_scaled10_ms1: wind at 2m *10 (m s^-1 *10)  (optional)
#  - lat_idx_x4 / lon_idx_x4: 0.25° grid-center indices (lat=idx/4, lon=idx/4)
VAR_VALIDTIME <- "validTime"
VAR_SSRD      <- "ssrd"
VAR_TA10      <- "ta_scaled10_degC"
VAR_TD10      <- "td_scaled10_degC"
VAR_WBGT10    <- "wbgt_scaled10_degC"
VAR_WIND10    <- "wind2m_scaled10_ms1"


VAR_LATIDX    <- "lat_idx_x4"
VAR_LONIDX    <- "lon_idx_x4"

##########################################
# 2) HELPERS
##########################################
scale10 <- function(x) as.numeric(x) / 10

# Workability S-curve: % workable per hour from WBGT
workability <- function(wbgt, steepness = 0.6, midpoint = 30) {
  100 * (1 - 1 / (1 + exp(-steepness * (wbgt - midpoint))))
}

# Build village???grid weights using 3×3 **centers** (not corners)
to_idx <- function(x) round(x * 4)
dist2_m2 <- function(lon1, lat1, lon2, lat2) {
  r <- 6371000
  x <- (lon2 - lon1) * cos((lat1 + lat2) * pi / 360)
  y <- (lat2 - lat1)
  (r * pi / 180)^2 * (x^2 + y^2)
}

to_idx <- function(x) round(x * 4)

make_weights_centers <- function(villages_dt, K_idw = 4L) {
  stopifnot(all(c("village","lon","lat") %in% names(villages_dt)))
  out <- vector("list", nrow(villages_dt))
  
  for (i in seq_len(nrow(villages_dt))) {
    v <- villages_dt[i]
    
    i_lat <- to_idx(v$lat)
    i_lon <- to_idx(v$lon)
    
    # 3x3 candidate **centers**
    cand <- CJ(
      lat_idx_x4 = as.integer((i_lat - 1):(i_lat + 1)),
      lon_idx_x4 = as.integer((i_lon - 1):(i_lon + 1))
    )
    
    # two-step creation so we can reference lat_c/lon_c when computing d2
    cand[, `:=`(
      lat_c = lat_idx_x4 / 4,
      lon_c = lon_idx_x4 / 4
    )]
    cand[, d2 := dist2_m2(v$lon, v$lat, lon_c, lat_c)]
    
    # K = 1 nearest center
    k1 <- cand[which.min(d2), .(lat_idx_x4, lon_idx_x4)]
    k1[, `:=`(scheme = "nearest1", village = v$village, weight = 1)]
    
    # K = 4 IDW on nearest centers
    k4 <- cand[order(d2)][1L:K_idw, .(lat_idx_x4, lon_idx_x4, d2)]
    k4[, w := 1 / pmax(d2, 1)]
    k4[, weight := as.numeric(w / sum(w))]
    k4[, `:=`(scheme = "idw4", village = v$village)]
    k4 <- k4[, .(lat_idx_x4, lon_idx_x4, scheme, village, weight)]
    
    out[[i]] <- rbind(
      k1[, .(lat_idx_x4, lon_idx_x4, scheme, village, weight)],
      k4
    )
  }
  
  ans <- rbindlist(out)
  setcolorder(ans, c("scheme","village","lat_idx_x4","lon_idx_x4","weight"))
  ans[]
}



list.files(stream_dir, recursive=TRUE)
library(dplyr)

##########################################
# 3) INGEST: READ ONE YEAR (HOURLY GRID CELLS), RETURN RAW DT
#    (Lazy Arrow read; collect at the end)
##########################################
read_one_year_raw <- function(yr) {
  ydir <- file.path(era5_root, yr)
  if (!dir.exists(ydir)) return(data.table())
  ds <- open_dataset(ydir, format = "parquet",
                     factory_options = list(exclude_invalid_files = TRUE))
  
  if (!"wind2m_scaled10_ms1" %in% names(ds)){
    VAR_WIND10 = 'wind2m_scaled10_ms'
  } else {
    VAR_WIND10 = 'wind2m_scaled10_ms1'
  }
  # yr2301 = open_dataset("E:/data/gridded/era5-africa/processed/2023/01")
  # yr2301 = yr2301 %>% collect()
  # yr2301$validTime = as.POSIXct(yr2301$validTime, tz = 'UTC')
  # # 
  # # yr2301$date = as.Date(yr2301$validTime)
  # # 
  # # names(ds)
  # # names(ds)
  # # validTime ??? timestamp UTC ??? local date/hour
  # # compute derived variables lazily; collect at end
  # dt <- ds |>
  #   select(all_of(c(VAR_LATIDX, VAR_LONIDX, VAR_VALIDTIME,
  #                                     VAR_SSRD, VAR_TA10, VAR_TD10, VAR_WBGT10, VAR_WIND10)))|>
  #   collect()
  # 
  # 
  # 
  # setDT(dt)
  # 
  # # --- eager transformations in R ---
  # # validTime ??? POSIXct UTC
  # dt[, validTime := as.POSIXct(get(VAR_VALIDTIME), origin = "1970-01-01", tz = "UTC")]
  # 
  # # local time shifted by offset (seconds)
  # dt[, local_ts := validTime + tz_offset_sec]
  # 
  # # date + hour
  # dt[, `:=`(
  #   date = as.Date(local_ts, tz = tz_local),
  #   hour = lhour(local_ts)
  # )]
  # 
  # # scale variables
  # dt[, ta   := round(get(VAR_TA10)   / 10, 1)]
  # dt[, td   := round(get(VAR_TD10)   / 10, 1)]
  # dt[, wbgt := round(get(VAR_WBGT10) / 10, 1)]
  # dt[, wind := round(get(VAR_WIND10) / 10, 1)]
  # 
  # # humidity / vapor pressure
  # dt[, `:=`(
  #   rh = exp((17.62 * td) / (243.12 + td) - (17.62 * ta) / (243.12 + ta)) * 100,
  #   es = 0.6108 * exp(17.27 * ta / (ta + 237.3)),
  #   ea = 0.6108 * exp(17.27 * td / (td + 237.3))
  # )]
  # dt[, vpd := pmax(es - ea, 0.0)]
  # 
  # # radiation in MJ/m2
  # dt[, ssrd_MJ := get(VAR_SSRD) / 1e6]
  # 
  # # enforce integer grid indices
  # setnames(dt, c(VAR_LATIDX, VAR_LONIDX), c("lat_idx_x4","lon_idx_x4"))
  # dt[, `:=`(
  #   lat_idx_x4 = as.integer(lat_idx_x4),
  #   lon_idx_x4 = as.integer(lon_idx_x4)
  # )]

  dt <- ds |>
    select(all_of(c(VAR_LATIDX, VAR_LONIDX, VAR_VALIDTIME,
                    VAR_SSRD, VAR_TA10, VAR_TD10, VAR_WBGT10, VAR_WIND10))) |>
    mutate(
      validTime = cast(cast(!!sym(VAR_VALIDTIME), int64()),
                       timestamp("s", timezone = "UTC")),
      local_ts  = cast(cast(validTime, int64()) + tz_offset_sec, timestamp("s")),
      date      = cast(local_ts, date32()),
      hour      = hour(local_ts),

      ta   = round(cast(!!sym(VAR_TA10),    float64()) / 10, 1),
      td   = round(cast(!!sym(VAR_TD10),    float64()) / 10, 1),
      wbgt = round(cast(!!sym(VAR_WBGT10),  float64()) / 10, 1),
      wind = round(cast(!!sym(VAR_WIND10),  float64()) / 10, 1),

      rh   = exp((17.62 * td) / (243.12 + td) - (17.62 * ta) / (243.12 + ta)) * 100,
      es   = 0.6108 * exp(17.27 * ta / (ta + 237.3)),
      ea   = 0.6108 * exp(17.27 * td / (td + 237.3)),
      vpd  = if_else(es - ea > 0, es - ea, 0.0),

      ssrd_MJ = cast(!!sym(VAR_SSRD), float64()) / 1e6
    ) |>
    select(all_of(c(VAR_LATIDX, VAR_LONIDX)), date, hour, ta, td, wbgt, wind, rh, vpd, ssrd_MJ) |>
    collect()
  # 
  setDT(dt)
  # enforce integer indices
  setnames(dt, c(VAR_LATIDX, VAR_LONIDX), c("lat_idx_x4","lon_idx_x4"))
  dt[, `:=`(lat_idx_x4 = as.integer(lat_idx_x4),
            lon_idx_x4 = as.integer(lon_idx_x4))]
  dt[]
}

###########################################
# 4) STREAM PROCESS: HOURLY ??? VILLAGE-WEIGHTED HOURLY + DAILY
########################################### 
yr = 2015

process_year <- function(yr, weights_dt) {
  raw <- read_one_year_raw(yr)
  if (nrow(raw) == 0) return(invisible(NULL))
  # merge weights (both schemes), compute weighted averages per village/hour
  setDT(raw)
  raw <- merge(raw, weights_dt,
               by = c("lat_idx_x4","lon_idx_x4"), allow.cartesian = TRUE)
  
  # Weighted hourly (per scheme, village, date, hour)
  hr <- raw[, .(
    ta      = sum(ta      * weight, na.rm = TRUE) / sum(weight),
    td      = sum(td      * weight, na.rm = TRUE) / sum(weight),
    wbgt    = sum(wbgt    * weight, na.rm = TRUE) / sum(weight),
    wind    = sum(wind    * weight, na.rm = TRUE) / sum(weight),
    rh      = sum(rh      * weight, na.rm = TRUE) / sum(weight),
    vpd     = sum(vpd     * weight, na.rm = TRUE) / sum(weight),
    ssrd_MJ = sum(ssrd_MJ * weight, na.rm = TRUE) / sum(weight)
  ), by = .(scheme, village, date, hour)]
  
  
  midpoint95 = as.numeric(quantile(hr$wbgt, 0.95))

  # Daily aggregates (sums/means)
  dly <- hr[, .(
    tmean_c     = mean(ta, na.rm = TRUE),
    tmax_c      = max(ta,  na.rm = TRUE),
    tmin_c      = min(ta,  na.rm = TRUE),
    dtr_c       = max(ta, na.rm = TRUE) - min(ta, na.rm = TRUE),
    rh_mean     = mean(rh, na.rm = TRUE),
    vpd_kpa     = mean(vpd, na.rm = TRUE),
    ssrd_MJ_day = sum(ssrd_MJ, na.rm = TRUE),       # sum of hourly ??? daily MJ m^-2
    wbgt28_h    = sum(wbgt >= 28, na.rm = TRUE),
    wbgt31_h    = sum(wbgt >= 31, na.rm = TRUE),
    hot35_h     = sum(ta   >= 35, na.rm = TRUE),
    
    # Workability windows
    work_day_pct   = mean(workability(wbgt, midpoint = midpoint95)[hour %in% 9:16],  na.rm = TRUE),
    work_night_pct = mean(workability(wbgt, midpoint = midpoint95)[hour %in% c(22:23,0:6)], na.rm = TRUE),
    work_all_pct   = mean(workability(wbgt, midpoint = midpoint95), na.rm = TRUE)
  ), by = .(scheme, village, date)]
  
  # Disease "risk hours" per day using hours logic (tweak as needed)
  disease_hourly <- function(ta, vpd, rh) {
    # Example: Phytophthora (palm): 20-30 C & (vpd <= 0.7 or rh >= 95)
    list(
      palm = as.integer(ta >= 20 & ta <= 30 & (vpd <= 0.7 | rh >= 95)),
      anth = as.integer(ta >= 22 & ta <= 32 & (vpd <= 0.9 | rh >= 93))
    )
  }
  dh <- hr[, {
    flags <- disease_hourly(ta, vpd, rh)
    .(palm_hours = sum(flags$palm, na.rm = TRUE),
      anth_hours = sum(flags$anth, na.rm = TRUE))
  }, by = .(scheme, village, date)]
  
  dly <- merge(dly, dh, by = c("scheme","village","date"), all.x = TRUE)

  # Write parquet partitions (append mode)
  hourly_dir <- file.path(stream_dir, "hourly_agg")
  daily_dir  <- file.path(stream_dir, "daily_agg")
  
  
  dly[, year := year(date)]
  hr[, year := year(date)]
  
  
  dly = dly[year == yr]
  hr = hr[year == yr]

  write_parquet(hr, paste0(hourly_dir, "/", yr, ".parquet"))
  write_parquet(hr, paste0(daily_dir, "/", yr, ".parquet"))
  
  invisible(NULL)

  
}


# Summary: Process one year ??? village-weighted hourly + enriched daily metrics, then write parquet.
process_year <- function(yr, weights_dt) {
  # --- load hourly grid, bail if nothing for this year ---
  raw <- read_one_year_raw(yr)
  if (nrow(raw) == 0) return(invisible(NULL))
  
  # --- merge 3x3 weights (both schemes) and compute weighted hourly by village ---
  setDT(raw)
  raw <- merge(raw, weights_dt,
               by = c("lat_idx_x4","lon_idx_x4"), allow.cartesian = TRUE)
  
  hr <- raw[, .(
    ta      = sum(ta      * weight, na.rm = TRUE) / sum(weight),
    td      = sum(td      * weight, na.rm = TRUE) / sum(weight),
    wbgt    = sum(wbgt    * weight, na.rm = TRUE) / sum(weight),
    wind    = sum(wind    * weight, na.rm = TRUE) / sum(weight),
    rh      = sum(rh      * weight, na.rm = TRUE) / sum(weight),
    vpd     = sum(vpd     * weight, na.rm = TRUE) / sum(weight),
    ssrd_MJ = sum(ssrd_MJ * weight, na.rm = TRUE) / sum(weight)
  ), by = .(scheme, village, date, hour)]
  
  # --- workability helpers (embedded to keep function self-contained) ---
  wbgt_band <- function(x) data.table::fifelse(x < 28, "safe",
                                               data.table::fifelse(x < 31, "caution", "high"))
  deg_hours_over <- function(x, t) pmax(x - t, 0)
  max_runlen <- function(x) { if (!any(x)) 0L else max(with(rle(x), lengths[values])) }
  count_runs_ge <- function(x, k = 2L) {
    if (!any(x)) return(0L)
    rl <- rle(x); sum(rl$values & rl$lengths >= k)
  }
  work_fraction <- function(wbgt_c, workload = c("light","moderate","heavy")) {
    wl <- match.arg(workload)
    if (wl == "light") {
      data.table::fifelse(wbgt_c < 30, 1.00,
                          data.table::fifelse(wbgt_c < 31.5, 0.75,
                                              data.table::fifelse(wbgt_c < 33.0, 0.50,
                                                                  data.table::fifelse(wbgt_c < 34.5, 0.25, 0.00))))
    } else if (wl == "moderate") {
      data.table::fifelse(wbgt_c < 28, 1.00,
                          data.table::fifelse(wbgt_c < 29.5, 0.75,
                                              data.table::fifelse(wbgt_c < 31.0, 0.50,
                                                                  data.table::fifelse(wbgt_c < 32.0, 0.25, 0.00))))
    } else {
      data.table::fifelse(wbgt_c < 26, 1.00,
                          data.table::fifelse(wbgt_c < 27.5, 0.75,
                                              data.table::fifelse(wbgt_c < 29.0, 0.50,
                                                                  data.table::fifelse(wbgt_c < 30.0, 0.25, 0.00))))
    }
  }
  
  H_DAY   <- 9:16
  H_MORN  <- 6:10
  H_MID   <- 10:14
  H_AFTER <- 14:18
  H_NIGHT <- c(22:23, 0:6)
  
  # --- choose workability midpoint from this year's distribution (95th %ile WBGT) ---
  midpoint95 <- as.numeric(stats::quantile(hr$wbgt, 0.95, na.rm = TRUE))
  
  # --- enrich hourly with workability-derived columns ---
  hr[, `:=`(
    work_pct = workability(wbgt, midpoint = midpoint95),      # 0-100
    band     = wbgt_band(wbgt),
    loss_pct = 100 - workability(wbgt, midpoint = midpoint95),
    dh_28    = deg_hours_over(wbgt, 28),
    dh_31    = deg_hours_over(wbgt, 31),
    wf_light    = work_fraction(wbgt, "light"),
    wf_moderate = work_fraction(wbgt, "moderate"),
    wf_heavy    = work_fraction(wbgt, "heavy")
  )]
  
  # --- disease rule-of-thumb flags (hourly) ---
  disease_hourly <- function(ta, vpd, rh) {
    list(
      palm = as.integer(ta >= 20 & ta <= 30 & (vpd <= 0.7 | rh >= 95)),
      anth = as.integer(ta >= 22 & ta <= 32 & (vpd <= 0.9 | rh >= 93))
    )
  }
  
  # --- daily baseline meteorology (means/sums) ---
  dly_base <- hr[, .(
    tmean_c     = mean(ta, na.rm = TRUE),
    tmax_c      = max(ta,  na.rm = TRUE),
    tmin_c      = min(ta,  na.rm = TRUE),
    dtr_c       = max(ta, na.rm = TRUE) - min(ta, na.rm = TRUE),
    rh_mean     = mean(rh, na.rm = TRUE),
    vpd_kpa     = mean(vpd, na.rm = TRUE),
    ssrd_MJ_day = sum(ssrd_MJ, na.rm = TRUE),
    wbgt28_h    = sum(wbgt >= 28, na.rm = TRUE),
    wbgt31_h    = sum(wbgt >= 31, na.rm = TRUE),
    hot35_h     = sum(ta   >= 35, na.rm = TRUE),
    # simple workability windows (kept for continuity)
    work_day_pct   = mean(workability(wbgt, midpoint = midpoint95)[hour %in% H_DAY],   na.rm = TRUE),
    work_night_pct = mean(workability(wbgt, midpoint = midpoint95)[hour %in% H_NIGHT], na.rm = TRUE),
    work_all_pct   = mean(workability(wbgt, midpoint = midpoint95), na.rm = TRUE)
  ), by = .(scheme, village, date)]
  
  # --- daily enriched workability metrics (distribution, thresholds, schedulability, shifts) ---
  dly_work <- hr[, .(
    # distributional shape
    work_p05 = stats::quantile(work_pct, 0.05, na.rm = TRUE),
    work_p25 = stats::quantile(work_pct, 0.25, na.rm = TRUE),
    work_p50 = stats::quantile(work_pct, 0.50, na.rm = TRUE),
    work_p75 = stats::quantile(work_pct, 0.75, na.rm = TRUE),
    work_p95 = stats::quantile(work_pct, 0.95, na.rm = TRUE),
    work_iqr = stats::IQR(work_pct, na.rm = TRUE),
    # expected shortfall (worst 20% of hours)
    work_es20 = {
      n <- sum(!is.na(work_pct)); k <- ceiling(0.20 * n)
      if (k == 0) NA_real_ else mean(sort(work_pct, na.last = NA)[seq_len(k)], na.rm = TRUE)
    },
    # thresholds/time + dose
    hours_safe    = sum(band == "safe",    na.rm = TRUE),
    hours_caution = sum(band == "caution", na.rm = TRUE),
    hours_high    = sum(band == "high",    na.rm = TRUE),
    dh28_sum      = sum(dh_28, na.rm = TRUE),
    dh31_sum      = sum(dh_31, na.rm = TRUE),
    # schedulability targets (???70% / ???80%)
    max_block70_h = max_runlen(work_pct >= 70),
    max_block80_h = max_runlen(work_pct >= 80),
    n_blocks2h70  = count_runs_ge(work_pct >= 70, k = 2),
    n_blocks4h70  = count_runs_ge(work_pct >= 70, k = 4),
    # shift-aware windows
    work_morn = mean(work_pct[hour %in% H_MORN],  na.rm = TRUE),
    work_mid  = mean(work_pct[hour %in% H_MID],   na.rm = TRUE),
    work_aft  = mean(work_pct[hour %in% H_AFTER], na.rm = TRUE),
    work_nite = mean(work_pct[hour %in% H_NIGHT], na.rm = TRUE),
    # earliest feasible 4-h start for ???70% (NA if none)
    start4h70 = {
      ok <- rep(FALSE, 24L)
      for (h in 0:20) ok[h + 1L] <- all(work_pct[hour %in% h:(h + 3L)] >= 70, na.rm = TRUE)
      if (any(ok)) which(ok)[1] - 1L else NA_integer_
    },
    # work:rest effective minutes
    eff_work_min_light    = 60 * sum(wf_light,    na.rm = TRUE),
    eff_work_min_moderate = 60 * sum(wf_moderate, na.rm = TRUE),
    eff_work_min_heavy    = 60 * sum(wf_heavy,    na.rm = TRUE),
    # risk-weighted "loss area"
    loss_area = sum(loss_pct, na.rm = TRUE)
  ), by = .(scheme, village, date)]
  
  # --- disease day-level sums from hourly flags ---
  dh <- hr[, {
    f <- disease_hourly(ta, vpd, rh)
    .(palm_hours = sum(f$palm, na.rm = TRUE),
      anth_hours = sum(f$anth, na.rm = TRUE))
  }, by = .(scheme, village, date)]
  
  # --- combine daily pieces ---
  dly <- Reduce(function(x, y) merge(x, y, by = c("scheme","village","date"), all.x = TRUE),
                list(dly_base, dly_work, dh))
  
  # --- ensure year column and limit to 'yr' (defensive) ---
  dly[, year := lubridate::year(date)]
  hr[,  year := lubridate::year(date)]
  dly <- dly[year == yr]
  hr  <- hr[year == yr]
  
  # --- write parquet files (one per year), create dirs if missing ---
  hourly_dir <- file.path(stream_dir, "hourly_agg")
  daily_dir  <- file.path(stream_dir, "daily_agg")
  dir.create(hourly_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(daily_dir,  recursive = TRUE, showWarnings = FALSE)
  
  arrow::write_parquet(hr,  file.path(hourly_dir, sprintf("%d.parquet", yr)))
  arrow::write_parquet(dly, file.path(daily_dir,  sprintf("%d.parquet", yr)))
  
  invisible(NULL)
}


# list.files("C:/Users/jordan/R_Projects/CHI-Data", recursive=TRUE)

##########################################
# 5) RUN: mapping + per-year streaming
##########################################
weights_dt <- make_weights_centers(villages)
# sanity check: villages should have different nearest1 cells
# print(weights_dt[scheme=="nearest1"])
# 

years_all = 1980:2024
# 9 minutes sequential.
jj::timed('start')
for (yr in years_all) {
  message("Processing year: ", yr)
  try(process_year(yr, weights_dt), silent = TRUE)
}
jj::timed('end')



