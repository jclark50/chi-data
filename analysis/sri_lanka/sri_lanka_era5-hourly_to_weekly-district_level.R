
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(lubridate)
  library(here)
})


library(data.table)
library(sf)
library(terra)
library(exactextractr)
library(arrow)



##########################################
# # 0) CONFIG
##########################################

# ---- Load helper functions ----
source(here("helpers", "helpers.R"))
########################
########################

era5_root = "E:/data/gridded/era5-srilanka/processed"

# ---- Analysis settings ----
years_all   <- 1980:2024
tz_local      <- "Asia/Colombo"
tz_offset_sec <- 5.5 * 3600L
# 
# # ERA5 total precipitation "tp" in meters -> convert to mm
# precip_mm <- tp * 1000
# 



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


VAR_MTPR = 'mtpr_scaled10_kg2s1'
VAR_TP = 'tp_scaled10_m'


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

# 
# list.files(stream_dir, recursive=TRUE)
# library(dplyr)

# ?????? 1) Get the correct **districts (ADM2)** ???????????????????????????????????????????????????????????????????????????????????????????????????
gadm_zip <- "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_LKA_shp.zip"

tdir <- tempfile("lka_gadm41_"); dir.create(tdir)
zipfile <- file.path(tdir, "gadm41_LKA_shp.zip")
download.file(gadm_zip, destfile = zipfile, mode = "wb", quiet = TRUE)
unzip(zipfile, exdir = tdir)
adm2_shp <- list.files(tdir, pattern = "^gadm41_LKA_2\\.shp$", full.names = TRUE, recursive = TRUE)

districts_sf <- st_read(adm2_shp, quiet = TRUE)[, c("GID_2","NAME_1","NAME_2","geometry")]
names(districts_sf) <- c("gid2","district","subdistrict","geometry")


# Normalize names to your convention
norm_dist <- function(x) {
  x1 <- str_squish(str_to_title(x))
  x1 <- str_replace_all(x1, "Nuwara Eliya", "Nuwara-Eliya")
  x1
}
districts_sf$district <- norm_dist(districts_sf$district)

# Dissolve all polygons into province-level geometries
districts_sf <- districts_sf %>%
  group_by(district) %>%
  summarise(geometry = st_union(geometry), .groups = "drop")


# yr = 2010

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
  
  # names(ds)
  
  dt <- ds |>
    select(all_of(c(VAR_LATIDX, VAR_LONIDX, VAR_VALIDTIME,
                    VAR_SSRD, VAR_TA10, VAR_TD10, VAR_WBGT10, VAR_WIND10, 
                    VAR_MTPR, VAR_TP))) |>
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

      mtpr = round(cast(!!sym(VAR_MTPR),  float64()) / 10, 1)*3600, # already in mm # mean_total_precipitation_rate
      tp = round(cast(!!sym(VAR_TP),  float64()) / 10, 1)*1000, # convert m to mm # total precipitation
      
      rh   = exp((17.62 * td) / (243.12 + td) - (17.62 * ta) / (243.12 + ta)) * 100,
      es   = 0.6108 * exp(17.27 * ta / (ta + 237.3)),
      ea   = 0.6108 * exp(17.27 * td / (td + 237.3)),
      vpd  = if_else(es - ea > 0, es - ea, 0.0),
      
      ssrd_MJ = cast(!!sym(VAR_SSRD), float64()) / 1e6
    ) |>
    select(all_of(c(VAR_LATIDX, VAR_LONIDX)), date, hour, ta, td, wbgt, wind, rh, vpd, ssrd_MJ, mtpr, tp) |>
    collect()
  # 
  setDT(dt)
  # enforce integer indices
  setnames(dt, c(VAR_LATIDX, VAR_LONIDX), c("lat_idx_x4","lon_idx_x4"))
  dt[, `:=`(lat_idx_x4 = as.integer(lat_idx_x4)/4,
            lon_idx_x4 = as.integer(lon_idx_x4)/4)]
  dt[]
}



# ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# Area-weighted district-day ERA5 summaries for Sri Lanka (ADM2)
# Requires: your read_one_year_raw(yr) from the prompt (returns hourly grid data)
# Variables summarized: ta, td, wbgt, wind, rh, vpd, ssrd_MJ
# Output: data.table with district-day area-weighted means + mins + maxes
# ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????



sf_use_s2(FALSE)  # robust polygon intersections in projected CRS

# ?????? CONFIG ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# root of your ERA5 parquet years (as assumed by read_one_year_raw)
# era5_root <- "/path/to/era5/parquet"   # set in your environment already

# Use a UTM projection covering Sri Lanka for area calculations (meters)
CRS_AREA <- "EPSG:32644"   # UTM zone 44N
EA_CRS <- "EPSG:6933"   # World equal-area (your earlier choice)

# ?????? 0) District polygons (ADM2) ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
get_lka_districts <- function() {
  # ?????? 1) Get the correct **districts (ADM2)** ???????????????????????????????????????????????????????????????????????????????????????????????????
  gadm_zip <- "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_LKA_shp.zip"
  
  tdir <- tempfile("lka_gadm41_"); dir.create(tdir)
  zipfile <- file.path(tdir, "gadm41_LKA_shp.zip")
  download.file(gadm_zip, destfile = zipfile, mode = "wb", quiet = TRUE)
  unzip(zipfile, exdir = tdir)
  adm2_shp <- list.files(tdir, pattern = "^gadm41_LKA_2\\.shp$", full.names = TRUE, recursive = TRUE)
  
  districts_sf <- st_read(adm2_shp, quiet = TRUE)[, c("GID_2","NAME_1","NAME_2","geometry")]
  names(districts_sf) <- c("gid2","district","subdistrict","geometry")
  
  
  # Normalize names to your convention
  norm_dist <- function(x) {
    x1 <- str_squish(str_to_title(x))
    x1 <- str_replace_all(x1, "Nuwara Eliya", "Nuwara-Eliya")
    x1
  }
  districts_sf$district <- norm_dist(districts_sf$district)
  
  # # Dissolve all polygons into province-level geometries
  # districts_sf <- districts_sf %>%
  #   group_by(district) %>%
  #   summarise(geometry = st_union(geometry), .groups = "drop")
  # 
  sf_use_s2(TRUE)
  districts_sf <- districts_sf |>
    dplyr::group_by(district) |>
    dplyr::summarise(geometry = st_union(geometry), .groups = "drop")
  sf_use_s2(FALSE)  # if you prefer planar ops elsewhere

  return(districts_sf)

}

library(stringr)

districts_sf_wgs84 <- get_lka_districts()
# districts_sf_m <- st_transform(districts_sf_wgs84, CRS_AREA)  # for areas
districts_sf_m <- st_transform(districts_sf_wgs84, EA_CRS)


# ?????? 1) Build lat/lon lookup from parquet (indices ??? true centers) ??????????????????????????????????????????
# This assumes your parquet has numeric columns `lat` and `lon` alongside indices.
# If not, we must derive coords separately; stop with clear error.
build_latlon_lookup <- function(year_for_coords = NULL) {
  yr <- if (is.null(year_for_coords)) {
    # pick any year folder present
    yrs <- list.dirs(era5_root, recursive = FALSE, full.names = FALSE)
    yrs <- yrs[grepl("^\\d{4}$", yrs)]
    if (length(yrs) == 0) stop("No year directories found under era5_root.")
    yrs[1]
  } else as.character(year_for_coords)
  
  ds <- open_dataset(file.path(era5_root, yr), format = "parquet",
                     factory_options = list(exclude_invalid_files = TRUE))
  cols <- names(ds)
  needed <- c("lat_idx_x4", "lon_idx_x4")
  if (!all(needed %in% cols))
    stop("Parquet is missing one of: lat_idx_x4, lon_idx_x4, lat, lon. ",
         "Please add a coordinate lookup or include lat/lon in parquet.")
  
  coords <- ds |>
    select(all_of(needed)) |>
    distinct() |>
    collect()
  setDT(coords)
  coords[, `:=`(
    lat = as.integer(lat_idx_x4)/4,
    lon = as.integer(lon_idx_x4)/4,
    lat_idx_x4 = as.integer(lat_idx_x4)/4,
    lon_idx_x4 = as.integer(lon_idx_x4)/4
  )]
  coords[]
}

coords_lookup <- build_latlon_lookup()



# coords_lookup
# ?????? 2) Construct ERA5 grid-cell polygons (from center coords) ?????????????????????????????????????????????????????????
# We compute cell edges by midpoint between adjacent centers; edges at domain
# bounds are extrapolated by half the neighbor spacing. Build polygons in WGS84,
# then transform to meters CRS (CRS_AREA) to compute areas reliably.
make_cell_polygons <- function(coords_lookup) {
  # unique sorted centers
  ux <- sort(unique(coords_lookup$lon))
  uy <- sort(unique(coords_lookup$lat))
  
  # helper to build edges from centers
  to_edges <- function(v) {
    dv <- diff(v)
    left  <- c(v[1] - dv[1]/2, v[-length(v)] + dv/2)
    right <- c(v[-1] - dv/2,   v[length(v)] + dv[length(dv)]/2)
    # edges length is same as centers; we need N+1 edges, so stitch:
    edges <- numeric(length(v) + 1)
    edges[1] <- v[1] - dv[1]/2
    edges[2:length(v)] <- (v[-length(v)] + v[-1]) / 2
    edges[length(v)+1] <- v[length(v)] + dv[length(dv)]/2
    edges
  }
  
  ex <- to_edges(ux)  # length nx+1
  ey <- to_edges(uy)  # length ny+1
  
  # map each lon/lat center to column/row indices
  lon_map <- data.table(lon = ux, col = seq_along(ux))
  lat_map <- data.table(lat = uy, row = seq_along(uy))
  
  coords <- merge(coords_lookup, lon_map, by = "lon")
  coords <- merge(coords,       lat_map, by = "lat")
  
  # polygon corners for each cell
  coords[, `:=`(
    x_min = ex[col],
    x_max = ex[col + 1L],
    y_min = ey[row],
    y_max = ey[row + 1L]
  )]
  
  # build sf polygons
  polys <- lapply(seq_len(nrow(coords)), function(i) {
    with(coords[i], {
      st_polygon(list(matrix(
        c(x_min, y_min,
          x_max, y_min,
          x_max, y_max,
          x_min, y_max,
          x_min, y_min),
        ncol = 2, byrow = TRUE)))
    })
  })
  
  cell_sf <- st_sf(
    lat_idx_x4 = coords$lat_idx_x4,
    lon_idx_x4 = coords$lon_idx_x4,
    lon = coords$lon,
    lat = coords$lat,
    geometry = st_sfc(polys, crs = 4326)
  )
  cell_sf <- st_make_valid(cell_sf)
  
  # add cell areas in meters
  cell_sf_m <- st_transform(cell_sf, CRS_AREA)
  cell_sf_m$cell_area_m2 <- as.numeric(st_area(cell_sf_m))
  cell_sf_m
}

cell_sf <- make_cell_polygons(coords_lookup)


cell_sf_m <- st_transform(cell_sf, EA_CRS)
cell_sf_m$cell_area_m2 <- as.numeric(st_area(cell_sf_m))


# ?????? 3) Precompute static overlap areas (cell ??? district) ????????????????????????????????????????????????????????????????????????
# Weights are intersection area in m². Reuse across all years/days/variables.
compute_cell_district_weights <- function(cell_sf_m, districts_sf_m) {
  # candidate pairs via bbox intersects (fast)
  idx <- st_intersects(cell_sf_m, districts_sf_m, sparse = TRUE)
  
  # flatten pairs
  pairs <- data.table(
    cell_row = rep(seq_along(idx), lengths(idx)),
    dist_row = unlist(idx)
  )
  if (nrow(pairs) == 0) stop("No cell-district intersects; check coordinates.")
  
  # compute intersections batched by district to reduce duplication
  out_list <- vector("list", length = nrow(districts_sf_m))
  for (j in seq_len(nrow(districts_sf_m))) {
    pj <- pairs[dist_row == j]
    if (nrow(pj) == 0) next
    sub_cells <- cell_sf_m[pj$cell_row, c("lat_idx_x4","lon_idx_x4","cell_area_m2")]
    inter     <- suppressWarnings(st_intersection(sub_cells, districts_sf_m[j, ]))
    if (nrow(inter) == 0) next
    inter$area_m2 <- as.numeric(st_area(inter))
    # keep positive areas only
    inter <- inter[inter$area_m2 > 0, ]
    if (nrow(inter) == 0) next
    out_list[[j]] <- data.table(
      lat_idx_x4 = inter$lat_idx_x4,
      lon_idx_x4 = inter$lon_idx_x4,
      # gid2       = districts_sf_wgs84$gid2[j],
      district   = districts_sf_wgs84$district[j],
      area_m2    = inter$area_m2
    )
  }
  weights <- rbindlist(out_list, use.names = TRUE, fill = TRUE)
  # (Optional) sanity: some tiny slivers can appear; you may threshold
  # weights <- weights[area_m2 >= 25]  # drop < 25 m² slivers, for example
  setkey(weights, lat_idx_x4, lon_idx_x4, district)
  weights
}

# names(districts_sf_m)

weights <- compute_cell_district_weights(cell_sf_m, districts_sf_m)


# ?????? 4) Cell-day statistics from your hourly table ?????????????????????????????????????????????????????????????????????????????????????????????
# read_one_year_raw(yr) must return:
#   lat_idx_x4, lon_idx_x4 (int), date (Date, local or UTC-your function makes local),
#   hour, and variables: ta, td, wbgt, wind, rh, vpd, ssrd_MJ
# cell_day_stats <- function(dt_hourly) {
#   if (nrow(dt_hourly) == 0) return(dt_hourly)
#   vars <- c("ta","td","wbgt","wind","rh","vpd","ssrd_MJ")
#   # per cell per day stats
#   # agg <- dt_hourly[, c(
#   #   lapply(.SD, function(x) mean(x, na.rm = TRUE)),  # daily mean
#   #   lapply(.SD, function(x) min(x,  na.rm = TRUE)),  # daily min
#   #   lapply(.SD, function(x) max(x,  na.rm = TRUE))   # daily max
#   # ),
#   # by = .(lat_idx_x4, lon_idx_x4, date),
#   # .SDcols = vars]
#   agg <- dt_hourly[, c(
#     # daily mean/min/max and name them now
#     setNames(lapply(.SD, \(x) mean(x, na.rm = TRUE)), paste0(vars, "_mean")),
#     setNames(lapply(.SD, \(x) min(x,  na.rm = TRUE)), paste0(vars, "_min")),
#     setNames(lapply(.SD, \(x) max(x,  na.rm = TRUE)), paste0(vars, "_max"))
#   ), by = .(lat_idx_x4, lon_idx_x4, date), .SDcols = vars]
# 
#   # Rename columns to *_mean/_min/_max
#   setnames(agg,
#            old = names(agg)[-(1:3)],
#            new = c(paste0(vars, "_mean"),
#                    paste0(vars, "_min"),
#                    paste0(vars, "_max")))
#   agg
# }
cell_day_stats <- function(dt_hourly) {
  if (nrow(dt_hourly) == 0) return(dt_hourly)
  
  # vars with mean/min/max by day
  vars_mmm <- c("ta","td","wbgt","wind","rh","vpd","ssrd_MJ")
  # precip variables to SUM by day (mm/hour -> mm/day)
  vars_sum <- c("tp","mtpr")
  
  mmm <- dt_hourly[, c(
    setNames(lapply(.SD, \(x) mean(x, na.rm = TRUE)), paste0(vars_mmm, "_mean")),
    setNames(lapply(.SD, \(x) min(x,  na.rm = TRUE)), paste0(vars_mmm, "_min")),
    setNames(lapply(.SD, \(x) max(x,  na.rm = TRUE)), paste0(vars_mmm, "_max"))
  ), by = .(lat_idx_x4, lon_idx_x4, date), .SDcols = vars_mmm]
  
  sums <- dt_hourly[, c(
    setNames(lapply(.SD, \(x) sum(x, na.rm = TRUE)), paste0(vars_sum, "_sum"))
  ), by = .(lat_idx_x4, lon_idx_x4, date), .SDcols = vars_sum]
  
  out <- merge(mmm, sums, by = c("lat_idx_x4","lon_idx_x4","date"), all = TRUE)
  setorder(out, lat_idx_x4, lon_idx_x4, date)
  out[]
}


aw_mean <- function(val, w) {
  i <- is.finite(val)
  if (!any(i)) return(NA_real_)
  sum(val[i] * w[i]) / sum(w[i])
}


district_day_from_cell_day <- function(cell_daily, weights) {
  if (nrow(cell_daily) == 0) return(cell_daily)
  x <- merge(cell_daily, weights,
             by = c("lat_idx_x4","lon_idx_x4"),
             allow.cartesian = TRUE)
  
  vars_mmm <- c("ta","td","wbgt","wind","rh","vpd","ssrd_MJ")
  
  x[, {
    # area-weighted means for mean variables
    means <- setNames(
      lapply(vars_mmm, \(v) aw_mean(get(paste0(v, "_mean")), area_m2)),
      paste0(vars_mmm, "_mean")
    )
    mins  <- setNames(
      lapply(vars_mmm, \(v) suppressWarnings(min(get(paste0(v, "_min")), na.rm = TRUE))),
      paste0(vars_mmm, "_min")
    )
    maxs  <- setNames(
      lapply(vars_mmm, \(v) suppressWarnings(max(get(paste0(v, "_max")), na.rm = TRUE))),
      paste0(vars_mmm, "_max")
    )
    # area-weighted means of DAILY SUMS (mm/day over district)
    tp_aw    <- aw_mean(tp_sum,   area_m2)
    mtpr_aw  <- aw_mean(mtpr_sum, area_m2)
    
    as.list(c(
      means, mins, maxs,
      tp_sum_aw = tp_aw,
      mtpr_sum_aw = mtpr_aw
    ))
  },
  by = .(district, date)
  ][order(district, date)]
}




# ?????? 6) Driver to run one or many years ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
summarize_years_area_weighted <- function(years) {
  res_list <- vector("list", length(years))
  for (i in seq_along(years)) {
    yr <- years[i]
    message("Processing ", yr, " .")
    hr <- read_one_year_raw(yr)  # from your code; must include indices + vars
    if (!all(c("lat_idx_x4","lon_idx_x4","date") %in% names(hr)))
      stop("read_one_year_raw(", yr, ") must return lat_idx_x4, lon_idx_x4, date.")
    cd <- cell_day_stats(hr)
    dd <- district_day_from_cell_day(cd, weights)
    class(dd)
    dd[, year := yr]
    res_list[[i]] <- dd
  }
  rbindlist(res_list, use.names = TRUE, fill = TRUE)
}

jj::timed('start')
years< - 2006:2024
out <- summarize_years_area_weighted(years)
jj::timed('end')

fwrite(out, "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/srilanka_district_daily_era5_areawt.csv")
# out[district == 'Addalachchenai']


out[,.N,by=district][order(district)]

sf_use_s2(TRUE)







# ================== WEEKLY WEATHER FEATURE PIPELINE ==========================
# Robust weekly aggregation for district×week epidemiology
# Author: you :)
# Packages
suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
})

# ----------------------------- Utilities -------------------------------------
# Safe helpers
q_na <- function(x, p) if (all(is.na(x))) NA_real_ else as.numeric(quantile(x, p, na.rm = TRUE))
mean_na <- function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
sum_na  <- function(x) if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)

winsor <- function(x, probs = c(0.01, 0.99)) {
  if (all(is.na(x))) return(x)
  qs <- quantile(x, probs = probs, na.rm = TRUE)
  pmin(pmax(x, qs[1]), qs[2])
}

# Vapor Pressure Deficit (approx) from Tmean (°C) and RH (%)
# es (kPa) = 0.6108*exp(17.27*T/(T+237.3)), ea = es*(RH/100); VPD = es - ea
vpd_from_t_rh <- function(t_mean_c, rh_pct) {
  es <- 0.6108 * exp(17.27 * t_mean_c / (t_mean_c + 237.3))
  ea <- es * (pmin(pmax(rh_pct, 0), 100) / 100)
  es - ea
}

# Longest wet spell in a vector of daily precip values, threshold in mm
longest_wet_spell <- function(prec, thr = 10) {
  if (all(is.na(prec))) return(NA_real_)
  wet <- as.integer(prec >= thr)
  rle_wet <- rle(wet)
  if (!length(rle_wet$lengths)) return(0)
  max(c(0, rle_wet$lengths[rle_wet$values == 1]))
}

# ISO week-of-year for the epi week (use end date convention)
iso_week <- function(d) as.integer(strftime(d, format = "%V"))

# ------------------------- Parameter block -----------------------------------
CFG <- list(
  # outlier control on daily inputs
  WINSORIZE = TRUE,
  WINSOR_PROBS = c(0.01, 0.99),
  
  # extremes / thresholds
  HOT_TMAX = 32,     # °C threshold for "hot day"
  WET_MM   = 10,     # mm/day threshold for wet day (for both tp_sum & mtpr_sum)
  
  # lags/rolling windows (in weeks)
  MAX_LAG_WEEKS = 6,
  ROLL_WINDOWS  = c(2, 4),
  
  # EWAP precipitation (antecedent precipitation index)
  EWAP_ALPHA = 0.8,  # 0.7-0.9 typical
  EWAP_K     = 4,    # weeks back included
  
  # anomalies: climatology by district × week-of-year
  ANOMALY_VARS = c("precip_tp_sum_week", "tmax_mean", "rh_mean_week"),
  
  # within-district standardization (optional)
  ZSCORE_VARS = c("precip_tp_sum_week", "precip_mtpr_sum_week", "tmax_mean", "vpd_mean_week"),
  
  # coverage rule
  MIN_DAYS_PER_WEEK = 5
)


# --------------------- Main weekly aggregation function ----------------------
# daily_dt: district,date + daily ERA5 variables
# weeks_dt: district,date_start,date_end for epidemiological weeks (inclusive)
weekly_weather_features <- function(daily_dt, weeks_dt, cfg = CFG) {
  must_daily <- c(
    "district","date","ta_mean","td_mean","wbgt_mean","wind_mean","rh_mean","vpd_mean","ssrd_MJ_mean",
    "ta_min","td_min","wbgt_min","wind_min","rh_min","vpd_min","ssrd_MJ_min",
    "ta_max","td_max","wbgt_max","wind_max","rh_max","vpd_max","ssrd_MJ_max",
    "tp_sum","mtpr_sum","year"
  )
  stopifnot(all(c("district","date") %in% names(daily_dt)))
  # we won't stop on all daily vars-some might be NA-only; but we do check key ones exist:
  need_now <- c("ta_max","ta_min","ta_mean","wbgt_mean","rh_mean","vpd_mean","tp_sum","mtpr_sum","ssrd_MJ_mean")
  stopifnot(all(need_now %in% names(daily_dt)))
  stopifnot(all(c("district","date_start","date_end") %in% names(weeks_dt)))
  
  DTd <- as.data.table(copy(daily_dt))
  DTw <- as.data.table(copy(weeks_dt))
  
  # Coerce dates
  if (!inherits(DTd$date, "Date")) DTd[,  date := as.Date(date)]
  if (!inherits(DTw$date_start, "Date")) DTw[, date_start := as.Date(date_start)]
  if (!inherits(DTw$date_end, "Date"))   DTw[, date_end   := as.Date(date_end)]
  
  # Optional winsorization (only on sensible numeric daily vars you have)
  num_cols <- intersect(
    c("ta_mean","ta_min","ta_max","wbgt_mean","rh_mean","vpd_mean",
      "ssrd_MJ_mean","tp_sum","mtpr_sum"),
    names(DTd)
  )
  if (cfg$WINSORIZE && length(num_cols)) {
    DTd[, (num_cols) := lapply(.SD, winsor, probs = cfg$WINSOR_PROBS), .SDcols = num_cols]
  }
  
  # Build week index, expand to days, and join
  idx <- DTw[, .(district, week_id = .I,
                 date_start, date_end,
                 year = year(date_end),
                 week_of_year = iso_week(date_end))]
  wk_days <- idx[, .(date = seq(date_start, date_end, by = "1 day")),
                 by = .(district, week_id, date_start, date_end, year, week_of_year)]
  
  setkey(DTd, district, date)
  setkey(wk_days, district, date)
  wk <- DTd[wk_days, on = .(district, date)]
  
  library(zoo)  # for rollapplyr
  
  library(zoo)  # for rollapplyr
  
  agg <- wk[, {
    ndays <- sum(!is.na(date))
    
    # Vectors
    tmax <- ta_max; tmin <- ta_min; tmean <- ta_mean
    wbgt <- wbgt_mean; rh <- rh_mean; vpd <- vpd_mean
    ssrd <- ssrd_MJ_mean
    tp   <- tp_sum
    mtpr <- mtpr_sum
    
    # Rolling 3-day sums within this **week window only**
    max3_tp   <- if (sum(!is.na(tp))   >= 3) max(rollapplyr(tp,   3, sum, na.rm = TRUE, partial = FALSE), na.rm = TRUE) else NA_real_
    max3_mtpr <- if (sum(!is.na(mtpr)) >= 3) max(rollapplyr(mtpr, 3, sum, na.rm = TRUE, partial = FALSE), na.rm = TRUE) else NA_real_
    
    res <- list(
      n_days_week          = as.integer(ndays),
      
      # temperature
      tmax_mean            = mean_na(tmax),
      tmax_p90             = q_na(tmax, 0.90),
      tmax_p95             = q_na(tmax, 0.95),
      tmax_range           = if (all(is.na(tmax))) NA_real_ else max(tmax, na.rm = TRUE) - min(tmax, na.rm = TRUE),
      tmin_mean            = mean_na(tmin),
      tmean_mean           = mean_na(tmean),
      wbgt_mean_week       = mean_na(wbgt),
      
      # humidity & vpd
      rh_mean_week         = mean_na(rh),
      vpd_mean_week        = mean_na(vpd),
      
      # radiation
      ssrd_MJ_mean_week    = mean_na(ssrd),
      
      # precipitation weekly sums
      precip_tp_sum_week   = sum_na(tp),
      precip_mtpr_sum_week = sum_na(mtpr),
      
      # wet days (>= threshold) - coerce to numeric for type stability
      wet_days_ge10_tp     = as.numeric(sum(tp   >= cfg$WET_MM, na.rm = TRUE)),
      wet_days_ge10_mtpr   = as.numeric(sum(mtpr >= cfg$WET_MM, na.rm = TRUE)),
      
      # rolling 3-day max within week
      max3d_tp             = max3_tp,
      max3d_mtpr           = max3_mtpr,
      
      # longest wet spell - returns numeric
      wet_spell_maxlen_tp   = as.numeric(longest_wet_spell(tp,   thr = cfg$WET_MM)),
      wet_spell_maxlen_mtpr = as.numeric(longest_wet_spell(mtpr, thr = cfg$WET_MM)),
      
      # heat exceedances - coerce to numeric
      hot_days_ge32        = as.numeric(sum(ta_max >= cfg$HOT_TMAX, na.rm = TRUE))
    )
    
    # If insufficient coverage, mask everything except n_days_week
    if (ndays < cfg$MIN_DAYS_PER_WEEK) {
      keep <- "n_days_week"
      res[names(res) != keep] <- lapply(res[names(res) != keep], function(.) NA_real_)
    }
    
    res
  }, by = .(district, week_id, date_start, date_end, year, week_of_year)]
  
  # 
  # # Lags & rolling windows (past-only) for selected vars
  setorder(agg, district, date_end)
  LAG_VARS <- intersect(
    c("precip_tp_sum_week","precip_mtpr_sum_week","rh_mean_week","tmax_mean","vpd_mean_week"),
    names(agg)
  )
  for (v in LAG_VARS) {
    # Lags
    for (L in seq_len(cfg$MAX_LAG_WEEKS)) {
      agg[, paste0(v, "_lag", L) := shift(get(v), L), by = district]
    }
    # Rolling windows (include current week)
    for (k in cfg$ROLL_WINDOWS) {
      agg[, paste0(v, "_roll", k, "w_mean") :=
            frollmean(get(v), k, align = "right", na.rm = TRUE), by = district]
      agg[, paste0(v, "_roll", k, "w_sum")  :=
            frollsum(get(v),  k, align = "right", na.rm = TRUE), by = district]
    }
  }
  
  # EWAP (antecedent) for both precip metrics
  K <- cfg$EWAP_K; a <- cfg$EWAP_ALPHA
  if ("precip_tp_sum_week" %in% names(agg)) {
    agg[, ewap_tp := {
      x <- precip_tp_sum_week
      ew <- x
      for (k in 1:K) ew <- ew + (a^k) * shift(x, k)
      ew
    }, by = district]
  }
  if ("precip_mtpr_sum_week" %in% names(agg)) {
    agg[, ewap_mtpr := {
      x <- precip_mtpr_sum_week
      ew <- x
      for (k in 1:K) ew <- ew + (a^k) * shift(x, k)
      ew
    }, by = district]
  }
  
  # Weekly climatology & anomalies (district × week_of_year)
  CLIM_VARS <- intersect(cfg$ANOMALY_VARS, names(agg))
  if (length(CLIM_VARS)) {
    clim <- agg[, lapply(.SD, mean_na), by = .(district, week_of_year), .SDcols = CLIM_VARS]
    setnames(clim, CLIM_VARS, paste0(CLIM_VARS, "_clim"))
    agg <- clim[agg, on = .(district, week_of_year)]
    for (v in CLIM_VARS) {
      vc <- paste0(v, "_clim")
      agg[, paste0(v, "_anom") := get(v) - get(vc)]
      agg[, paste0(v, "_pct_normal") := ifelse(get(vc) > 0, get(v)/get(vc), NA_real_)]
    }
  }
  
  # Optional within-district z-scores
  ZV <- intersect(cfg$ZSCORE_VARS, names(agg))
  if (length(ZV)) {
    for (v in ZV) {
      agg[, paste0(v, "_z") := {
        xv <- get(v); mu <- mean(xv, na.rm = TRUE); sdv <- sd(xv, na.rm = TRUE)
        ifelse(is.finite(sdv) & sdv > 0, (xv - mu)/sdv, NA_real_)
      }, by = district]
    }
  }
  
  setorder(agg, district, date_end)
  agg[]
}

# ------------------------------- Example -------------------------------------
# Example usage (replace with your real frames):
daily_dt <- fread("C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/srilanka_district_daily_era5_areawt.csv")       # district, date, daily vars...
names(daily_dt) <- gsub("_aw","",names(daily_dt))
# daily_dt$ta_max
names(daily_dt)
weeks_dt <- unique(lepto[, .(district, date_start, date_end)])  # from your case data

features_weekly <- weekly_weather_features(daily_dt, weeks_dt)
names(features_weekly)


fwrite(features_weekly, "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/srilanka_district_weekly_era5_areawt.csv")

features_weekly = fread("C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/srilanka_district_weekly_era5_areawt.csv")


# Join back onto your epi table (district×week outcomes):
lepto_feat <- features_weekly[lepto, on = .(district, date_start, date_end)]
# Now model with Poisson/NB using offset(log(poptot)) and the features you like.










