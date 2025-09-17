library(arrow)
library(data.table)
library(dplyr)
library(lubridate)
library(sf)
library(jj)  # if you use j.datetime or j.date
library(future.apply)

# # Set up paths
# data_dir <- "E:/data/gridded/era5land/processed"
# data_dir <- "E:/data/gridded/era5land/counties-output/gridded_output_parquet-202507/hourly"
# # grid_map_file <- "G:/My Drive/R/R_Projects/ERA5/grid_to_geograhpy_dt-counties-output-202504.Rds"
# # output_dir <- "E:/data/gridded/era5land/counties-output/gridded_output_parquet-202505/hourly"
# grid_map_file <- "G:/My Drive/R/R_Projects/ERA5/grid_to_geograhpy_dt-counties-output-202507.Rds"



data_dir <- "E:/data/gridded/era5-africa/processed/"
# DERIVED_ROOT <- "E:/data/gridded/era5-africa/daily/"
# dir.create(DERIVED_ROOT)

# 
# 
# 
# output_dir <- "C:/data/gridded/era5land/counties_ben/"
# dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)


# add this:
# output_csv_dir  <- file.path(output_dir, "state_csvs")
# dir.create(output_csv_dir, recursive = TRUE, showWarnings = FALSE)

# file.exists(grid_map_file)


# # Load spatial weights (lat, lon, GISJOIN, weight)
# grid_map <- readRDS(grid_map_file)
# grid_map[, `:=`(lat = round(lat, 4), lon = round(lon, 4))]
# setkey(grid_map, lat, lon)

# Define years and monthsf
years <- 2000:2024
months <- 1:12



yr = 2005
mo = 1


plan(multisession, workers = 3)
# yr = 2020
future_lapply(years, function(yr) {
  ds <- open_dataset(file.path(data_dir, yr), format = "parquet")
  # names(ds)
  
  
  # ds %>% head(1) %>% collect()

  dt <- ds %>%
    select(
      lat_idx_x4, lon_idx_x4, validTime,
      ta_scaled10_degC, wbgt_scaled10_degC, td_scaled10_degC, wind2m_scaled10_ms1
    ) %>%
    mutate(
      # 1) int32 epoch seconds -> int64 -> UTC timestamp (Arrow-native)
      validTime = cast(cast(validTime, int64()), timestamp("s", timezone = "UTC")),
      
      # 2) rescale variables (lazy in Arrow)
      ta     = round(cast(ta_scaled10_degC,    float64()) / 10, 1),
      wbgt   = round(cast(wbgt_scaled10_degC,  float64()) / 10, 1),
      td     = round(cast(td_scaled10_degC,    float64()) / 10, 1),
      wind2m = round(cast(wind2m_scaled10_ms1, float64()) / 10, 1),
      
      # 3) relative humidity
      rh = exp((17.62 * td) / (243.12 + td) - (17.62 * ta) / (243.12 + ta)) * 100
      # ,
      # 
      # # 4) crude longitude-based offset (whole hours)
      # tz_offset_hours = floor(((lon_idx_x4 / 4) + 7.5) / 15)
      # 
      # 
      # # 5) add offset in seconds in int64 space, cast back to timestamp
      # # local_ts = cast(
      # #   cast(validTime, int64()) + (cast(tz_offset_hours, int64()) * 3600L),
      # #   timestamp("s")
      # # ),
      # # Madagascar fixed offset: UTC+3 => 10800 seconds (works lazily)
      # local_ts = cast(cast(validTime, int64()) + 10800L, timestamp("s")),
      # localdate = cast(local_ts, date32())
      # 
    ) %>%
    select(
      lat_idx_x4, lon_idx_x4,
      validTime, 
      # local_ts, localdate,
      ta, wbgt, rh, td, wind2m
    )
  
  
  dt = dt %>% collect()

  attr(dt$validTime, "tzone") <- 'Indian/Antananarivo'
  
  dt = dt[order(validTime)]
  
  
  # Packages
  library(data.table)
  library(terra)          # raster & vector ops
  library(sf)             # for IO/plotting if needed
  library(rnaturalearth)  # Madagascar outline
  library(rnaturalearthdata)
  
  # --- 1) Annual mean WBGT per grid cell (data.table) --------------------------
  # dt has: lat_idx_x4, lon_idx_x4, validTime (POSIXct), ta, wbgt, rh, td, wind2m
  
  # Pick your target year (or derive from validTime)
  # target_year <- 2020L
  
  dt_year <- dt
  
  
  dt_year$lat = dt_year$lat_idx_x4 / 4
  dt_year$lon = dt_year$lon_idx_x4 / 4
  
  # Aggregate to annual mean per grid index
  avg_dt <- dt_year[
    ,
    .(wbgt_mean = max(wbgt, na.rm = TRUE)),
    by = .(lat, lon)
  ]
  

  
  
  # 2) Get daily maximum WBGT per grid cell
  dt_dailymax <- dt_year[
    ,
    .(wbgt_max = max(wbgt, na.rm = TRUE)),
    by = .(lat, lon, date = as.Date(validTime))
  ]
  
  # 3) Count days where daily max WBGT > 32 °C
  dt_days_over32 <- dt_dailymax[
    wbgt_max > 30,
    .(days_over_32 = .N),
    by = .(lat, lon)
  ]
  

  # --- 3) Build a regular SpatRaster on EPSG:4326 ------------------------------
  # Derive grid resolution from unique coords (assumes regular grid)
  lon_vals <- sort(unique(dt_days_over32$lon))
  lat_vals <- sort(unique(dt_days_over32$lat))
  
  # Use first forward difference as resolution (works on regular grids)
  res_lon <- unique(round(diff(lon_vals), 6))[1]
  res_lat <- unique(round(diff(lat_vals), 6))[1]
  
  # Construct empty raster covering the grid (inclusive)
  r_template <- rast(
    xmin = min(lon_vals),
    xmax = max(lon_vals),
    ymin = min(lat_vals),
    ymax = max(lat_vals),
    crs  = "EPSG:4326",
    resolution = c(res_lon, res_lat)
  )
  
  # Fill raster cells from point table
  xy <- as.matrix(dt_days_over32[, .(lon, lat)])
  cell_ids <- cellFromXY(r_template, xy)  # terra version
  
  # Initialize with NA and insert values where we have data
  r_mean <- r_template
  values(r_mean) <- NA_real_
  # r_mean[cell_ids] <- avg_dt$wbgt_mean
  r_mean[cell_ids] <- dt_days_over32$days_over_32
  

  
  names(r_mean) <- sprintf("wbgt_mean_%d", target_year)
  
  # --- 4) Madagascar outline, crop & mask --------------------------------------
  mada_sf  <- rnaturalearth::ne_countries(country = "Madagascar", scale = "large", returnclass = "sf")
  mada_vect <- vect(mada_sf)             # terra SpatVector
  
  # Align extents and mask to the country boundary
  # r_mada <- mask(crop(r_mean, mada_vect), mada_vect)
  r_mada <- r_mean #  mask(r_mean, mada_vect)
  
  plot(r_mean, main = sprintf("Annual Mean WBGT (°C), %d - Madagascar", target_year))
  lines(mada_vect, lwd = 1.2)
  
  
  
  # --- 5) (Optional) quick plot -------------------------------------------------
  # terra native plot

  # helper to build a Gaussian kernel
  gaussian_kernel <- function(size, sigma) {
    m      <- matrix(0, size, size)
    center <- (size + 1) / 2
    for (i in seq_len(size)) {
      for (j in seq_len(size)) {
        m[i, j] <- exp(-(((i - center)^2 + (j - center)^2) / (2 * sigma^2)))
      }
    }
    m / sum(m)
  }
  
  # e.g. 5×5 kernel with sigma = 1
  gauss_w <- gaussian_kernel(5, sigma = 8)
  r_gauss <- focal(
    r_mada,
    w         = gauss_w,
    fun       = sum,
    na.policy = "omit",
    pad       = TRUE
  )
  
  
  plot(r_gauss, main = sprintf("Annual Mean WBGT (°C), %d - Madagascar", target_year))
  lines(mada_vect, lwd = 1.2)
  
  
  
  

  
  
  
  
  
  
  
  
  
  
  
  library(terra)
  
  # 1) get range without pulling full raster into memory
  rng  <- as.numeric(global(r_gauss, "range", na.rm = TRUE))
  brks <- pretty(rng, n = 9)                 # nice, even legend bins
  
  # 2) red heat palette (light yellow -> deep red)
  pal  <- hcl.colors(length(brks) - 1, palette = "YlOrRd", rev = FALSE)
  
  # 3) pretty plot
  plot(
    r_gauss,
    col    = pal,
    breaks = brks,
    axes   = TRUE,
    mar    = c(3, 3, 2.5, 6),                # room for legend
    plg    = list(title = "WBGT (°C)", cex = 0.9, shrink = 0.8, frame = FALSE),
    main   = sprintf("Annual Mean WBGT - Madagascar (%d)", target_year)
  )
  
  # 4) country outline + subtle graticule
  lines(mada_vect, lwd = 1.3, col = "grey20")
  grat <- graticule(r_gauss, lon = seq(40, 55, by = 2), lat = seq(-28, -10, by = 2))
  lines(grat, col = "grey80", lwd = 0.6, lty = 3)
  
  lines(mada_vect, lwd = 1.2)
  
  
  
  # --- 6) (Optional) write to GeoTIFF ------------------------------------------
  # writeRaster(r_mada, filename = sprintf("wbgt_mean_%d_madagascar.tif", target_year), overwrite = TRUE)
  
  # --- 7) (Optional) get a SpatVector of cell centers for mapping points -------
  # pts <- spatSample(r_mada, size = ncell(r_mada), method = "regular", as.points = TRUE)
  # pts <- as.points(r_mada)  # or use avg_dt as sf:
  # pts_sf <- st_as_sf(data.frame(avg_dt), coords = c("lon", "lat"), crs = 4326)
  # cleanup for next iteration
  # result[ , STATEFP := NULL]
})

message("All done! CSV files are in:\n  ", normalizePath(output_csv_dir))

















#-----------------------------------------------------------------------------#
# 5) ZIP each year's CSV folder
#-----------------------------------------------------------------------------#

years <- 2000:2023
for (yr in years) {
  year_dir <- file.path(output_csv_dir, as.character(yr))
  if (!dir.exists(year_dir)) next  # skip years with no files
  
  # all CSVs under that year (including sub-folders for months)
  csv_files <- list.files(
    path       = year_dir,
    pattern    = "\\.csv$",
    full.names = TRUE,
    recursive  = TRUE
  )
  
  # path for the ZIP (in your main output_dir)
  zip_path <- file.path(
    output_dir,
    sprintf("wbgt_%d_state_csvs.zip", yr)
  )
  
  # create the ZIP; uses the system zip (utils::zip)
  utils::zip(
    zipfile = zip_path,
    files   = csv_files,
    flags   = "-j"    # omit directory structure inside the zip; remove if you want to preserve folders
  )
  
  message("Zipped ", length(csv_files), " CSVs ??? ", zip_path)
}







years <- 2009:2023



library(future.apply)
plan(multisession, workers = 4)   # adjust number of workers to your CPU

# Parallel ZIP over years
future_lapply(years, function(yr) {
  year_dir <- file.path(output_csv_dir, as.character(yr))
  if (!dir.exists(year_dir)) {
    message("Skipping ", yr, " (no folder)")
    return(NULL)
  }
  
  # find all CSVs under that year (including months)
  csv_files <- list.files(
    path       = year_dir,
    pattern    = "\\.csv$",
    full.names = TRUE,
    recursive  = TRUE
  )
  if (length(csv_files) == 0) {
    message("No CSVs for ", yr)
    return(NULL)
  }
  
  zip_path <- file.path(
    output_dir,
    sprintf("wbgt_%d_state_csvs.zip", yr)
  )
  
  # zip them (drop folder structure; remove flags = "-j" to keep it)
  utils::zip(
    zipfile = zip_path,
    files   = csv_files,
    flags   = "-j"
  )
  
  message("Zipped ", length(csv_files), " CSVs ??? ", zip_path)
})










