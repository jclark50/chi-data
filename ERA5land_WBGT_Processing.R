####################################################################################################
# Script Name: ERA5land_WBGT_Final_Processing_20250309.R
#
# Author: Jordan
# Date Created: [Insert Creation Date]
# Last Updated: March 09, 2025
#
# Purpose:
#   This script processes raw ERA5 GRIB files by converting them to NetCDF, extracting key meteorological
#   variables, and computing the Wet Bulb Globe Temperature (WBGT) using a combination of R and optimized
#   C++ routines. Processed data is saved as Parquet files for further analysis.
#
#   NOTE:
#     Downstream derivations for county-level or census tract-level metrics are performed in separate 
#     scripts:
#       * era5-hourly-daily-censustract-20250306.R
#       * era5-hourly-daily-counties-20250224-V3.R
#
# Requirements:
#   - R (version 4.x or later recommended)
#   - Required R packages: data.table, wbgt, jj, arrow, ncdf4, units, Rcpp, RcppParallel, foreach, doParallel
#   - External dependency: C++ source file ("wbgt_cpp.cpp") located at the specified path.
#   - System: Windows with WSL configured for GRIB-to-NetCDF conversion.
#
# Usage:
#   1. Configure input parameters (e.g., file paths, year/month ranges, number of threads).
#   2. Run this script to process ERA5 data and compute WBGT.
#   3. Processed data is saved as Parquet files for downstream analysis.
#
# Revision History:
#   - [Insert Creation Date]: Initial version.
#   - March 09, 2025: Updated for improved error handling, unit conversion, and parallel processing.
#
####################################################################################################


library(data.table)
# library(parallel)
library(ncdf4)
library(wbgt)  
library(jj)
library(furrr)
library(arrow)
library(foreach)
library(doParallel)

# ================================================
# =            SET UP PARAMETERS                 =
# ================================================
# years <- c(1975:2024)
# hawaii remaining
years <- c(1980:2024)

# # alaska remaining
# years <- c(2018:2019, 2021:2024)

months <- 1:12

# Create a complete grid of year/month combinations.
expected_grid <- CJ(year = years, month = months)

# Define the root directory for raw GRIB files.
# rootdir <- "F:/data/gridded/era5land/raw/"


# ================================================

base_dirs <- c( "C:/data/era5land", "E:/data/gridded/era5land",
                "F:/data/gridded/era5land/", "P:/data/gridded/era5land")

rootdir = c("C:/data/era5land", "E:/data/gridded/era5land",
            "F:/data/gridded/era5land/","F:/data/gridded/era5/","P:/data/gridded/era5land","F:/data/gridded/era5_se_ak") #,

rootdir = c("F:/data/gridded/era5") #,

rootdir = c("F:/data/gridded/era5/raw2") #,


rootdir = c("E:/data/gridded/era5-africa/raw/") #,



era5_wbgt_cppfile = "G:/My Drive/R/R Codes/Package/rcpp_codes/wbgt_cpp.cpp"

# "2024-12-09 21:07:47 EST"
# took 26 minutes with 2 cluster parallellization of wbgt calc chunk_dt thing.
# took 45 minutes for one month when not doing any parallelization on wbgt calculation...

# 
# area_str = 'alaska'
# era5type = 'era5'

era5_grib_process <- function(grib_file, year, month, redo = FALSE, 
                              redo_wsl_netcdf = FALSE,
                              remove_netcdf = FALSE,
                              processed_out_dir = "E:/data/gridded/era5land/processed/",
                              log_file = "C:/Users/jordan/Desktop/progresstracker.txt",
                              numOfThreads_forWBGTcalc = 7,
                              era5type = 'era5land',
                              era5vars = NULL,
                              area_str = "N21W128S52E66") {
  
  jj::timed('start')
  ###############################################
  ### ======================================== ###
  ###          LOGGING HELPER FUNCTION         ###
  ### ======================================== ###
  ###############################################
  log_progress <- function(log_file, year, month, step, status, additional_info = "") {
    timestamp <- Sys.time()
    log_entry <- data.frame(
      year = year,
      month = month,
      step = step,
      timestamp = as.character(timestamp),
      status = status,
      additional_info = additional_info,
      stringsAsFactors = FALSE
    )
    if (!file.exists(log_file)) {
      write.table(log_entry, log_file, append = FALSE, sep = ",", row.names = FALSE, col.names = TRUE)
    } else {
      write.table(log_entry, log_file, append = TRUE, sep = ",", row.names = FALSE, col.names = FALSE)
    }
  }
  
  ###############################################
  ### ======================================== ###
  ###         INITIAL SETUP & LIBRARIES        ###
  ### ======================================== ###
  ###############################################
  startTime <- Sys.time()
  
  
  library(foreach)
  library(doParallel)
  library(units)
  library(Rcpp)
  library(RcppParallel)
  library(arrow)
  library(ncdf4)  # For nc_open, ncvar_get, etc.
  
  outFileNameBase <- sprintf("%s_%s_%s_%s", era5type, year, charnum(month), area_str)
  
  # Use the provided GRIB file list and process the first file.
  # grib_file <- grib_files[[1]]
  tempDir <- dirname(grib_file)
  
  wbgtFileName <- file.path(processed_out_dir, year, charnum(month),
                            paste0(j.strsplit(basename(grib_file), "_allvars", 1),
                                   paste0("_", area_str, "_wbgt.Rds")))
  parquet_dir <- file.path(dirname(wbgtFileName), "parquet")
  parquet_file <- file.path(parquet_dir, gsub(".Rds", "_gridded_processed_wbgtcpp.parquet", basename(wbgtFileName)))
  
  # file.exists(parquet_file)
  ###############################################
  ### ======================================== ###
  ###      CHECK FOR EXISTING PARQUET FILE     ###
  # ### ======================================== ###
  # ###############################################
  # if (file.exists(parquet_file) && !redo) {
  #   if (file.info(parquet_file)$size < 2000) {
  #     file.remove(parquet_file)
  #     log_progress(log_file, year, month, "REDOING PARQUET", "REDOING", as.character(Sys.time()))
  #   } else {
  #     log_progress(log_file, year, month, "Parquet file already done", "DONE", as.character(Sys.time()))
  #     return()
  #   }
  # }
  # 
  ###############################################
  ### ======================================== ###
  ###      GRIB TO NETCDF CONVERSION SETUP     ###
  ### ======================================== ###
  ###############################################
  wsl_grib_to_netcdf <- function(input_grib, output_nc) {
    input_grib <- gsub("E:/", "/mnt/e/", input_grib)
    input_grib <- gsub("F:/", "/mnt/f/", input_grib)
    input_grib <- gsub("C:/", "/mnt/c/", input_grib)
    output_nc <- gsub("E:/", "/mnt/e/", output_nc)
    output_nc <- gsub("F:/", "/mnt/f/", output_nc)
    output_nc <- gsub("C:/", "/mnt/c/", output_nc)
    system(paste('wsl', "/usr/bin/grib_to_netcdf -k 1 -o", output_nc, input_grib, sep = " "))
  }
  
  netcdf_path <- file.path(processed_out_dir, year, charnum(month), 
                           gsub(".grib", sprintf("%s.nc", area_str), basename(grib_file)))
  dir.create(dirname(netcdf_path), recursive = TRUE, showWarnings = FALSE)
  
  ###############################################
  ### ======================================== ###
  ###  NETCDF FILE VALIDATION & CONVERSION STEP  ###
  ### ======================================== ###
  ###############################################
  if (file.exists(netcdf_path) && file.info(netcdf_path)$size < 500000) {
    log_progress(log_file, year, month, "recreating_netcdf", "netcdf", as.character(Sys.time()))
    file.remove(netcdf_path)
  }
  
  if (!file.exists(netcdf_path) || redo_wsl_netcdf) {
    setwd("C:/Users/jordan/Desktop/")
    wsl_grib_to_netcdf(grib_file, netcdf_path)
  }
  
  ###############################################
  ### ======================================== ###
  ###    OPEN NETCDF FILE & EXTRACT DATA       ###
  ### ======================================== ###
  ###############################################
  nc_data <- nc_open(netcdf_path)
  validTimes <- nc_data$dim$time$vals
  time_units <- ncatt_get(nc_data, "time", "units")$value
  origin_string <- sub("hours since ", "", time_units)
  validTime_seconds <- validTimes * 3600
  validTime_posixct <- as.POSIXct(validTime_seconds, origin = origin_string, tz = "UTC")
  
  lat <- ncvar_get(nc_data, "latitude")
  lon <- ncvar_get(nc_data, "longitude")
  
  if (length(validTimes) < 672) {
    log_progress(log_file, year, month, "ISSUE VALIDTIMES", "ISSUE VALIDTIMES", as.character(Sys.time()))
    cat("Issue: File does not have the expected number of timesteps/validtimes\n")
    return()
  }
  # 
  # 
  # length(lat)
  # length(lon)
  # range(lat)
  # range(lon)
  # # >   length(lat)
  # # [1] 77
  # >   length(lon)
  # [1] 125
  # >   range(lat)
  # [1] 53 72
  # >   range(lon)
  # [1] -171 -140
  # > 
  # 
  # netcdf_path
  # [1]  "F:/data/gridded/era5/processed2025//2024/02/era5_single_levels_20240201_20240229_allvars-alaskaalaska.nc"
  # 
  # netcdf_path
  # "F:/data/gridded/era5/processed2025//2024/02/alaska-se/era5_single_levels_20240201_20240229_allvars-alaska.nc"  # 
  
  if (is.null(era5vars)){
    era5vars = c("u10","v10","d2m","t2m","ssrd",'sst')
    names(era5vars) <- c("U10","V10","td",'ta','ssrd','sst')
  }
  
  existingvars = names(nc_data$var)
  # Get list of variables in the NetCDF file
  existingvars <- names(nc_data$var)
  # Keep only those that exist in the file
  era5vars <- era5vars[era5vars %in% existingvars]
  
  ###############################################
  ### ======================================== ###
  ###       BUILD DATA TABLE FROM NETCDF       ###
  ### ======================================== ###
  ###############################################
  # clim_dat <- data.table(
  #   lat = rep(lat, each = length(lon)),
  #   lon = rep(lon, times = length(lat)),
  #   validTime = rep(validTime_posixct, each = length(lat) * length(lon)),
  #   U10 = as.vector(ncvar_get(nc_data, "u10")),
  #   V10 = as.vector(ncvar_get(nc_data, "v10")),
  #   td = as.vector(ncvar_get(nc_data, "d2m")),
  #   ta = as.vector(ncvar_get(nc_data, "t2m")),
  #   ssrd = as.vector(ncvar_get(nc_data, "ssrd"))
  # )
  clim_dat <- data.table(
    lat = rep(lat, each = length(lon)),
    lon = rep(lon, times = length(lat)),
    validTime = rep(validTime_posixct, each = length(lat) * length(lon))
  )
  # Dynamically add each ERA5 variable
  for (i in seq_along(era5vars)) {
    var_name_in_file <- era5vars[i]
    var_name_in_dt   <- names(era5vars)[i]
    
    clim_dat[, (var_name_in_dt) := as.vector(ncvar_get(nc_data, var_name_in_file))]
  }
  
  
  gc()
  
  nc_close(nc_data)
  
  # clim_dat = clim_dat[lon <= -129]
  # gc()
  
  # ###############################################
  ### ======================================== ###
  ###      CALCULATE WIND SPEED & DIRECTION      ###
  ### ======================================== ###
  ###############################################
  clim_dat[!is.na(U10) & !is.na(V10), c("wind10m", "wdir") := {
    wind_results <- uv2wdws(U10, V10)
    .(wind10m = wind_results[, 2], wdir = as.integer(wind_results[, 1]))
  }]
  clim_dat[, c("U10", "V10") := NULL]
  
  ###############################################
  ### ======================================== ###
  ###    TEMPERATURE CONVERSION & CLEANING     ###
  ### ======================================== ###
  ###############################################
  
  unit(clim_dat$ta) <- 'degK|degC'
  unit(clim_dat$td) <- 'degK|degC'
  
  # units(clim_dat$ta) <- "degK"
  # units(clim_dat$td) <- "degK"
  # clim_dat[, ta := set_units(ta, "degC")]
  # clim_dat[, td := set_units(td, "degC")]
  clim_dat <- clim_dat[!is.na(ta)]
  clim_dat[, rh := round(jj::calcRH(ta, td, inputunits = "degC", ignoreattr = TRUE), 1)]
  clim_dat[, pres := 1010]
  
  ###############################################
  ### ======================================== ###
  ###      SORT DATA & ADD AUXILIARY COLUMNS     ###
  ### ======================================== ###
  ###############################################
  setorder(clim_dat, lat, lon, validTime)
  clim_dat[, validHour := as.integer(lhour(validTime))]
  clim_dat[, id := .I]
  
  if (max(clim_dat$ssrd, na.rm = TRUE) > 5000) {
    clim_dat[, ssrd := as.integer(ifelse(validHour == 1,
                                         ssrd / 3600,
                                         (ssrd - data.table::shift(ssrd)) / 3600)),
             by = .(lat, lon)]
  }
  
  ###############################################
  ### ======================================== ###
  ###   WIND SPEED ADJUSTMENT & UNIT CONVERSION  ###
  ### ======================================== ###
  ###############################################
  # units::install_unit("mph", "0.44704 m/s")
  clim_dat[!is.na(wind10m), wind2m := logwind(ssrd, wind10m, "rural")]
  unit(clim_dat$wind10m) <- "m/s"
  unit(clim_dat$wind2m) <- "m/s"
  
  # windSpeedThreshold <- set_units(1, "mi/h") # 1.5 mph
  # windSpeedThreshold = set_units(windSpeedThreshold, "m/s")
  windSpeedThreshold = 0.44704
  # windSpeedThreshold <- set_units(0.67056, "m/s") # 1.5 mph
  # windSpeedThreshold <- set_units(0.67056, "m/s") # 1.5 mph
  clim_dat[wind2m < windSpeedThreshold, wind2m := windSpeedThreshold]
  
  ###############################################
  ### ======================================== ###
  ###     SEPARATE RECORDS WITH MISSING ssrd    ###
  ### ======================================== ###
  ###############################################
  tempdt_nassrd <- clim_dat[is.na(ssrd)]
  clim_dat <- clim_dat[!is.na(ssrd)]
  
  ###############################################
  ### ======================================== ###
  ###      WBGT CALCULATION & C++ INTEGRATION    ###
  ### ======================================== ###
  # ###############################################
  log_progress(log_file, year, month, "calc_wbgt", "started", as.character(Sys.time()))
  # options(warn = -1)
  # Rcpp::sourceCpp(era5_wbgt_cppfile)
  # options(warn = 0)
  # 
  # calc_wbgt_cpp <- function(validTime, 
  #                           # year = NULL, month = NULL, day = NULL, hour = NULL, minute = NULL,
  #                           lat, lon, solar, pres, Tair, relhum, 
  #                           speed, zspeed, dT, urban, calcTpsy = FALSE,
  #                           convergencethreshold = 0.02, outputUnits = "C",
  #                           surface_type = NULL, surface_properties = NULL,
  #                           numOfThreads = parallel::detectCores() - 1) {
  #   
  #   library(RcppParallel)
  #   RcppParallel::setThreadOptions(numThreads  = numOfThreads)
  #   
  #   # if (!is.null(surface_type)){
  #   #   EMIS_SFC <- surface_properties[surface == surface_type]$emissivity
  #   #   ALB_SFC <- surface_properties[surface == surface_type]$albedo
  #   # } else {
  #   # 
  #   # EMIS_SFC <- 0.999
  #   # ALB_SFC <- 0.45
  #   # }
  #   year = lyear(validTime)
  #   month = lmonth(validTime)
  #   day = lday(validTime)
  #   hour = lhour(validTime)         # extract hour from your datatable
  #   minute = lminute(validTime)     # extract minute from your datatable
  #   # if (is.null(year)){
  #   #   year = lyear(validTime)
  #   # }
  #   # if (is.null(month)){
  #   #   month = lmonth(validTime)
  #   # }
  #   # if (is.null(month)){
  #   #   month = lmonth(validTime)
  #   # }
  #   # if (is.null(minute)){
  #   #   minute = lminute(validTime)
  #   # }
  #   # 
  #   ############################################################
  #   gmt = 0
  #   avg = 1
  #   # Convert time to GMT and center in avg period
  #   hour_gmt <- hour - gmt + (minute - 0.5 * avg) / 60
  #   dday <- day + hour_gmt / 24
  #   
  #   ############################################################
  #   
  #   # solarParams = calc_solar_parameters_cpp(year, month, dday, lat, lon, solar)
  #   solarParams = calc_solar_parameters_cpp2(year, month, dday, lat, lon, solar)
  #   
  #   rm(list = c('year', 'month', 'day', 'hour', 'minute','hour_gmt','dday'))
  #   gc()
  #   # solarposition_spa_cpp(Rcpp::NumericVector year, Rcpp::NumericVector month, Rcpp::NumericVector day, 
  #   #                      Rcpp::NumericVector latitude, Rcpp::NumericVector longitude)
  #   # solarParams2 = solarposition_spa_cpp(year, month, dday, lat, lon)
  #   # cza <- solarParams$cza
  #   # fdir <- solarParams$fdir
  #   # solar <- solarParams$solar
  #   
  #   ###############################
  #   ###############################
  #   # Unit conversions
  #   # Tair_og = Tair
  #   Tair <- Tair + 273.15       # Convert temperature to Kelvin
  #   relhum <- 0.01 * relhum           # Convert relative humidity to fraction
  #   ###############################
  #   ###############################
  #   
  #   # Calculate temperatures
  #   # Tg <- Tglobe(Tair_K, rh, pres, speed, solar, fdir, cza, EMIS_SFC, ALB_SFC)
  #   # Tg_cpp = Tglobe_cpp(Tair_K, rh, pres, speed, solar, fdir, cza)
  #   cpp_tg = Tglobe_cpp_vec(Tair, relhum, pres, speed, solarParams$solar, solarParams$fdir, solarParams$cza)
  #   
  #   # cat("here5\n")
  #   
  #   # data.table(og = round(Tg_og, 1), new = round(Tg, 1), new_cpp = round(Tg_cpp, 1))
  #   # Tnwb_cpp = Twb_cpp_vec(Tair_K, rh, pres, speed, solar, fdir, cza, 1)
  #   # Tnwb_cpp = Twb_cpp(Tair_K, rh, pres, speed, solar, fdir, cza, 1)
  #   cpp_nwb = Twb_cpp_vec(Tair, relhum, pres, speed, solarParams$solar, solarParams$fdir, solarParams$cza, 1)
  #   
  #   if (calcTpsy) {
  #     Tpsy <- Twb(Tair, relhum, pres, speed, solarParams$solar, solarParams$fdir, solarParams$cza, 0)
  #   }
  #   
  #   cpp_Twbg <- 0.1 * (Tair-273.15) + 0.2 * cpp_tg + 0.7 * cpp_nwb
  #   
  #   # Convert to Fahrenheit if needed
  #   if (outputUnits == "F") {
  #     # Tg <- Tg * 9/5 + 32
  #     cpp_tg <- cpp_tg * 9/5 + 32
  #     # Tnwb <- Tnwb * 9/5 + 32
  #     cpp_nwb <- cpp_nwb * 9/5 + 32
  #     # Tair <- Tair * 9/5 + 32
  #     # Twbg <- Twbg * 9/5 + 32
  #     cpp_Twbg <- cpp_Twbg * 9/5 + 32
  #     
  #     cpp_tg = round(cpp_tg, 1)
  #     # Tg_cpp = round(Tg_cpp, 1)
  #     # Tnwb = round(Tnwb, 1)
  #     cpp_nwb = round(cpp_nwb, 1)
  #     # Twbg = round(Twbg, 1)
  #     cpp_Twbg = round(cpp_Twbg, 1)
  #     
  #     
  #     if (calcTpsy && !is.null(Tpsy)) {
  #       Tpsy <- Tpsy * 9/5 + 32
  #     }
  #   }
  #   
  #   # Prepare the result
  #   if (calcTpsy) {
  #     return(data.table(Twbg = cpp_Twbg, Tnwb = cpp_nwb, Tg = cpp_tg, Tpsy = Tpsy))
  #   } else {
  #     return(data.table(Twbg = cpp_Twbg, Tnwb = cpp_nwb, Tg = cpp_tg))
  #   }
  # }
  # 
  # 
  # ###############################################
  # ### ======================================== ###
  # ###       APPLY WBGT CALCULATION TO DATA     ###
  # ### ======================================== ###
  # ###############################################
  # clim_dat[, ta := as.numeric(ta)]
  # clim_dat[, wind2m := as.numeric(wind2m)]
  # 
  # clim_dat[, c("wbgt_cpp", "nwb_cpp", "tg_cpp") := {
  #   wbgt_result <- calc_wbgt_cpp(
  #     validTime = validTime,
  #     lat = lat, lon = lon,
  #     solar = ssrd, pres = pres, Tair = ta, relhum = rh, speed = wind2m,
  #     zspeed = rep(2, .N), dT = rep(1, .N), urban = rep(1, .N),
  #     calcTpsy = FALSE, convergencethreshold = 0.01, outputUnits = "C",
  #     numOfThreads = numOfThreads_forWBGTcalc
  #   )
  #   .(wbgt_result$Twbg, wbgt_result$Tnwb, wbgt_result$Tg)
  # }]
  # 
  clim_dat[, c("wbgt", "nwb", "tg") := {
    wbgt_result <- jj::calcWBGT(
      validTime = validTime,
      lat = lat, lon = lon,
      solar = ssrd, pres = pres, Tair = ta, relhum = rh, speed = wind2m,
      zspeed = rep(2, .N), dT = rep(1, .N), urban = rep(1, .N),
      calcTpsy = FALSE, convergencethreshold = 0.01, outputUnits = "C",
      numOfThreads = numOfThreads_forWBGTcalc
    )
    .(wbgt_result$wbgt, wbgt_result$nwb, wbgt_result$tg)
  }]
  
  
  log_progress(log_file, year, month, "calc_wbgt", "completed", as.character(Sys.time()))
  
  
  ###############################################
  ### ======================================== ###
  ###    UNIT CONVERSION & FINAL DATA PROCESSING   ###
  # ### ======================================== ###
  # ###############################################
  # unit(clim_dat$ta) <- "degC"
  # unit(clim_dat$wind2m) <- "m/s"
  # # clim_dat[, `:=`(
  # #   wbgt = round(set_units(wbgt, "degF"), 1),
  # #   nwb = round(set_units(nwb, "degF"), 1),
  # #   tg = round(set_units(tg, "degF"), 1),
  # #   ta = set_units(ta, "degF"),
  # #   td = set_units(td, "degF"),
  # #   wind2m = set_units(wind2m, "mph"),
  # #   wind10m = set_units(wind10m, "mph")
  # # )]
  
  clim_dat <- rbindlist(list(clim_dat, tempdt_nassrd), fill = TRUE)
  setorder(clim_dat, id)
  
  ###############################################
  ### ======================================== ###
  ###         CALCULATE HEAT INDEX (HI)          ###
  ### ======================================== ###
  ###############################################
  clim_dat[, hi := calcHI(airTemp = ta, relativeHumidity = rh, inputunits = "degC", outputunits = "degC")]
  
  
  cols2round <- c("ta", "td", "rh", "wind10m", "wind2m")
  clim_dat[, (cols2round) := lapply(.SD, round, 1), .SDcols = cols2round]
  # 
  # 
  #  # 1) Base-R style with sapply(): a named character vector of primary classes
  # col_classes <- sapply(clim_dat, function(x) class(x)[1])
  # print(col_classes)
  # 
  # 
  # 1) Figure out your grid resolution
  #    (should be constant like 0.25°)
  res_lon <- unique(diff(sort(unique(clim_dat$lon))))
  res_lat <- unique(diff(sort(unique(clim_dat$lat))))
  stopifnot(length(res_lon)==1, length(res_lat)==1)
  mult_lon <- round(1 / res_lon)   # should be 4
  mult_lat <- round(1 / res_lat)   # should be 4
  
  # 1) Compute the new column names
  lon_name <- paste0("lon_idx_x", mult_lon)
  lat_name <- paste0("lat_idx_x", mult_lat)
  
  # 2) Create both columns in one go
  clim_dat[, c(lon_name, lat_name) := .(
    # note: round() before casting to integer
    as.integer(round(lon * mult_lon)),
    as.integer(round(lat * mult_lat))
  )]
  
  # 3) Drop the original floats if you no longer need them
  clim_dat[, c("lon","lat") := NULL]
  
  ###############################################
  clim_dat[, ssrd := as.integer(ssrd)]
  clim_dat[, rh := as.integer(rh)]
  clim_dat[, pres := as.integer(pres)]
  
  
  # climate_vars <- c("ta","td","wind2m","wind10m")
  climate_vars <- c("ta","td","wind10m","wind2m","wbgt","nwb","tg","hi")
  
  fallback_units <- c(
    ta      = "degC",
    td      = "degC",
    wind10m = "ms",
    wind2m  = "ms",
    wbgt    = "degC",
    nwb     = "degC",
    tg      = "degC",
    hi      = "degC"
  )
  
  # 1) Build units_map by inspecting each column once
  units_map <- setNames(
    sapply(climate_vars, function(var) {
      col <- clim_dat[[var]]
      if (inherits(col, "units")) {
        # units::deparse_unit always returns a single string
        deparse_unit(col)
      } else {
        # fallback
        fallback_units[var]
      }
    }, USE.NAMES = FALSE),
    climate_vars
  )
  
  
  
  # 2) Strip any existing units so arithmetic is numeric
  for (v in climate_vars) {
    if (inherits(clim_dat[[v]], "units")) {
      set(clim_dat, j = v, value = as.numeric(clim_dat[[v]]))
    }
  }
  
  
  
  # 3) Create the integer-tenths columns with proper names
  for (v in climate_vars) {
    suffix <- units_map[v]
    # sanitize unit suffix for safe names (remove any punctuation)
    suffix <- gsub("[^A-Za-z0-9]", "", suffix)
    new_col <- paste0(v, "_scaled10_", suffix)
    
    # compute and assign
    clim_dat[, (new_col) := as.integer(round(get(v) * 10))]
  }
  
  clim_dat[, (climate_vars) := NULL]
  
  clim_dat[, validTime := as.integer(validTime)]
  
  ### ======================================== ###
  ###         CLEANUP INTERMEDIATE FILES         ###
  ### ======================================== ###
  ###############################################
  if (remove_netcdf) {
    try(file.remove(netcdf_path))
  }
  
  dir.create(dirname(parquet_file), showWarnings = FALSE, recursive = TRUE)
  # alaska
  # without switching to integers, removing units... 247665912
  # with integers, removing units... 112582103
  write_parquet(clim_dat, parquet_file, compression = "snappy")
  
  file.info(parquet_file)$size
  
  jj::timed('end')
  # 2.12 min
  # after rcpp already loaded: XX min
  
  log_progress(log_file, year, charnum(month), "save_results", "completed",
               paste0("Saved file:", parquet_file))
  cat("Saved parquet", parquet_file, as.character(Sys.time()), "\n")
  
  rm(clim_dat)
  gc()
  
  ###############################################
  ### ======================================== ###
  ###     CLEANUP TEMPORARY FILES IN WORK DIR     ###
  ### ======================================== ###
  ###############################################
  temp_files <- list.files(tempDir, full.names = TRUE)
  temp_files <- temp_files[temp_files %likeany% c(".nc", ".txt")]
  temp_files <- temp_files[!temp_files == netcdf_path]
  temp_files <- temp_files[!temp_files %like% "parquet"]
  file.remove(temp_files)
}


# ================================================
# =         LIST & FILTER ALL RAW FILES          =
# ================================================
# List all files recursively under rootdir
# Loop over each path, list files, then combine into one vector
all_files <- do.call(
  c,
  lapply(rootdir, function(d) {
    list.files(d, recursive = TRUE, full.names = TRUE)
  })
)

# all_files <- list.files(rootdir, recursive = TRUE, full.names = TRUE)

# Keep only files containing "grib" and exclude any containing "request"
all_files <- all_files[all_files %like% "grib"]
all_files <- all_files[!all_files %like% "request"]


all_files = all_files[all_files %like% "madagascar"]

# ================================================
# =         CREATE DATA.TABLE OF FILES           =
# ================================================
# Filter to retain only those files with "allvars" in the filename.

all_grib_allvars_files <- all_files[all_files %like% "allvars"]

# all_grib_allvars_files = gsub("allvarsalaska","allvars-alaska", all_grib_allvars_files)

# Extract year and month from the filename (assuming the naming convention is consistent)
all_grib_allvars_files <- data.table(
  filename = all_grib_allvars_files,
  year = as.integer(substr(j.strsplit(basename(all_grib_allvars_files), "_", 4), 1, 4)),
  month = as.integer(substr(j.strsplit(basename(all_grib_allvars_files), "_", 5), 5, 6))
)

# Merge with the complete grid to ensure all year/month combinations are represented.
all_grib_allvars_files <- merge(expected_grid, all_grib_allvars_files, by = c("year", "month"), all.x = TRUE)
all_grib_allvars_files <- all_grib_allvars_files[order(year, month)]

all_grib_allvars_files[is.na(filename)][,.N,by=year]


all_grib_allvars_files[,.N,by=year]

all_grib_allvars_files = all_grib_allvars_files[!is.na(filename)]

# ================================================
# =            SET UP PARALLEL ENV               =
# ================================================


# Use all available files for processing.
all_grib_allvars_files =all_grib_allvars_files[filename %like% "madagascar"]

all_grib_allvars_files

length(all_grib_allvars_files)


which(all_grib_allvars_files$year == 1992)

all_grib_allvars_files = all_grib_allvars_files[year == 1992]
# ================================================
# =            RUN PARALLEL PROCESSING           =
# ============================S===================
# (Optional) Initialize your environment if needed.
env_info <- startEnvironment(num_cores = 4, threshold = 0.99, consecutive_seconds = 20)
# timed("start")

# for 12 ALASKA FILES 12.27 mins
afile = 2
# like  21.56 secs per file seconds to do subset of se alaska.... 3 files at once. could likely do 5-6 files at once.


timed('start')

foreach(afile = 1:nrow(all_grib_allvars_files),
        .packages = c('data.table', 'wbgt', 'jj', 'arrow', 'ncdf4', 'units', 'Rcpp', 'RcppParallel')) %dopar% {
          
          
          # for (afile in 1:nrow(all_grib_allvars_files)){
          
          
          current_grib_file <- all_grib_allvars_files[afile]
          
          if (nrow(current_grib_file[is.na(filename)]) > 0) next
          
          # timed("start")
          
          era5_grib_process(
            grib_file = current_grib_file$filename,
            year = current_grib_file$year,
            month = current_grib_file$month,
            processed_out_dir = "E:/data/gridded/era5-africa/processed/",
            log_file = "C:/Users/jordan/Desktop/progresstracker_20250309-alaska.txt",
            numOfThreads_forWBGTcalc = 6,
            redo_wsl_netcdf = TRUE,
            redo = FALSE,
            remove_netcdf = TRUE,
            area_str = '-madagascar',
            era5type = 'era5'
          )
          
          # timed("end")
          
          catcolor("\n\nFinished at", as.character(now()), 'blue')
          
        }

timed("end")

closeEnvironment(env_info)

# # ================================================
# dirs = list.dirs("F:/data/gridded/era5/processed2025_alaska")
# # Count files in each directory (non-recursive)
# file_counts <- sapply(dirs, function(d) {
#   if (!dir.exists(d)) {
#     warning("Directory not found: ", d)
#     return(NA_integer_)
#   }
#   length(list.files(path = d, recursive = FALSE, all.files = FALSE))
# })
# 
# counts_df <- data.frame(
#   directory = dirs,
#   count     = file_counts,
#   expected12 = file_counts == 12
# )
# 
# print(counts_df)

######################################################################
######################################################################
# 
# library(jj)
# library(data.table)
# 
# Eprocessed = list.files("E:/data/gridded/era5land/processed/", full.names=TRUE, recursive = TRUE)
# Fprocessed = list.files("F:/data/gridded/era5land/processed/", full.names=TRUE, recursive = TRUE)
# # tff= tff[basename(tff) < 2005]
# 
# Eprocessed = Eprocessed[Eprocessed %like% "wbgtcpp"]
# Fprocessed = Fprocessed[Fprocessed %like% "wbgtcpp"]
# 
# 
# all_processed_parquet = c(Eprocessed, Fprocessed)
# 
# all_processed_parquet = all_processed_parquet[!basename(all_processed_parquet) %in% c("era5_land_20230701_20230731_N21W128S52E66_1_wbgt_gridded_processed_wbgtcpp-COMPARE.parquet",
#                                        "era5_land_20230701_20230731_N21W128S52E66_1_wbgt_gridded_processed_wbgtcpp.parquet") ]
# 
# 
# all_processed_parquet = data.table(parquet_filename = all_processed_parquet, 
#                                    year = as.integer(substr(j.strsplit(basename(all_processed_parquet), "_", 3), 1, 4)), 
#                                    month = as.integer(substr(j.strsplit(basename(all_processed_parquet), "_", 4), 5, 6)))
# 
# all_processed_parquet <- merge(expected_grid, all_processed_parquet, by = c("year", "month"), all.x = TRUE)
# all_processed_parquet = all_processed_parquet[order(year, month)]
# # all_processed_parquet$ctime = file.info(all_processed_parquet$filename)$ctime
# 
# 
# all_processed_parquet = merge(all_processed_parquet, all_grib_allvars_files, by = c("year", "month"), all = TRUE)
# 
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################

# 
# allf = list.files("F:/data/gridded/era5land/raw", full.names = TRUE, recursive = TRUE)
# allf = allf[basename(allf) %like% 'alaska']
# 
# af=1
# 
# for (af in 1:length(allf)){
#   newfilename = file.path(gsub("era5land","era5",dirname(allf[[af]])), basename(allf[[af]]))
#   
#   file.rename(allf[[af]], newfilename)
#   
#   cat(af, '\n')
# }
# 






















