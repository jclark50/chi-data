# Summary: Build an index of WER issues. Parse filename when possible; otherwise
# read the PDF header text to extract volume/issue/week and date range.

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(pdftools)
  library(xml2)
  library(rvest)
})

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(httr)
  library(pdftools)
})
library(stringr)


##########################################
# # 0) CONFIG
##########################################
Sys.setenv("local_working_dir" = "C:/Users/jordan/Desktop/srilanka/")
local_working_dir = Sys.getenv("local_working_dir")

# ---- Load helper functions ----
source(here("helpers", "helpers.R"))

# ---- Configurable paths via .Renviron ----
# In your ~/.Renviron or project .Renviron file, set:
#   ERA5_MADAGASCAR=C:/Users/jordan/Google Drive/CHI-Data/era5_madagascar
#   ERA5_STREAM=C:/Users/jordan/Google Drive/CHI-Data/madagascar_streamed
#
# # To check what R sees:
# # Sys.getenv("ERA5_MADAGASCAR")
# Sys.setenv("ERA5_SRILANKA" = "G:/My Drive/Duke/DGHI/data/era5/srilanka/")
# era5_root <- Sys.getenv("ERA5_SRILANKA", unset = NA)
# if (is.na(era5_root)) {
#   stop("Environment variable ERA5_SRILANKA not set. Please edit your .Renviron.")
#   # usethis::edit_r_environ("project")
#   # Sys.setenv("ERA5_MADAGASCAR" = "G:/My Drive/Duke/DGHI/data/era5/")
# }

# Sys.setenv("ERA5_OUT" = "C:/Users/jordan/Desktop/srilanka/")
# Sys.setenv("ERA5_OUT" = "C:/Users/jordan/Desktop/srilanka/stream")
# out_root <- Sys.getenv("ERA5_OUT", unset = "C:/data/chi_outputs/srilanka")

Sys.setenv("JAVA_HOME"="C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot")
github_root_dir = 'C:/Users/jordan/R_Projects/CHI-Data/'

##########################################
##########################################

# Define file paths for inputs/outputs.
paths = list(
  local_working_dir_out = file.path(local_working_dir, "outputs"),
  
  # midyear population estimates.
  midyear_pop_pdf_path = file.path(github_root_dir, 'analysis/sri_lanka',
                                    'Mid-year_population_by_district_and_sex_2024.pdf'),
  
  # weather station data
  wx_station_data_path = file.path(github_root_dir, 'analysis/sri_lanka/station_data/', 
                                    'SriLanka_Weather_Dataset.csv'),
  
  # landcover data.
  landcover_path = file.path(github_root_dir, 'analysis/sri_lanka/SriLanka_Landcover_2018/', 'SriLanka_Landcover_2018.tif'),
  
  # aggregated (weekly) era5 data.
  era5_agg_path = file.path(github_root_dir, 'analysis/sri_lanka/', 'srilanka_district_daily_era5_areawt.csv'),
  
  # dir for figures output (if running code at end of script)
  fig_dir = file.path(github_root_dir, 'analysis/sri_lanka/outputs/figures')
  


)
dir.create(paths$local_working_dir_out, recursive = TRUE, showWarnings = FALSE)

paths = c(paths, 
          
          outputs = list(
            
            pdf_index_csv = file.path(github_root_dir, 'analysis/sri_lanka/outputs/sri_lanka_WER_index_of_pdfs.csv'),
            case_counts_txt = file.path(paths$local_working_dir_out, 'disease_counts_v4.txt')
            
          )
)


##########################################
##########################################


parse_issue_from_filename_v34plus <- function(u) {
  f <- basename(u)
  
  vol   <- as.integer(str_match(f, "(?i)vol[._ -]*(\\d+)")[,2])
  issue <- as.integer(str_match(f, "(?i)no[._ -]*(\\d+)")[,2])
  
  # base anchor: Vol 34 Issue 1 ??? Week ending 2007-01-05
  base_start <- as.Date("2006-12-30")   # Saturday
  base_end   <- as.Date("2007-01-05")   # Friday
  
  # how many weeks after that?
  offset <- if (!is.na(vol) && !is.na(issue)) {
    (vol - 34) * 52 + (issue - 1)
  } else NA_integer_
  
  date_start <- if (!is.na(offset)) base_start + (7 * offset) else NA
  date_end   <- if (!is.na(offset)) base_end   + (7 * offset) else NA
  
  year <- if (!is.na(date_end)) year(date_end) else NA_integer_
  week <- if (!is.na(date_end)) isoweek(date_end) else NA_integer_
  
  data.table(
    url    = u,
    file   = f,
    volume = vol,
    issue  = issue,
    year   = year,
    week   = week,
    date_start = date_start,
    date_end   = date_end,
    language = fifelse(grepl("eng|english", f, TRUE), "English",
                       fifelse(grepl("tam|tamil",   f, TRUE), "Tamil",
                               fifelse(grepl("sin|sinhala", f, TRUE), "Sinhala", NA_character_)))
  )
}

wer_url= "https://www.epid.gov.lk/weekly-epidemiological-report"

coalesce1 <- function(a, b) ifelse(!is.na(a), a, b)

# ---------- crawl + index ----------
idx_html <- read_html(wer_url)
hrefs    <- html_attr(html_elements(idx_html, "a"), "href")
pdfs     <- unique(grep("\\.pdf$", hrefs, value = TRUE))
pdfs     <- ifelse(startsWith(pdfs, "http"), pdfs, paste0(wer_base, pdfs))
pdfs = pdfs[2:length(pdfs)]


# Build initial index from filenames
idx <- rbindlist(lapply(pdfs, parse_issue_from_filename_v34plus), fill = TRUE)

idx_path  <- file.path(paths$outputs.pdf_index_csv)
dir.create(dirname(idx_path), recursive=TRUE)
write.csv(idx, idx_path)


##############################
##############################
idx = fread(paths$outputs.pdf_index_csv)

######################################################################
######################################################################
######################################################################

# Canonical district names & common variants (older volumes)
DIST_CANON <- c("Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara Eliya",
                "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
                "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
                "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa","Badulla",
                "Monaragala","Ratnapura","Kegalle","Kalmunai")

DIST_PATTERN <- paste0(
  "(?i)\\b(",
  paste(c("Colombo","Gampaha","Kalutara","Kandy","Matale",
          "Nuwara\\s*Eliya","Galle","Matara","Hambantota","Jaffna",
          "Kil?l?inochchi",          # Kilinochchi/Killinochchi
          "Mannar","Vavuniya","Mullaitivu","Batticaloa","Ampara",
          "Trincomalee","Kurunegala","Puttalam","Anuradhapura",
          "Polonnaruwa","Badulla","Mon[eo]ragala",  # Monaragala/Moneragala
          "Kegalle","Kalmunai"), collapse="|"),
  ")\\b"
)

# Normalize district variants back to canonical labels
canonize_district <- function(x) {
  x <- str_squish(x)
  x <- str_replace_all(x, "(?i)Nuwara\\s*Eliya", "Nuwara Eliya")
  x <- str_replace_all(x, "(?i)Kil?l?inochchi", "Kilinochchi")
  x <- str_replace_all(x, "(?i)Mon[eo]ragala",  "Monaragala")
  # keep only known names
  y <- match(tolower(x), tolower(DIST_CANON))
  fifelse(!is.na(y), DIST_CANON[y], x)
}



library(data.table)
library(stringr)
# Sys.setenv("JAVA_HOME"="C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot")
Sys.setenv("JAVA_HOME"="/usr/lib/jvm/java-17-openjdk-amd64")
# install.packages("rJava")
library(tabulapdf)
library(tabulizerjars)
library(data.table)
library(stringr)



`%||%` <- function(a,b) if (!is.null(a)) a else b
.norm <- function(x) { x <- gsub("[\u00A0]", " ", x, perl=TRUE); trimws(x) }
.is_footer <- function(x) grepl("(?i)^(total|source|key to table|page|wer\\s+sri\\s+lanka)", .norm(x %||% ""))

# Extract all integers (in order) from a string
.parse_ints <- function(x) {
  xs <- str_extract_all(.norm(x %||% ""), "\\b\\d{1,7}\\b")[[1]]
  if (!length(xs)) integer(0) else as.integer(xs)
}

# District name = text before first number (after collapsing the row)
.extract_district_from_row <- function(s_row) {
  m <- str_match(s_row, "^(.*?)(?=\\b\\d)")[,2]
  d <- .norm(m %||% "")
  if (.is_footer(d)) return("")
  d <- gsub("(?i)^sri\\s*lanka\\s*$", "Sri Lanka", d, perl=TRUE)
  d
}

# Build a 2*k position map from a disease vector
.make_pos_map <- function(diseases) {
  setNames(
    lapply(seq_along(diseases), function(i) list(A = 2L*i - 1L, B = 2L*i)),
    diseases
  )
}

# Default order from your screenshot (14 diseases -> 28 positions)
DISEASES <- c(
  "dengue", "dysentery", "encephalitis", "enteric_fever",
  "food_poisoning", "leptospirosis", "typhus_f", "viral_hep",
  "rabies", "chickenpox", "meningitis", "leishmania",
  "tuberculosis", "wrcd"
)
POS_MAP <- .make_pos_map(DISEASES)  # e.g., dengue A=1,B=2; dysentery A=3,B=4; .; wrcd A=27,B=28

# Helper to safely pick nth number
.pick_n <- function(ints, n) if (length(ints) >= n) ints[n] else NA_integer_

# ??????????????????????????????????????????????????? MAIN ???????????????????????????????????????????????????
extract_all_diseases_by_position <- function(
    pdf_path,
    diseases = DISEASES,
    pos_map  = POS_MAP,
    keep_total = FALSE,
    debug = FALSE
) {
  tabs <- tryCatch(
    extract_tables(pdf_path, guess = TRUE, method = "stream", output = "tibble"),
    error = function(e) NULL
  )
  if (is.null(tabs) || !length(tabs)) return(NULL)
  
  max_idx <- max(unlist(lapply(pos_map, unlist)), na.rm = TRUE)
  
  out_all <- list()
  
  for (ti in seq_along(tabs)) {
    tab <- as.data.table(tabs[[ti]])  # <- as requested
    if (!is.data.table(tab) || nrow(tab) < 2 || ncol(tab) < 1) next
    
    # Force character; normalize whitespace
    tab_chr <- as.data.frame(lapply(tab, as.character), stringsAsFactors = FALSE)
    tab_chr[] <- lapply(tab_chr, .norm)
    
    rows_out <- vector("list", nrow(tab_chr)); n_out <- 0L
    
    for (r in seq_len(nrow(tab_chr))) {
      s_row <- .norm(paste(tab_chr[r, ], collapse = " "))
      if (!nzchar(s_row) || .is_footer(s_row)) next
      
      district <- .extract_district_from_row(s_row)
      if (!nzchar(district)) next
      if (!keep_total && grepl("(?i)^sri\\s*lanka$", district)) next
      
      ints <- .parse_ints(s_row)
      
      # Build a named list of values like dengue_A, dengue_B, .
      vals <- list(table_id = ti, district = district, n_numbers_in_row = length(ints))
      for (d in diseases) {
        idxA <- pos_map[[d]]$A
        idxB <- pos_map[[d]]$B
        vals[[paste0(d, "_A")]] <- .pick_n(ints, idxA)
        vals[[paste0(d, "_B")]] <- .pick_n(ints, idxB)
      }
      
      n_out <- n_out + 1L
      rows_out[[n_out]] <- as.data.table(vals)
    }
    
    if (n_out) out_all[[length(out_all)+1L]] <- rbindlist(rows_out[seq_len(n_out)], use.names = TRUE, fill = TRUE)
  }
  
  for (ti in 1:length(out_all)){
    if (sum(out_all[[ti]]$n_numbers_in_row, na.rm=TRUE) < 300){
      out_all[[ti]] <- data.table()
    }
  }
  
  if (!length(out_all)) return(NULL)
  out <- rbindlist(out_all, use.names = TRUE, fill = TRUE)
  
  # Optional diagnostics
  if (debug) {
    if (any(out$n_numbers_in_row < max_idx, na.rm = TRUE)) {
      message(sprintf("Some rows have fewer than %d numbers; corresponding *_B (and possibly later diseases) will be NA.", max_idx))
      print(out[n_numbers_in_row < max_idx])
    }
  }
  
  # Drop diagnostic column if you prefer
  out[, n_numbers_in_row := NULL]
  
  out[]
}



# out <- try(extract_lepto_anylayout(u), silent = TRUE)
jj::timed('start')
atp=100
allresults = list()
for (atp in 156:nrow(idx)){
  currow = idx[atp]
  
  file <- currow$file
  if (grepl("^https?://", currow$url, ignore.case = TRUE)) {
    tf <- tempfile(fileext = ".pdf")
    trydn = try(utils::download.file(currow$url, tf, mode = "wb", quiet = TRUE))
    if (inherits(trydn, "try-error")) next
    file <- tf
  }
  file.copy(file, "test2.pdf", overwrite = TRUE)
  pdf_path <- "test2.pdf"  # or "/mnt/data/file5cd8c39c70.pdf"
  # berryFunctions::openFile(pdf_path)
  res <- extract_dengue_lepto_stream(pdf_path, debug = TRUE)
  
  # if (all(is.na(res$lepto_A))) stop()
  # if (nrow(res[is.na(lepto_A) & district %like% 'Nuwara Eliya']) != 0) stop()
  # res =  try(extract_lepto_anylayout2(currow$url), silent = TRUE)
  if (!is.null(res)){
    allresults[[atp]] <- res
    allresults[[atp]]$date_start = currow$date_start
    allresults[[atp]]$date_end = currow$date_end
    # cat(atp, '\n')
    
  } else {
    allresults[[atp]] <- data.table()
    cat("couldn't get data", atp, '\n')
  }
  cat(atp, '\n')
}
jj::timed('end')

allresults2 = rbindlist(allresults, fill=TRUE)

fwrite(allresults2, paths$outputs.case_counts_txt)



#################################################################################
#################################################################################
#################################################################################
#################################################################################



# ---- Helpers ----
normalize_district <- function(x) {
  # Basic cleanup + known aliases/hyphen fixes (extend as needed)
  y <- str_squish(str_to_title(x))
  # specific replacements
  y <- str_replace_all(y, c(
    "^Nuwara[ -]?Eliya$" = "Nuwara-Eliya",
    "^Matale$"          = "Matale",
    "^Gampaha$"         = "Gampaha",
    "^Kalutara$"        = "Kalutara",
    "^Anuradhapura$"    = "Anuradhapura",
    "^Polonnaruwa$"     = "Polonnaruwa",
    "^Kurunegala$"      = "Kurunegala",
    "^Puttalam$"        = "Puttalam",
    "^Ratnapura$"       = "Ratnapura",
    "^Kegalle$"         = "Kegalle"
  ))
  y
}

# --- 1) Canonical district list (25 census districts) ---
districts_census <- c(
  "Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara-Eliya",
  "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
  "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
  "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa",
  "Badulla","Monaragala","Ratnapura","Kegalle"
)

# --- 2) Normalizer + alias map (add rows as you encounter new variants) ---
alias_map <- data.table(
  raw = c("Sri Lanka","Nuwara Eliya","Nuwaraeliya","Nuwara-eliya",
          "Moneragala","Rathnapura","Kalmunai","Kalmuniya",
          "Galle District","Colombo District","Ampara District",
          "Puttlam","Vavniya"),
  canon = c(NA_character_,"Nuwara-Eliya","Nuwara-Eliya","Nuwara-Eliya",
            "Monaragala","Ratnapura","Ampara","Ampara",
            "Galle","Colombo","Ampara",
            "Puttalam","Vavuniya")
)

norm_dist <- function(x) {
  x1 <- str_squish(x)
  x1 <- ifelse(is.na(x1) | x1 == "", NA_character_, x1)
  # title case but keep hyphenated Eliya
  x1 <- str_replace_all(str_to_title(x1), "Nuwara Eliya", "Nuwara-Eliya")
  # apply alias map
  m <- match(x1, alias_map$raw)
  x2 <- ifelse(!is.na(m), alias_map$canon[m], x1)
  x2
}





lepto = fread(paths$outputs.case_counts_txt)
lepto[, date_mid := as.IDate(date_start + (as.integer(date_end - date_start) / 2))]
lepto$year = lyear(lepto$date_start)

lepto[, district := norm_dist(district)]
lepto[, date_mid := as.IDate(date_start + (as.integer(date_end - date_start) / 2))]
lepto$lepto = lepto$leptospirosis_A
lepto$dengue = lepto$dengue_A
lepto$year = lyear(lepto$date_start)
lepto[year >= 2014, year2merge := year]
lepto[year < 2014, year2merge := 2014]


lepto = merge(lepto, allpopdat, by.x = c("district","year2merge"), by.y = c("district","year"), all=F)

lepto[, lepto_100k := (lepto / poptot) * 1e5]
lepto[, dengue_100k := (dengue / poptot) * 1e5]


#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################



# Summary: Build a district-week panel from WER data and save multiple plots:
# national trend (lines), top districts (bars), small-multiple trends, seasonality
# (boxplots), heatmap of rolling burden, and YoY change bars.

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(ggplot2)
  library(scales)
})
library(jj)

library(data.table)
library(stringr)

library(tabulapdf)
library(tabulizerjars)
# remotes::install_github(c("ropensci/tabulizerjars", "ropensci/tabulizer"), INSTALL_opts = "--no-multiarch")

setDTthreads(8)
# MID YEAR POPULATION ESTIMATES 2014-2023


# --- CONFIG ---
tabs <- extract_tables(paths$midyear_pop_pdf_path, method = "lattice", guess = TRUE, output = "tibble")

pdfyears = 2014:2023
allpopdat = list()
for (apage in 1:3){
  pdfpg1 = as.data.table(tabs[[apage]])
  
  yrheads = names(pdfpg1)[which(names(pdfpg1) %likeany% pdfyears)]
  ayy=1
  alist = list()
  for (ayy in 1:length(yrheads)){
    curyear = yrheads[[ayy]]
    
    district_names = pdfpg1$District[-1]
    
    curpopcol = which(pdfpg1[1] == 'Total')[ayy]
    curpopcolname = names(pdfpg1)[curpopcol]
    curpopcol = pdfpg1[, ..curpopcolname]
    
    curpopcol = tail(curpopcol, -1)
    alist[[ayy]] = data.table(year = curyear, district = district_names, poptot = curpopcol)
    names(  alist[[ayy]] ) <- c('year','district','poptot')
    
    
  }
  allpopdat[[apage]] = rbindlist(alist)
  
}
allpopdat = rbindlist(allpopdat)
allpopdat$year = gsub("\\*","",allpopdat$year)
allpopdat$poptot = gsub(",","",allpopdat$poptot)
allpopdat$poptot = as.numeric(allpopdat$poptot) * 1000
allpopdat$year = as.integer(allpopdat$year)
allpopdat[, district := norm_dist(district)]








####################################################################################
####################################################################################
####################################################################################
####################################################################################

# link lepto with weather station data.

wx_station_data = fread(paths$wx_station_data_path, header=TRUE)

# wx_station_data[,.N,by=city][order(city)]
# wx_station_data[!city %in% lepto$district]
# ?????? 0) Inputs ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# data.tables you already have:
#   - lepto:   columns at least district, date, cases   (date = Date/IDate)
#   - wx_station_data: daily rows with cols 'time' (Date/IDate), 'latitude','longitude','city', and weather vars

stopifnot(all(c("time","latitude","longitude","city") %in% names(wx_station_data)))
if (!inherits(wx_station_data$time, "Date")) wx_station_data[, time := as.IDate(time)]

lepto$date = lepto$date_mid

if (!inherits(lepto$date, "Date")) lepto[, date := as.IDate(date)]

# ?????? 1) Download & read districts (ADM2) ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
gadm_zip <- "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_LKA_shp.zip"
tdir <- tempfile("lka_gadm41_"); dir.create(tdir)
zipfile <- file.path(tdir, "gadm41_LKA_shp.zip")
download.file(gadm_zip, destfile = zipfile, mode = "wb", quiet = TRUE)
unzip(zipfile, exdir = tdir)
adm2_shp <- list.files(tdir, pattern = "^gadm41_LKA_2\\.shp$", full.names = TRUE, recursive = TRUE)
stopifnot(length(adm2_shp) == 1)

districts_sf <- st_read(adm2_shp, quiet = TRUE)[, c("GID_2","NAME_1","NAME_2","geometry")]
names(districts_sf) <- c("gid2","province","district","geometry")

# normalize district names to match your lepto/pop conventions
norm_dist <- function(x) {
  x1 <- str_squish(str_to_title(x))
  x1 <- str_replace_all(x1, "Nuwara Eliya", "Nuwara-Eliya")
  x1
}
districts_sf$district <- norm_dist(districts_sf$district)

# ?????? 2) Assign each station/city to a district via spatial join ?????????????????????????????????????????????
# Use unique (city, lat, lon) to build a station lookup -> district
stations_lu <- unique(wx_station_data[, .(city, latitude, longitude)])
stations_sf <- st_as_sf(stations_lu, coords = c("longitude","latitude"), crs = 4326, remove = FALSE)
stations_sf <- st_transform(stations_sf, st_crs(districts_sf))
districts_sf <- st_make_valid(districts_sf)

# spatial join (point-in-polygon)
stations_joined <- st_join(stations_sf, districts_sf["district"], left = TRUE)

# build a lookup table city -> district (via geometry)
city_to_district <- as.data.table(stations_joined)[, .(city, latitude, longitude, district)]
# If any city falls outside polygons (shouldn't happen), flag it:
unmatched_cities <- city_to_district[is.na(district)]
if (nrow(unmatched_cities)) {
  warning("Some station cities did not match a district: ",
          paste(unique(unmatched_cities$city), collapse = ", "))
}

# ?????? 3) Attach district to every daily weather row and aggregate ?????????????????????????????????????????????
wx_with_dist <- merge(wx_station_data, city_to_district,
                      by = c("city","latitude","longitude"), all.x = TRUE)

# sanity check: which districts in lepto have no weather after mapping?
no_wx_for_lepto <- setdiff(unique(lepto$district), unique(wx_with_dist$district))
if (length(no_wx_for_lepto)) {
  message("Districts in lepto but no mapped weather: ",
          paste(no_wx_for_lepto, collapse = ", "))
}

# Decide aggregation: for district-level *areal* weather, use mean/median across stations in district.
# (Summing precipitation across stations is usually incorrect; use mean for areal average.)
# Customize the variable set as needed:
weather_means <- c("temperature_2m_max","temperature_2m_min","temperature_2m_mean",
                   "apparent_temperature_max","apparent_temperature_min","apparent_temperature_mean",
                   "shortwave_radiation_sum","precipitation_sum","rain_sum",
                   "precipitation_hours","windspeed_10m_max","windgusts_10m_max",
                   "et0_fao_evapotranspiration")

keep_cols <- c("district","time", weather_means)
wx_keep <- wx_with_dist[, intersect(names(wx_with_dist), keep_cols), with = FALSE]

# aggregate to district × day (mean; switch to median if you prefer robustness)
wx_dist_daily <- wx_keep[
  , lapply(.SD, mean, na.rm = TRUE), by = .(district, date = time),
  .SDcols = setdiff(names(wx_keep), c("district","time"))
]

# ?????? 4) Harmonize district names and join to lepto ????????????????????????????????????????????????????????????????????????????????????
lepto[, district := norm_dist(district)]
wx_dist_daily[, district := norm_dist(district)]

# Join weather to lepto by district + date
setkey(lepto, district, date)
setkey(wx_dist_daily, district, date)
lepto_weather <- wx_dist_daily[lepto]  # left join onto lepto; keeps all lepto rows

# ?????? 5) Quick diagnostics ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# if (anyNA(lepto_weather$temperature_2m_mean)) {
#   miss <- lepto_weather[is.na(temperature_2m_mean), .N, by = district][order(-N)]
#   message("Lepto rows lacking matched weather (top):")
#   print(head(miss, 10))
# }

# ?????? 6) Result ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# 'lepto_weather' now has district, date, cases (+ your other lepto fields) and the district-aggregated daily weather.
lepto = lepto_weather

# names(lepto_weather)
# "temperature_2m_max"         "temperature_2m_min"        
# [5] "temperature_2m_mean"        "apparent_temperature_max"   "apparent_temperature_min"   "apparent_temperature_mean" 
# [9] "shortwave_radiation_sum"    "precipitation_sum"          "rain_sum"                   "precipitation_hours"       
# [13] "windspeed_10m_max"          "windgusts_10m_max"          "et0_fao_evapotranspiration" 
####################################################################################
####################################################################################
####################################################################################
####################################################################################

# MERGE WITH LAND COVER DATA.

# ?????? 0) Inputs ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

lc_path = paths$landcover_path
r <- rast(lc_path)

# Define your class legend (update if your TIFF uses different codes)
class_map <- data.table(
  code  = c(10,20,30,40,50,60,70,80,90,255),
  label = c("Water","BuiltUp","Cropland","Forest","Shrub",
            "Grass","Bare","Wetland","Paddy","NoData")
)

# ?????? 1) Get the correct **districts (ADM2)** ???????????????????????????????????????????????????????????????????????????????????????????????????
gadm_zip <- "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_LKA_shp.zip"

tdir <- tempfile("lka_gadm41_"); dir.create(tdir)
zipfile <- file.path(tdir, "gadm41_LKA_shp.zip")
download.file(gadm_zip, destfile = zipfile, mode = "wb", quiet = TRUE)
unzip(zipfile, exdir = tdir)
adm2_shp <- list.files(tdir, pattern = "^gadm41_LKA_2\\.shp$", full.names = TRUE, recursive = TRUE)

districts_sf <- st_read(adm2_shp, quiet = TRUE)[, c("GID_2","NAME_1","NAME_2","geometry")]
names(districts_sf) <- c("gid2","district","subdistrict","geometry")


# plot(districts_sf)
# districts_sf_dt = as.data.table(districts_sf)

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

# # Check result
# print(districts_sf)
# 
# # Plot to verify
# plot(st_geometry(districts_sf), col = rainbow(nrow(districts_sf)), border = "black")




# ?????? 2) Make sure CRS match
districts_sf <- st_transform(districts_sf, crs(r))

# ?????? 3) Define extraction function
freq_fun <- function(values, coverage_fraction) {
  dt <- data.table(code = values, w = coverage_fraction)
  dt <- dt[!is.na(code)]
  if (nrow(dt) == 0) return(data.frame(code = integer(), prop = numeric()))
  dt <- dt[, .(w = sum(w, na.rm = TRUE)), by = code]
  dt[, prop := w / sum(w)]
  as.data.frame(dt[, .(code, prop)])
}

# ?????? 4) Run extraction
lc_list <- exact_extract(r, districts_sf, 
                         fun = function(values, coverage_fraction) {
                           list(freq_fun(values, coverage_fraction))
                         },
                         progress = TRUE)

# Now it's a list of data.frames ??? bind safely
lc_by_dist <- rbindlist(lc_list, idcol = "dist_idx")
lc_by_dist[, district := districts_sf$district[dist_idx]]
lc_by_dist[, dist_idx := NULL]

# jj::countna(lc_by_dist)

# Drop NoData (e.g., 255) before going wide (or keep and drop later)
lc_by_dist <- lc_by_dist[code %in% class_map$code]

# Attach labels and go wide to district × class
lc_by_dist <- merge(lc_by_dist, class_map, by = "code", all.x = TRUE)
lc_wide <- dcast(lc_by_dist, district ~ label, value.var = "prop", fill = 0)

# Optional: re-normalize rows to ensure sums = 1 after dropping NoData
prop_cols <- setdiff(names(lc_wide), "district")
row_sums <- lc_wide[, rowSums(.SD), .SDcols = prop_cols]
lc_wide[, (prop_cols) := lapply(.SD, function(z) ifelse(row_sums > 0, z / row_sums, 0)), .SDcols = prop_cols]

# Inspect
lc_wide[order(district)][1:10]


lepto = merge(lepto, lc_wide, by = c("district"))

######################################################################
######################################################################

# MERGE IN ERA5 DATA.

# aggregated to district/daily in script "sri_lanka_daily_era5-district_level.R"
era5 = fread(paths$era5_agg_path)

lepto = merge(lepto, era5, by = c("date","district"))
names(lepto)







######################################################################
######################################################################


######################################################################
######################################################################



######################################################################
######################################################################














######################################################################
######################################################################
# lepto$lepto_100k

lepto[, .(rate = mean(lepto_100k, na.rm = TRUE),
          paddy = mean(Paddy),
          wetland = mean(Wetland),
          built = mean(BuiltUp)), 
      by = district]

# Remove obvious outliers/erroneous data points.
# plot(lepto[district == 'Jaffna']$date_mid, lepto[district == 'Jaffna']$lepto_100k, type = 'l')
# plot(lepto[district == 'Vavuniya']$date_mid, lepto[district == 'Vavuniya']$dengue_100k, type = 'l')
lepto[district == 'Ampara' & dengue_100k > 80, dengue_100k := NA]
lepto[district == 'Mullaitivu' & dengue_100k > 200, dengue_100k := NA]
lepto[district == 'Kilinochchi' & dengue_100k > 200, dengue_100k := NA]
lepto[district == 'Mannar' & dengue_100k > 200, dengue_100k := NA]
lepto[district == 'Vavuniya' & year == 2020 & dengue_100k > 60, dengue_100k := NA]

##################
##################
# Test basic correlations.
num_vars <- c("lepto_100k","BuiltUp","Cropland","Grass","Paddy","Shrub","Water","Wetland")
cor_mat <- cor(lepto[, ..num_vars], use = "complete.obs")
cor_mat["lepto_100k",]


num_vars <- c("dengue_100k","BuiltUp","Cropland","Grass","Paddy","Shrub","Water","Wetland")
cor_mat <- cor(lepto[, ..num_vars], use = "complete.obs")
cor_mat["dengue_100k",]

# test basic linear models.
# m1 <- lm(lepto_100k ~ Paddy + Wetland + BuiltUp + Water + Grass + Shrub, data = lepto)
# summary(m1)
# 
# m1 <- lm(dengue_100k ~ Paddy + Wetland + BuiltUp + Water + Grass + Shrub, data = lepto)
# summary(m1)
# 
# m1 <- lm(lepto_100k ~ temperature_2m_max + temperature_2m_min, data = lepto)
# summary(m1)




######################################################################
######################################################################
######################################################################
######################################################################



######################################################################
######################################################################
######################################################################
######################################################################

# lepto$cases = lepto$lepto_100k

dir.create(paths$fig_dir, recursive = TRUE, showWarnings = FALSE)

# -------- Helpers --------
canon <- c("Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara Eliya",
           "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
           "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
           "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa","Badulla",
           "Monaragala","Ratnapura","Kegalle","Kalmunai")

canonize <- function(v) {
  i <- match(tolower(v), tolower(canon))
  ifelse(is.na(i), v, canon[i])
}

th <- theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(),
        plot.title = element_text(face = "bold"),
        axis.title.x = element_text(margin = margin(t = 6)),
        axis.title.y = element_text(margin = margin(r = 6)))

# -------- 1) Load ??? standardize ??? weekly panel --------
# lep_raw <- fread(lepto_path)
lep_raw = lepto
# Use weekly counts: prefer column A (weekly) if present; else 'cases'
if ("A" %in% names(lep_raw) && !"cases" %in% names(lep_raw)) {
  setnames(lep_raw, "A", "cases")
} else if ("A" %in% names(lep_raw) && "cases" %in% names(lep_raw)) {
  lep_raw[, cases := fifelse(!is.na(A), as.integer(A), as.integer(cases))]
}

lep <- lep_raw[, .(district, date_end, cases)]
lep[, `:=`(district = canonize(district),
           date_end = as.IDate(date_end),
           cases = as.integer(cases))]
lep <- lep[!is.na(date_end) & !is.na(cases) & nzchar(district)]

# Aggregate just in case there are duplicates per district-week
lep_week <- lep[, .(cases = sum(cases, na.rm = TRUE)),
                by = .(district, date_end)]

# Complete weekly grid
weeks_seq <- seq(min(lep_week$date_end, na.rm = TRUE),
                 max(lep_week$date_end, na.rm = TRUE),
                 by = "7 days")
panel <- CJ(district = sort(unique(lep_week$district)),
            date_end = weeks_seq, unique = TRUE)
panel <- lep_week[panel, on = .(district, date_end)]
panel[is.na(cases), cases := 0L]

# Time features + rolling metrics (per district)
panel[, `:=`(iso_week = isoweek(date_end),
             iso_year = isoyear(date_end),
             month    = month(date_end),
             year     = year(date_end))]
setorder(panel, district, date_end)
panel[, `:=`(
  ma4   = frollmean(cases,  4, align = "right", na.rm = TRUE),
  ma12  = frollmean(cases, 12, align = "right", na.rm = TRUE),
  sum12 = frollsum(cases,  12, align = "right", na.rm = TRUE)
), by = district]

# YoY deltas: match same ISO week, previous ISO year
prev <- panel[, .(district, iso_week, iso_year, cases_prev = cases)]
prev[, iso_year := iso_year + 1L]
panel <- merge(panel, prev,
               by = c("district","iso_week","iso_year"),
               all.x = TRUE, sort = FALSE)
panel[, `:=`(
  yoy_abs = cases - cases_prev,
  yoy_pct = ifelse(!is.na(cases_prev) & cases_prev > 0,
                   100 * (cases - cases_prev) / cases_prev, NA_real_)
)]

# National time series (compute smoothers on the national series itself)
nat <- panel[, .(cases = sum(cases)), by = date_end][order(date_end)]
nat[, `:=`(ma4 = frollmean(cases, 4, align = "right"),
           ma12 = frollmean(cases, 12, align = "right"))]

latest <- max(panel$date_end, na.rm = TRUE)

# -------- 2) Plots --------











# Small helpers
fmt_int <- function(x) format(as.integer(x), big.mark = ",")
safe_ylim <- function(v, pad = 0.06) {
  v <- v[is.finite(v)]
  if (!length(v)) return(c(0, 1))
  r <- range(v, na.rm = TRUE)
  d <- diff(r); if (d == 0) d <- max(1, r[2])
  c(max(0, r[1] - d*pad), r[2] + d*pad)
}

# A simple theme via par()
set_par <- function() {
  par(family = "sans", cex.axis = 1.05, cex.lab = 1.2, cex.main = 1.25,
      mar = c(4.2, 4.5, 3.0, 1.0), las = 1, xaxs = "i", yaxs = "i")
}

# ---------------- P1: National weekly line (with 4w/12w MA) ----------------
ragg::agg_png(file.path(fig_dir, "P1_national_trend.png"),
              width = 10, height = 5.5, units = "in", res = 450)
on.exit(dev.off(), add = TRUE)
set_par()
yl <- safe_ylim(c(nat$cases, nat$ma12, nat$ma4))
plot(nat$date_end, nat$cases, type = "l", lwd = 2.2, col = "#2C3E50",
     xlab = "Week ending", ylab = "Cases", main = "Sri Lanka leptospirosis - national weekly cases",
     ylim = yl)
grid(col = "grey90"); box()
lines(nat$date_end, nat$ma12, lwd = 2.8, col = "#E74C3C")
lines(nat$date_end, nat$ma4,  lwd = 2.0, col = "#3498DB")
legend("topleft",
       legend = c("Weekly cases","12-week MA","4-week MA"),
       col = c("#2C3E50","#E74C3C","#3498DB"), lwd = c(2.2,2.8,2.0), bty = "n")
dev.off()
berryFunctions::openFile(file.path(fig_dir, "P1_national_trend.png"))

# ---------------- P2: Top 15 districts (latest week, horizontal bars) ----------------
top15 <- panel[date_end == latest][order(-cases)][1:15]
cols <- "#2E86C1"
ragg::agg_png(file.path(fig_dir, "P2_top15_latest_week.png"),
              width = 9, height = 7.5, units = "in", res = 450)
set_par(); par(mar = c(4.2, 10, 3.0, 1.0))
bp <- barplot(rev(top15$cases), horiz = TRUE, col = cols, border = NA,
              names.arg = rev(top15$district),
              xlab = "Cases (weekly)", main = paste0("Top districts - week ending ", latest))
grid(nx = NA, ny = NULL, col = "grey90"); box()
# annotate values
text(x = rev(top15$cases), y = bp, labels = fmt_int(rev(top15$cases)),
     pos = 4, xpd = NA, cex = 0.9, offset = 0.4)
dev.off()
berryFunctions::openFile(file.path(fig_dir, "P2_top15_latest_week.png"))

# ---------------- P3: Small multiples (top 6 by 52-week burden) ----------------
top6 <- panel[date_end > (latest - 7*52),
              .(cases_52 = sum(cases)), by = district][order(-cases_52)][1:6, district]
ragg::agg_png(file.path(fig_dir, "P3_top6_last52_facets.png"),
              width = 12, height = 8, units = "in", res = 450)
set_par(); par(mfrow = c(2,3), mar = c(3.8, 4.5, 2.8, 1.0))
for (d in top6) {
  dt <- panel[district == d & date_end > (latest - 7*52)]
  yl <- safe_ylim(c(dt$cases, dt$ma12))
  plot(dt$date_end, dt$cases, type = "l", lwd = 2.0, col = "#2C3E50",
       xlab = "Week", ylab = "Cases", main = d, ylim = yl)
  grid(col = "grey92"); box()
  lines(dt$date_end, dt$ma12, lwd = 2.6, col = "#E74C3C")
}
dev.off()
berryFunctions::openFile(file.path(fig_dir, "P3_top6_last52_facets.png"))

# ---------------- P4: Seasonality boxplots (ISO week) for top 8 ----------------
top8 <- panel[, .(tot = sum(cases)), by = district][order(-tot)][1:8, district]
ragg::agg_png(file.path(fig_dir, "P4_seasonality_boxplots.png"),
              width = 14, height = 8, units = "in", res = 450)
set_par(); par(mfrow = c(2,4), mar = c(4.2, 4.5, 2.8, 0.8))
for (d in top8) {
  dt <- panel[district == d]
  boxplot(cases ~ iso_week, data = dt, outline = FALSE,
          xaxt = "n", col = "#AED6F1", border = "#2E86C1",
          xlab = "ISO week", ylab = "Weekly cases", main = d)
  axis(1, at = seq(1, 52, by = 4), labels = seq(1, 52, by = 4), tick = TRUE)
  grid(nx = NA, ny = NULL, col = "grey90"); box()
}
dev.off()
berryFunctions::openFile(file.path(fig_dir, "P4_seasonality_boxplots.png"))



# ---------------- P6: YoY change bars (latest week) ----------------
yoy <- panel[date_end == latest & !is.na(yoy_abs),
             .(district, yoy_abs)][order(-abs(yoy_abs))][1:15]
cols <- ifelse(yoy$yoy_abs >= 0, "#E74C3C", "#2E86C1")
ragg::agg_png(file.path(fig_dir, "P6_yoy_change_top15.png"),
              width = 9, height = 7.5, units = "in", res = 450)
set_par(); par(mar = c(4.2, 10, 3.0, 1.0))
bp <- barplot(rev(yoy$yoy_abs), horiz = TRUE,
              names.arg = rev(yoy$district), col = rev(cols), border = NA,
              xlab = "?? vs same ISO week last year (cases)",
              main = paste0("YoY change - week ending ", latest))
abline(v = 0, col = "grey40", lwd = 1.5)
grid(nx = NA, ny = NULL, col = "grey90"); box()
text(x = rev(yoy$yoy_abs), y = bp, labels = rev(fmt_int(yoy$yoy_abs)),
     pos = ifelse(rev(yoy$yoy_abs) >= 0, 4, 2), xpd = NA, cex = 0.9, offset = 0.4)
dev.off()
berryFunctions::openFile(file.path(fig_dir, "P6_yoy_change_top15.png"))

message("Saved plots to: ", normalizePath(fig_dir, winslash = "/"))

















# ---------------- P5: Heatmap of rolling 12-week burden (last 3 years) ----------------
# --- FIXED HEATMAP: rolling 12-week burden (last 3 years), base R + ragg ---

stopifnot(requireNamespace("ragg", quietly = TRUE))

# subset last 3y and order districts by total burden in window
start3y <- as.Date(latest) - 365*3
start3y = '2014-01-01'
sub <- panel[date_end >= start3y, .(district, date_end, sum12)]
ord <- sub[, .(burden = sum(sum12, na.rm = TRUE)), by = district][order(-burden), district]
sub[, district := factor(district, levels = ord)]

# build z matrix with rows = dates, cols = ordered districts
dates <- sort(unique(sub$date_end))
dists <- levels(sub$district)

zmat <- vapply(dists, function(d) {
  df  <- sub[district == d]
  v   <- rep(NA_real_, length(dates))
  pos <- match(df$date_end, dates)
  v[pos] <- df$sum12
  v
}, numeric(length(dates)))  # result is [length(dates) x length(dists)]

# color palette
pal <- colorRampPalette(c("#FFFFFF","#FF9F66","#E74C3C","#A93226"))(256)

# plotting params
set_par <- function() {
  par(family = "sans", cex.axis = 1.0, cex.lab = 1.1, cex.main = 1.2,
      mar = c(5, 9.5, 3, 4), las = 1, xaxs = "i", yaxs = "i")
}

# ragg::agg_png(file.path(fig_dir, "P5_heatmap_sum12_last3y.png"),
#               width = 12, height = 8.5, units = "in", res = 450)
set_par()

# image() expects: nrow(z) == length(x), ncol(z) == length(y)
x <- seq_along(dates)                 # columns along time axis
y <- seq_along(dists)                 # rows for districts
image(x = x, y = y, z = zmat,
      col = pal, xlab = "Week ending", ylab = "", xaxt = "n", yaxt = "n",
      useRaster = TRUE)
box()

# x-axis ticks (about quarterly)
xticks <- unique(round(seq(1, length(dates), length.out = 9)))
axis(1, at = xticks, labels = format(dates[xticks], "%Y-%m"))

# y-axis labels (districts), ordered by burden
axis(2, at = y, labels = dists, las = 2, cex.axis = 0.9)

# color legend (right side)
zvals <- zmat[is.finite(zmat)]
brks  <- pretty(range(zvals, na.rm = TRUE), n = 5)
# draw vertical legend bar
usr <- par("usr")
leg_x0 <- usr[2] + 0.3
leg_x1 <- leg_x0 + 0.35
leg_y0 <- usr[3]
leg_y1 <- usr[3] + 0.6 * (usr[4] - usr[3])
segments <- length(pal)
yy <- seq(leg_y0, leg_y1, length.out = segments + 1L)
for (i in seq_len(segments)) {
  rect(leg_x0, yy[i], leg_x1, yy[i+1], col = pal[i], border = NA, xpd = NA)
}
axis(4, at = seq(leg_y0, leg_y1, length.out = length(brks)),
     labels = format(brks, big.mark = ","), las = 1)
mtext("12-week sum", side = 4, line = 2.5, at = (leg_y0 + leg_y1)/2)

title("Rolling 12-week burden by district (last 3 years)")
# dev.off()














