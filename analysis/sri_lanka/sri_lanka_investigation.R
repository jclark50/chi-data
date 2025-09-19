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

# Example usage:
urls <- c("https://www.epid.gov.lk/storage/post/pdfs/en_68aca1e16f16d_Vol_52_no_23-english.pdf")
parse_issue_from_filename_v34plus(urls)

# read_html
wer_url= "https://www.epid.gov.lk/weekly-epidemiological-report"

coalesce1 <- function(a, b) ifelse(!is.na(a), a, b)

# ---------- crawl + index ----------
idx_html <- read_html(wer_url)
hrefs    <- html_attr(html_elements(idx_html, "a"), "href")
pdfs     <- unique(grep("\\.pdf$", hrefs, value = TRUE))
pdfs     <- ifelse(startsWith(pdfs, "http"), pdfs, paste0(wer_base, pdfs))
pdfs = pdfs[2:length(pdfs)]


# Build initial index from filenames
idx_fn <- rbindlist(lapply(pdfs, parse_issue_from_filename_v34plus), fill = TRUE)

idx_path  <- "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/sri_lanka_WER_index.csv"
dir.create(dirname(idx_path), recursive=TRUE)

write.csv(idx_fn, idx_path)



fread("C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/sri_lanka_WER_index.csv")
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

# --- Helper: find header line & Leptos column index (A token index) ---
.find_header_and_index <- function(lines) {
  # Fix hyphenation for "Leptos-pirosis"
  lines2 <- gsub("Leptos-\\s*pirosis", "Leptospirosis", lines, ignore.case = TRUE)
  lines2 <- gsub("\u00A0", " ", lines2, useBytes = TRUE)
  
  # Candidate header lines must mention Leptos... AND either RDHS/DPDHS OR another disease (Typhus/Viral)
  hdr_idx <- which(
    grepl("(?i)Leptos", lines2) &
      (grepl("(?i)\\b(RDHS|DPDHS)\\b", lines2) | grepl("(?i)Typhus|Viral\\s*Hep", lines2))
  )
  if (!length(hdr_idx)) return(NULL)
  
  # Use the first plausible header; split into columns by >=2 spaces
  header <- lines2[hdr_idx[1]]
  hdr_cols <- strsplit(header, "\\s{2,}")[[1]]
  hdr_cols <- trimws(hdr_cols)
  
  # Some PDFs print "DPDHS Division" (old) or "RDHS" (new) as first column header
  # Ensure the first token is the "district" column
  if (!length(hdr_cols)) return(NULL)
  if (!grepl("(?i)RDHS|DPDHS|Division", hdr_cols[1])) {
    # Sometimes the disease names start on this line; try previous line for RDHS/DPDHS
    prev <- max(1, hdr_idx[1] - 1)
    hdr_prev <- strsplit(lines2[prev], "\\s{2,}")[[1]]
    if (length(hdr_prev) && grepl("(?i)RDHS|DPDHS|Division", hdr_prev[1])) {
      hdr_cols <- c(trimws(hdr_prev[1]), hdr_cols)
    } else {
      # If still missing, inject a placeholder so indexing still works
      hdr_cols <- c("RDHS/DPDHS", hdr_cols)
    }
  }
  
  # Find the "Leptospirosis" column among headers
  lepto_i <- which(grepl("(?i)^\\s*Leptos", hdr_cols))[1]
  if (is.na(lepto_i)) return(NULL)
  
  # After the header, there is often a schema row like "A   B   A   B ..."
  # We'll skip it if present, but column indexing doesn't depend on it.
  list(hdr_line = hdr_idx[1], hdr_cols = hdr_cols, lepto_i = lepto_i)
}

# --- Main extractor ---
extract_lepto_anylayout <- function(url) {
  tf <- tempfile(fileext = ".pdf")
  GET(url, write_disk(tf, overwrite = TRUE),
      user_agent("DGHI-CHI-WER/1.0"), timeout(60))
  
  pages <- pdf_text(tf)
  results <- list()
  
  for (p in seq_along(pages)) {
    lines <- unlist(strsplit(pages[[p]], "\n", fixed = TRUE), use.names = FALSE)
    lines <- sub("\\s+$", "", lines)  # trim trailing spaces
    
    hdr <- .find_header_and_index(lines)
    if (is.null(hdr)) next
    
    header   <- lines[hdr$hdr_line]
    hdr_cols <- hdr$hdr_cols
    lepto_i  <- hdr$lepto_i
    
    # Position-based backup: index of "Leptos..." start in the header line
    lepto_pos <- regexpr("(?i)Leptos", header, perl = TRUE)[1]
    # Token-based A index for Leptos: 1 = district, each disease has A,B ??? A index = 2 * j
    # j is the position of the disease among hdr_cols excluding first (district)
    disease_positions <- which(seq_along(hdr_cols) > 1)
    j <- which(disease_positions == lepto_i)
    a_token_idx <- if (length(j)) 2 * j else NA_integer_
    
    # Start scanning rows after header (skip schema line "A   B   A   B" if present)
    i <- hdr$hdr_line + 1L
    if (i <= length(lines) && grepl("\\bA\\s+B\\b", lines[i])) i <- i + 1L
    
    page_out <- list()
    # while (i <= length(lines)) {
    for (i in 1:length(lines)){
      l <- lines[i]
      if (!nzchar(trimws(l))) next
      if (grepl("(?i)^\\s*(Total|Source|Key to Table|Page|WER\\s+Sri\\s+Lanka)", l)) next
      
      # Does the row start with a district label?
      m <- str_match(l, DIST_PATTERN)
      if (!all(is.na(m))) {
        district_raw <- m[1,2]
        district <- canonize_district(district_raw)
        
        # Preferred: token-based (stable when columns are aligned)
        val <- NA_integer_
        parts <- strsplit(l, "\\s{2,}")[[1]]
        if (!is.na(a_token_idx) && length(parts) >= a_token_idx) {
          val <- suppressWarnings(as.integer(gsub("[^0-9]", "", parts[a_token_idx])))
        }
        
        # Fallback: character-window around the Leptos column start
        if (is.na(val) || is.na(a_token_idx)) {
          if (!is.na(lepto_pos) && lepto_pos > 0) {
            start <- max(1, lepto_pos - 8)
            end   <- min(nchar(l), lepto_pos + 14)
            seg   <- substr(l, start, end)
            cand  <- str_extract(seg, "\\b\\d{1,4}\\b")
            val   <- suppressWarnings(as.integer(cand))
          }
        }
        
        if (!is.na(val)) {
          page_out[[length(page_out) + 1L]] <- data.table(
            district = district, cases = val,
            page = p, method = if (!is.na(a_token_idx)) "columns" else "window"
          )
        }
      }
      # i <- i + 1L
    }
    
    if (length(page_out)) {
      out <- rbindlist(page_out)
      out[, url := url]
      results[[length(results) + 1L]] <- out
    }
  }
  
  if (!length(results)) return(NULL)
  rbindlist(results, fill = TRUE)
}

idx = idx_fn

urls = idx$url # [451]

# out <- try(extract_lepto_anylayout(u), silent = TRUE)

atp=1
allresults = list()
for (atp in 1:nrow(idx)){
  currow = idx[atp]
  res =  try(extract_lepto_anylayout(currow$url), silent = TRUE)
  if (!is.null(res)){
    allresults[[atp]] <- res
    allresults[[atp]]$date_start = currow$date_start
    allresults[[atp]]$date_end = currow$date_end
    cat(atp, '\n')
    
  } else {
    allresults[[atp]] <- data.table()
    cat("couldn't get data", atp, '\n')
    
  }

}

allresults = rbindlist(allresults, fill=TRUE)

# 
# allresults[district == 'Colombo']
# allresults2 = allresults[,c("district","cases","date_end")]
# allresults2[district == 'Colombo']
# plot(allresults2[district == 'Colombo']$cases, type = 'l')
# write.csv(allresults, 'xxWER_leptospirosis_counts.csv')
write.csv(allresults, "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/WER_leptospirosis_counts.csv")


#################################################################################
#################################################################################
#################################################################################
#################################################################################
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


setDTthreads(8)






# MID YEAR POPULATION ESTIMATES 2014-2023


library(data.table)
library(stringr)

Sys.setenv("JAVA_HOME"="C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot")

library(tabulapdf)
library(tabulizerjars)
# remotes::install_github(c("ropensci/tabulizerjars", "ropensci/tabulizer"), INSTALL_opts = "--no-multiarch")

# --- CONFIG ---
pdf_path <- "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/Mid-year_population_by_district_and_sex_2024.pdf"  # adjust if needed
tabs <- extract_tables(pdf_path, method = "lattice", guess = TRUE, output = "tibble")

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



# head(allpopdat)
# 



# ---- Inputs (rename these if your column names differ) ----
# allpopdat: columns = year (char/int), district (char), poptot (numeric; absolute count)
# lepto_dt : columns = date (IDate/Date/char), district (char), cases (int)

# Example: if your lepto table uses different names, rename like:
# setnames(lepto_dt, c("your_date_col","your_dist_col","your_count_col"),
#                     c("date","district","cases"))





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

build_pop_lookup <- function(pop_dt, reference_year = 2012L) {
  # pop_dt: year, district, poptot
  DT <- copy(pop_dt)
  # coerce types
  DT[, year := as.integer(year)]
  DT[, district := normalize_district(district)]
  # drop national total rows
  DT <- DT[!district %in% c("Sri Lanka","Sri-lanka","Sri_Lanka")]
  
  # if exact ref year exists, use it; otherwise nearest <= ref; otherwise nearest overall
  if (reference_year %in% DT$year) {
    sel <- DT[year == reference_year, .(district, pop = as.numeric(poptot))]
  } else {
    # nearest year <= ref
    yrs_le <- DT[year <= reference_year, max(year, na.rm = TRUE)]
    if (is.finite(yrs_le)) {
      sel <- DT[year == yrs_le, .(district, pop = as.numeric(poptot))]
    } else {
      # absolute nearest year
      nearest <- DT[, .(year = year[which.min(abs(year - reference_year))]), by = district]
      sel <- nearest[DT, on = .(district, year), .(district, pop = as.numeric(poptot))]
    }
  }
  # ensure one row per district
  sel <- unique(sel, by = "district")
  sel
}

prepare_lepto <- function(lepto_dt) {
  DT <- copy(lepto_dt)
  # coerce types
  if (!inherits(DT$date, "Date")) DT[, date := as.IDate(date)]
  DT[, district := normalize_district(district)]
  DT[, cases := as.integer(cases)]
  DT
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


# -------- Paths --------
lepto_path <-  "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/WER_leptospirosis_counts.csv"

# ---- Build lookup + join ----
# pop_lookup <- build_pop_lookup(allpopdat, reference_year = 2012L)


lepto = fread(lepto_path)
lepto[, district := norm_dist(district)]
lepto$V1 = NULL
lepto$year = lyear(lepto$date_end)
lepto[year >= 2014, year2merge := year]
lepto[year < 2014, year2merge := 2014]




allpopdat[, district := norm_dist(district)]

lepto = merge(lepto, allpopdat, by.x = c("district","year2merge"), by.y = c("district","year"), all=F)

lepto[, rate_per_100k := (cases / poptot) * 1e5][]

# ---- Optional: quality checks ----
missing_pop <- lepto[is.na(poptot), unique(district)]
if (length(missing_pop)) {
  message("No population match for districts: ",
          paste(sort(missing_pop), collapse = ", "))
}
# names(lepto)
lepto$V1 = NULL
lepto$url = NULL
lepto$page = NULL
lepto$method = NULL
lepto = lepto[!is.na(district)]

names(lepto)



####################################################################################
####################################################################################
####################################################################################
####################################################################################

# link lepto with station data.

wx_station_data = fread("C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/station_data/SriLanka_Weather_Dataset.csv", header=TRUE)

head(wx_station_data)
str(wx_station_data)

wx_station_data[,.N,by=city][order(city)]
wx_station_data[!city %in% lepto$district]



# ?????? 0) Inputs ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# data.tables you already have:
#   - lepto:   columns at least district, date, cases   (date = Date/IDate)
#   - wx_station_data: daily rows with cols 'time' (Date/IDate), 'latitude','longitude','city', and weather vars

stopifnot(all(c("time","latitude","longitude","city") %in% names(wx_station_data)))
if (!inherits(wx_station_data$time, "Date")) wx_station_data[, time := as.IDate(time)]

lepto$date = lepto$date_start

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

# (Optional special admin handling: Kalmunai is in Ampara; if it ever appears as a district in lepto)
lepto[district == "Kalmunai", district := "Ampara"]

# Join weather to lepto by district + date
setkey(lepto, district, date)
setkey(wx_dist_daily, district, date)
lepto_weather <- wx_dist_daily[lepto]  # left join onto lepto; keeps all lepto rows

# ?????? 5) Quick diagnostics ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
if (anyNA(lepto_weather$temperature_2m_mean)) {
  miss <- lepto_weather[is.na(temperature_2m_mean), .N, by = district][order(-N)]
  message("Lepto rows lacking matched weather (top):")
  print(head(miss, 10))
}

# ?????? 6) Result ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
# 'lepto_weather' now has district, date, cases (+ your other lepto fields) and the district-aggregated daily weather.
lepto = lepto_weather

####################################################################################
####################################################################################
####################################################################################
####################################################################################

# MERGE WITH LAND COVER DATA.

# ?????? 0) Inputs ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
lc_path <- "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/SriLanka_Landcover_2018/SriLanka_Landcover_2018.tif"
r <- rast(lc_path)

# Define your class legend (update if your TIFF uses different codes)
class_map <- data.table(
  code  = c(10,20,30,40,50,60,70,80,90,255),
  label = c("Water","BuiltUp","Cropland","Forest","Shrub",
            "Grass","Bare","Wetland","Paddy","NoData")
)

# ?????? 1) Get the correct **districts (ADM2)** ???????????????????????????????????????????????????????????????????????????????????????????????????
gadm_zip <- "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_LKA_shp.zip"
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
######################################################################
######################################################################

lepto[, .(rate = mean(rate_per_100k, na.rm = TRUE),
          paddy = mean(Paddy),
          wetland = mean(Wetland),
          built = mean(BuiltUp)), 
      by = district]

num_vars <- c("rate_per_100k","BuiltUp","Cropland","Grass","Paddy","Shrub","Water","Wetland")
cor_mat <- cor(lepto[, ..num_vars], use = "complete.obs")
cor_mat["rate_per_100k",]




lepto[, high_paddy := Paddy > median(Paddy, na.rm=TRUE)]
lepto[, .(mean_rate = mean(rate_per_100k, na.rm=TRUE)), by = high_paddy]


m1 <- lm(rate_per_100k ~ Paddy + Wetland + BuiltUp + Water, data = lepto)
summary(m1)

m1 <- lm(rate_per_100k ~ precipitation_hours + temperature_2m_max + temperature_2m_min, data = lepto)
summary(m1)


countna(lepto)


lepto[district == "Ampara" & year == 2008][order(date_start),
                                           .(date_start, cases, new_cases = c(cases[1], diff(cases)))]

# Step 1: enforce monotonic cumulative cases
lepto[, cases_monotonic := cummax(cases), by = .(district, year)]

# Step 2: compute weekly incident cases
lepto[, new_cases := c(cases_monotonic[1], diff(cases_monotonic)), 
      by = .(district, year)]

# Step 3: optional check for negatives
summary(lepto$new_cases)  # should all be >= 0





######################################################################
######################################################################
######################################################################
######################################################################



######################################################################
######################################################################
######################################################################
######################################################################




fig_dir <-  "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/figures/"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

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
lep_raw <- fread(lepto_path)

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














