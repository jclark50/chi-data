# Summary: Build an index of WER issues. Parse filename when possible; otherwise
# read the PDF header text to extract volume/issue/week and date range.

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(pdftools)
})
suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(lubridate)
})

library(xml2)

library(rvest)

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


# head(pdfs)
# pdfs = pdfs[1]




# Build initial index from filenames
idx_fn <- rbindlist(lapply(pdfs, parse_issue_from_filename_v34plus), fill = TRUE)

idx_path  <- "sri_lanka_WER_index.csv"
write.csv(allresults, idx_path)



suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(httr)
  library(pdftools)
})

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


allresults[district == 'Colombo']

allresults2 = allresults[,c("district","cases","date_end")]

allresults2[district == 'Colombo']

plot(allresults2[district == 'Colombo']$cases, type = 'l')


write.csv(allresults, 'xxWER_leptospirosis_counts.csv')



# 
# # For rows missing key fields, extract from PDF text
# need_pdf <- idx_fn[is.na(week) | is.na(year) | is.na(volume) | is.na(issue), url]
# idx_pdf  <- if (length(need_pdf)) rbindlist(lapply(need_pdf, extract_meta_from_pdf), fill = TRUE) else data.table()
# 
# # Merge and coalesce columns
# idx <- merge(idx_fn, idx_pdf, by = c("url","file"), all = TRUE, suffixes = c("_fn",""))
# if (nrow(idx_pdf)) {
#   for (col in c("volume","issue","week","year","date_start","date_end")) {
#     fn <- paste0(col, "_fn")
#     if (fn %in% names(idx)) {
#       idx[[col]] <- if (col %in% c("date_start","date_end")) idx[[col]] else suppressWarnings(as.integer(idx[[col]]))
#       idx[[col]] <- coalesce1(idx[[fn]], idx[[col]])
#       idx[[fn]]  <- NULL
#     }
#   }
# }
# 
# setorder(idx, -as.integer(year), -as.integer(week), -date_end, -date_start)
# fwrite(idx, "sri_lanka_WER_index.csv")
# idx[, .(file, year, week, volume, issue, date_start, date_end)][1:10]
# 
# 
# 
# 
# 
# 
# # 2) Download one recent PDF and prototype extraction
# #    (Pick the first with detectable year/week; adjust as needed.)
# cand <- wer_index[!is.na(year)][1]
# if (nrow(cand)) {
#   dest <- file.path(tempdir(), cand$file)
#   resp <- GET(cand$url, write_disk(dest, overwrite = TRUE), timeout(60))
#   stop_for_status(resp)
#   
#   txt <- pdf_text(dest)
#   # Heuristic: find the page/lines containing "Leptospirosis"
#   lines <- unlist(strsplit(txt, "\n", fixed = TRUE))
#   lep_ix <- grep("(?i)leptospirosis", lines)
#   snippet <- lines[pmax(1, lep_ix - 5):pmin(length(lines), lep_ix + 40)]
#   
#   # Very rough parse: look for lines like "Gampaha 12", "Kandy 5", etc.
#   # Maintain a district list to anchor extraction.
#   districts <- c("Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara Eliya",
#                  "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
#                  "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
#                  "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa","Badulla",
#                  "Monaragala","Ratnapura","Kegalle")
#   pat <- paste0("^(", paste(districts, collapse="|"), ")\\s+([0-9]{1,4})\\b")
#   
#   lep_dt <- data.table(raw = snippet)[
#     , .(district = str_match(raw, pat)[,2],
#         cases    = as.integer(str_match(raw, pat)[,3]))][!is.na(district)]
#   
#   fwrite(lep_dt, "WER_sample_leptospirosis_counts.csv")
#   print(lep_dt)
# } else {
#   message("No candidate WER PDF found with parsable year/week; inspect 'sri_lanka_WER_index.csv'.")
# }

# Summary: Analyze weekly leptospirosis cases (A) in Sri Lanka WER data:
# builds a complete district-week panel, computes national/district trends,
# YoY and 12-week changes, and writes synthesized tables to CSV.
# Summary: Clean leptospirosis WER data ??? complete weekly panel ??? trends, YoY,
# and district comparisons; outputs synthesized CSV tables.

# suppressPackageStartupMessages({
#   library(data.table)
#   library(lubridate)
# })
# 
# setDTthreads(8)
# 
# # -------- 0) Load & standardize --------
# idx_path   <- "sri_lanka_WER_index.csv"         # optional (used for completeness table)
# lepto_path <- "WER_leptospirosis_counts.csv"    # required
# out_dir    <- "outputs"
# dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
# 
# # Read what's available
# lep_raw <- fread(lepto_path)
# 
# # Keep only needed columns, tolerate different schemas
# need <- intersect(names(lep_raw),
#                   c("issue_id","district","date_end","A","B","cases","year","week",
#                     "url","method","page","date_start"))
# lep <- lep_raw[, ..need]
# 
# # If weekly column is named A, prefer that; otherwise use 'cases'
# if ("A" %in% names(lep) && !"cases" %in% names(lep)) {
#   setnames(lep, "A", "cases")
# } else if ("A" %in% names(lep) && "cases" %in% names(lep)) {
#   # prefer 'A' (weekly) if both exist
#   lep[, cases := fifelse(!is.na(A), as.integer(A), as.integer(cases))]
#   lep[, A := NULL]
# }
# # Ensure types
# lep[, date_end := as.IDate(date_end)]
# lep[, cases    := as.integer(cases)]
# lep <- lep[!is.na(date_end) & nzchar(district)]
# 
# # Canonicalize districts defensively
# canon <- c("Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara Eliya",
#            "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
#            "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
#            "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa","Badulla",
#            "Monaragala","Ratnapura","Kegalle","Kalmunai")
# # lep[, district := canon[match(tolower(district), tolower(canon))] %chin% FALSE]
# # lep[!(tolower(district) %chin% tolower(canon)),
# #     district := district]  # leave unknowns unchanged
# # lep[tolower(district) %chin% tolower(canon),
# #     district := canon[match(tolower(district), tolower(canon))]]
# 
# # Aggregate in case multiple lines per district/week exist
# lep_week <- lep[, .(cases = sum(cases, na.rm = TRUE)),
#                 by = .(district, date_end)]
# 
# # -------- 1) Complete district-week panel --------
# districts <- sort(unique(lep_week$district))
# weeks_seq <- seq(min(lep_week$date_end, na.rm = TRUE),
#                  max(lep_week$date_end, na.rm = TRUE), by = "7 days")
# 
# panel <- CJ(district = districts, date_end = weeks_seq, unique = TRUE)
# panel <- lep_week[panel, on = .(district, date_end)]
# panel[is.na(cases), cases := 0L]
# 
# # Time features
# panel[, `:=`(
#   iso_week = isoweek(date_end),
#   iso_year = isoyear(date_end),
#   month    = month(date_end),
#   year     = year(date_end)
# )]
# 
# setorder(panel, district, date_end)
# 
# # -------- 2) Rolling metrics & YoY deltas --------
# panel[, `:=`(
#   ma_4w   = frollmean(cases,  4, align = "right", na.rm = TRUE),
#   ma_12w  = frollmean(cases, 12, align = "right", na.rm = TRUE),
#   sum_12w = frollsum(cases,  12, align = "right", na.rm = TRUE)
# ), by = district]
# 
# # Year-over-year: same ISO week last ISO year (handles 52/53 weeks reasonably)
# prev <- panel[, .(district, iso_week, iso_year, cases_prev = cases)]
# prev[, iso_year := iso_year + 1L]  # shift to match "current" year on join
# 
# panel <- merge(panel, prev,
#                by = c("district","iso_week","iso_year"),
#                all.x = TRUE, sort = FALSE)
# 
# panel[, `:=`(
#   yoy_abs = cases - cases_prev,
#   yoy_pct = ifelse(!is.na(cases_prev) & cases_prev > 0,
#                    100 * (cases - cases_prev) / cases_prev, NA_real_)
# )]
# 
# # -------- 3) National aggregates --------
# nat <- panel[, .(cases   = sum(cases),
#                  ma_4w   = sum(ma_4w,   na.rm = TRUE),
#                  ma_12w  = sum(ma_12w,  na.rm = TRUE),
#                  sum_12w = sum(sum_12w, na.rm = TRUE)),
#              by = .(date_end, iso_week, iso_year)][order(date_end)]
# 
# # -------- 4) Synthesized tables --------
# # T1: National weekly trend
# T1_national_trend <- nat
# fwrite(T1_national_trend, file.path(out_dir, "T1_national_trend.csv"))
# 
# 
# T1_national_trend$doy = yday(T1_national_trend$date_end)
# 
# 
# plot(T1_national_trend$ma_12w)
# 
# 
# 
# doyavg = T1_national_trend[,lapply(.SD, mean, na.rm=TRUE), by = doy, .SDcols = c("cases")]
# plot(doyavg$cases, type = 'l')
# 
# 
# 
# 
# # T2: Latest week - district ranks (share of national, WoW, YoY)
# latest_week <- max(panel$date_end, na.rm = TRUE)
# prev_week   <- latest_week - 7
# 
# t2_base <- panel[date_end %in% c(prev_week, latest_week),
#                  .(cases = sum(cases)), by = .(district, date_end)]
# t2_wide <- dcast(t2_base, district ~ date_end, value.var = "cases", fill = 0)
# setnames(t2_wide, c("district","prev","curr"))
# 
# nat_curr <- sum(t2_wide$curr)
# T2_district_rank_latest <- merge(
#   t2_wide,
#   panel[date_end == latest_week, .(district, yoy_abs, yoy_pct)],
#   by = "district", all.x = TRUE
# )[order(-curr)][
#   , `:=`(
#     share_pct = if (nat_curr > 0) (curr / nat_curr) * 100 else NA_real_,
#     wow_abs   = curr - prev,
#     wow_pct   = ifelse(prev > 0, 100 * (curr - prev) / prev, NA_real_)
#   )
# ][, .(district, curr, share_pct, wow_abs, wow_pct, yoy_abs, yoy_pct)]
# 
# fwrite(T2_district_rank_latest, file.path(out_dir, "T2_district_rank_latest.csv"))
# 
# # T3: 12-week momentum vs previous 12 weeks
# end_12          <- latest_week
# start_12        <- latest_week - 7*12 + 1
# prior_12_start  <- start_12 - 7*12
# prior_12_end    <- end_12 - 7*12
# 
# win_curr <- panel[date_end >= start_12 & date_end <= end_12,
#                   .(sum_12 = sum(cases)), by = district]
# win_prev <- panel[date_end >= prior_12_start & date_end <= prior_12_end,
#                   .(sum_12_prev = sum(cases)), by = district]
# 
# T3_momentum_12w <- merge(win_curr, win_prev, by = "district", all.x = TRUE)[
#   , `:=`(
#     diff_abs = sum_12 - sum_12_prev,
#     diff_pct = ifelse(sum_12_prev > 0, 100 * (sum_12 - sum_12_prev) / sum_12_prev,
#                       NA_real_)
#   )
# ][order(-diff_abs)]
# 
# fwrite(T3_momentum_12w, file.path(out_dir, "T3_momentum_12w.csv"))
# 
# # T4: Seasonality profile - median & IQR by ISO week (per district)
# T4_seasonality <- panel[
#   , .(median_week = median(cases, na.rm = TRUE),
#       iqr_week    = IQR(cases, na.rm = TRUE)),
#   by = .(district, iso_week)
# ][order(district, iso_week)]
# 
# fwrite(T4_seasonality, file.path(out_dir, "T4_seasonality.csv"))
# 
# # T5: Completeness - districts per issue/week (if index file exists)
# if (file.exists(idx_path)) {
#   idx <- fread(idx_path)
#   keep <- intersect(names(idx), c("issue_id","date_end"))
#   if (length(keep) == 2L) {
#     idx[, date_end := as.IDate(date_end)]
#     T5_completeness <- lep_raw[
#       , .(n_districts = uniqueN(district)), by = issue_id
#     ][
#       idx[, .(issue_id, date_end)], on = "issue_id"
#     ][order(date_end)][
#       , flag_low := n_districts < 23L][]
#     fwrite(T5_completeness, file.path(out_dir, "T5_completeness.csv"))
#   }
# }
# 
# # T6: Peaks - top 5 weeks per district
# T6_peaks <- panel[order(-cases), .SD[1:5], by = district][
#   order(district, -cases),
#   .(district, date_end, cases, ma_4w, ma_12w)
# ]
# fwrite(T6_peaks, file.path(out_dir, "T6_peaks.csv"))
# 
# # T7: Annual totals by district (calendar year)
# T7_annual_totals <- panel[, .(annual_cases = sum(cases)), by = .(district, year)][
#   order(year, -annual_cases)
# ]
# fwrite(T7_annual_totals, file.path(out_dir, "T7_annual_totals.csv"))
# 
# # T8: Latest 52-week totals by district (rolling burden)
# T8_burden_52w <- panel[
#   date_end > (latest_week - 7*52),
#   .(cases_52w = sum(cases)),
#   by = district
# ][order(-cases_52w)]
# fwrite(T8_burden_52w, file.path(out_dir, "T8_burden_52w.csv"))
# 
# # -------- 5) Quick console peek --------
# cat("\nLatest week:", as.character(latest_week), "\n\n")
# print(T2_district_rank_latest[1:10])
# print(T3_momentum_12w[1:10])
# 
# 
# 
# 
# 



lep_raw$year = lyear(lep_raw$date_start)


lep_raw[,.N,by=year][order(year)]








# Summary: Build a district-week panel from WER data and save multiple plots:
# national trend (lines), top districts (bars), small-multiple trends, seasonality
# (boxplots), heatmap of rolling burden, and YoY change bars.

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(ggplot2)
  library(scales)
})

setDTthreads(8)

# -------- Paths --------
lepto_path <- "WER_leptospirosis_counts.csv"
fig_dir <- file.path("outputs", "figures")
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





















# Summary: Base R plots (no ggplot). Saves high-DPI PNGs via ragg::agg_png.
# Inputs: WER_leptospirosis_counts.csv (columns: district, date_end, A/cases)
# Outputs: PNGs in outputs/figures/

suppressPackageStartupMessages({
  library(data.table)
})

if (!requireNamespace("ragg", quietly = TRUE)) {
  stop("Package 'ragg' is required for agg_png output. Please install.packages('ragg').")
}

setDTthreads(8)

# ---------------- Paths ----------------
in_path <- "WER_leptospirosis_counts.csv"
fig_dir <- file.path("outputs", "figures")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

# ---------------- Load & standardize ----------------
lep_raw <- fread(in_path)

# prefer 'A' (weekly) if present; else 'cases'
if ("A" %in% names(lep_raw) && !"cases" %in% names(lep_raw)) {
  setnames(lep_raw, "A", "cases")
} else if ("A" %in% names(lep_raw) && "cases" %in% names(lep_raw)) {
  lep_raw[, cases := ifelse(!is.na(A), as.integer(A), as.integer(cases))]
}
keep <- intersect(names(lep_raw), c("district","date_end","cases","url"))
lep <- lep_raw[, ..keep]
lep[, date_end := as.Date(date_end)]
lep[, cases    := as.integer(cases)]
lep <- lep[!is.na(date_end) & !is.na(cases) & nzchar(district)]

# Canonicalize district names (defensive)
canon <- c("Colombo","Gampaha","Kalutara","Kandy","Matale","Nuwara Eliya",
           "Galle","Matara","Hambantota","Jaffna","Kilinochchi","Mannar",
           "Vavuniya","Mullaitivu","Batticaloa","Ampara","Trincomalee",
           "Kurunegala","Puttalam","Anuradhapura","Polonnaruwa","Badulla",
           "Monaragala","Ratnapura","Kegalle","Kalmunai")
match_idx <- match(tolower(lep$district), tolower(canon))
lep[, district := ifelse(is.na(match_idx), district, canon[match_idx])]

# Aggregate in case of duplicates per district/week
lep_week <- lep[, .(cases = sum(cases, na.rm = TRUE)), by = .(district, date_end)]

# Complete weekly grid
weeks_seq <- seq(min(lep_week$date_end, na.rm = TRUE),
                 max(lep_week$date_end, na.rm = TRUE), by = "7 days")
panel <- CJ(district = sort(unique(lep_week$district)),
            date_end = weeks_seq, unique = TRUE)
panel <- lep_week[panel, on = .(district, date_end)]
panel[is.na(cases), cases := 0L]

# Time features (base R ISO week/year)
panel[, `:=`(
  iso_week = as.integer(strftime(date_end, "%V")),  # ISO week
  iso_year = as.integer(strftime(date_end, "%G")),  # ISO year
  year     = as.integer(format(date_end, "%Y"))
)]
setorder(panel, district, date_end)

# Rolling metrics (data.table fast rolling)
panel[, `:=`(
  ma4   = frollmean(cases,  4, align = "right", na.rm = TRUE),
  ma12  = frollmean(cases, 12, align = "right", na.rm = TRUE),
  sum12 = frollsum(cases,  12, align = "right", na.rm = TRUE)
), by = district]

# YoY deltas: same ISO week, previous ISO year
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

# National weekly series
nat <- panel[, .(cases = sum(cases)), by = date_end][order(date_end)]
nat[, `:=`(
  ma4  = frollmean(cases,  4, align = "right"),
  ma12 = frollmean(cases, 12, align = "right")
)]

latest <- max(panel$date_end, na.rm = TRUE)

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














