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

setDTthreads(8)

# -------- Paths --------
lepto_path <-  "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/WER_leptospirosis_counts.csv"
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
# -------- Paths --------
in_path <-  "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/WER_leptospirosis_counts.csv"
fig_dir <-  "C:/Users/jordan/R_Projects/CHI-Data/analysis/sri_lanka/outputs/figures/"

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














