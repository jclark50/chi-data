# Robust Figure Pack builder (handles corrupt PNG headers)

if (!requireNamespace("magick", quietly = TRUE)) install.packages("magick")
if (!requireNamespace("png", quietly = TRUE))    install.packages("png")
library(magick)

fig_dir <- Sys.getenv("figures_dir")
stopifnot(dir.exists(fig_dir))

png_files <- list.files(fig_dir, pattern = "\\.png$", full.names = TRUE)
png_files <- sort(png_files)

log <- data.frame(file = basename(png_files),
                  size_bytes = file.info(png_files)$size,
                  status = NA_character_, stringsAsFactors = FALSE)

safe_read <- function(p) {
  # 0-byte guard
  if (is.na(file.info(p)$size) || file.info(p)$size == 0) {
    stop("zero-byte file")
  }
  # try magick first
  tryCatch(
    magick::image_read(p),
    error = function(e) {
      # fallback: read via png -> re-encode to a temp PNG
      arr <- try(png::readPNG(p), silent = TRUE)
      if (inherits(arr, "try-error")) stop("png::readPNG failed")
      # write to temp clean PNG using base device (re-encode)
      tf <- tempfile(fileext = ".png")
      # Preserve pixel dims from array
      h <- dim(arr)[1]; w <- dim(arr)[2]
      png(tf, width = w, height = h)
      op <- par(mar = c(0,0,0,0)); plot.new()
      rasterImage(arr, 0, 0, 1, 1, interpolate = FALSE)
      par(op); dev.off()
      magick::image_read(tf)
    }
  )
}

# Title + index pages (same as before, light tweaks)
title_txt <- sprintf(
  "Madagascar - Climate & Workability Figure Pack\n\nGenerated: %s\nScheme: %s\nYears: %s",
  format(Sys.time(), "%Y-%m-%d %H:%M"),
  get0("scheme_focus", ifnotfound = "idw4"),
  paste(range(get0("years_keep", ifnotfound = 1980:2024)), collapse = "-")
)
title_img <- image_blank(width = 2480, height = 3508, color = "white") |>
  image_annotate(title_txt, gravity = "center", size = 80, color = "black")

idx <- data.frame(
  page  = seq_along(png_files) + 2L,
  label = sub("\\.png$", "", basename(png_files), ignore.case = TRUE),
  stringsAsFactors = FALSE
)
idx_lines <- paste(sprintf("%-5s  %s", idx$page, idx$label), collapse = "\n")
index_img <- image_blank(width = 2480, height = 3508, color = "white") |>
  image_annotate("Figure Index (page ??? figure name)\n\n", gravity = "north", size = 70) |>
  image_annotate(idx_lines, gravity = "north", location = "+150+300", size = 48)

# Read/repair each PNG, resize to A4, and collect
imgs <- list()
for (i in seq_along(png_files)) {
  p <- png_files[i]
  img <- try(safe_read(p), silent = TRUE)
  if (inherits(img, "try-error")) {
    log$status[i] <- "SKIPPED (unreadable)"
    next
  }
  log$status[i] <- if (grepl("magick-image", paste(class(img), collapse = " "))) "OK" else "REENCODED"
  img2 <- img |>
    image_resize("2480x3508") |>
    image_extent("2480x3508", color = "white")
  imgs[[length(imgs) + 1]] <- img2
}

# Write PDF + log
pdf_out <- file.path(fig_dir, sprintf("FigurePack_%s.pdf", format(Sys.time(), "%Y%m%d_%H%M")))
all_pages <- c(list(title_img, index_img), imgs) |> image_join()
image_write(all_pages, path = pdf_out, format = "pdf")

qa_log <- file.path(fig_dir, sprintf("FigurePack_QA_%s.csv", format(Sys.time(), "%Y%m%d_%H%M")))
write.csv(log, qa_log, row.names = FALSE)

message("??? Wrote Figure Pack: ", pdf_out)
message("??? QA log: ", qa_log)



##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################



# Summary: Combine all CSVs from tables_dir into one Excel with an Index sheet.
# Requires: openxlsx (no Java), data.table for fast reads.

if (!requireNamespace("openxlsx", quietly = TRUE)) install.packages("openxlsx")
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table")

library(openxlsx); library(data.table)

tbl_dir <- Sys.getenv("tables_dir")
stopifnot(dir.exists(tbl_dir))

csvs <- list.files(tbl_dir, pattern = "\\.csv$", full.names = TRUE)
stopifnot(length(csvs) > 0)

# Helper: guess simple column classes for formatting
guess_fmt <- function(DT) {
  sapply(DT, function(x) {
    if (inherits(x, c("Date","IDate"))) "date"
    else if (inherits(x, c("POSIXct","POSIXt"))) "datetime"
    else if (is.numeric(x) && any(grepl("(pct|percent|share)", tolower(names(DT)[which(DT==x)[1]])))) "percent"
    else if (is.numeric(x)) "numeric"
    else "text"
  })
}

wb <- createWorkbook()

# Styles
s_header <- createStyle(textDecoration = "bold", fgFill = "#F2F2F2", halign = "center")
s_pct    <- createStyle(numFmt = "0.0%")
s_num    <- createStyle(numFmt = "0.00")
s_date   <- createStyle(numFmt = "yyyy-mm-dd")
s_dt     <- createStyle(numFmt = "yyyy-mm-dd hh:mm")
s_wrap   <- createStyle(wrapText = TRUE)

# Index sheet
addWorksheet(wb, "Index")
writeData(wb, "Index",
          data.frame(
            Sheet = character(0),
            File  = character(0),
            Rows  = integer(0),
            Cols  = integer(0),
            stringsAsFactors = FALSE
          ))
setColWidths(wb, "Index", cols = 1:4, widths = c(28, 60, 10, 10))
addStyle(wb, "Index", s_header, rows = 1, cols = 1:4, gridExpand = TRUE)

index_rows <- list()

# Add each CSV as its own sheet
for (p in sort(csvs)) {
  nm <- tools::file_path_sans_ext(basename(p))
  # Excel sheet names max 31 chars; sanitize
  sheet <- substr(gsub("[^A-Za-z0-9 _-]", "_", nm), 1, 31)
  addWorksheet(wb, sheet)
  DT <- fread(p, showProgress = FALSE)
  
  writeData(wb, sheet, DT, withFilter = TRUE)
  addStyle(wb, sheet, s_header, rows = 1, cols = 1:ncol(DT), gridExpand = TRUE)
  freezePane(wb, sheet, firstActiveRow = 2, firstActiveCol = 1)
  setColWidths(wb, sheet, cols = 1:ncol(DT), widths = "auto")
  
  # Light formatting by guessed types
  fmts <- guess_fmt(DT)
  for (j in seq_along(fmts)) {
    rng <- paste0(int2col(j), 2, ":", int2col(j), nrow(DT) + 1)
    if (fmts[j] == "percent") addStyle(wb, sheet, s_pct, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "numeric") addStyle(wb, sheet, s_num, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "date")    addStyle(wb, sheet, s_date, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "datetime")addStyle(wb, sheet, s_dt,   rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
  }
  
  # Update index info
  index_rows[[length(index_rows)+1]] <- data.frame(
    Sheet = sheet, File = basename(p), Rows = nrow(DT), Cols = ncol(DT),
    stringsAsFactors = FALSE
  )
}

# Write index table with hyperlinks
idx <- do.call(rbind, index_rows)
writeData(wb, "Index", idx, startRow = 2)
for (i in seq_len(nrow(idx))) {
  writeFormula(wb, "Index",
               x = makeHyperlinkString(sheet = idx$Sheet[i], row = 1, col = 1, text = idx$Sheet[i]),
               startRow = i + 2, startCol = 1)
}
addStyle(wb, "Index", s_header, rows = 1, cols = 1:4, gridExpand = TRUE)

# Save
xlsx_out <- file.path(tbl_dir, sprintf("TablesPack_%s.xlsx", format(Sys.time(), "%Y%m%d_%H%M")))
saveWorkbook(wb, xlsx_out, overwrite = TRUE)
message("??? Wrote Excel pack: ", xlsx_out)























# Summary: Combine all CSVs from tables_dir into one Excel with an Index sheet.
# Requires: openxlsx (no Java), data.table for fast reads.

if (!requireNamespace("openxlsx", quietly = TRUE)) install.packages("openxlsx")
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table")

library(openxlsx); library(data.table)

tbl_dir <- Sys.getenv("tables_dir")
stopifnot(dir.exists(tbl_dir))

csvs <- list.files(tbl_dir, pattern = "\\.csv$", full.names = TRUE)
stopifnot(length(csvs) > 0)

# Helper: guess simple column classes for formatting
guess_fmt <- function(DT) {
  sapply(DT, function(x) {
    if (inherits(x, c("Date","IDate"))) "date"
    else if (inherits(x, c("POSIXct","POSIXt"))) "datetime"
    else if (is.numeric(x) && any(grepl("(pct|percent|share)", tolower(names(DT)[which(DT==x)[1]])))) "percent"
    else if (is.numeric(x)) "numeric"
    else "text"
  })
}

wb <- createWorkbook()

# Styles
s_header <- createStyle(textDecoration = "bold", fgFill = "#F2F2F2", halign = "center")
s_pct    <- createStyle(numFmt = "0.0%")
s_num    <- createStyle(numFmt = "0.00")
s_date   <- createStyle(numFmt = "yyyy-mm-dd")
s_dt     <- createStyle(numFmt = "yyyy-mm-dd hh:mm")
s_wrap   <- createStyle(wrapText = TRUE)

# Index sheet
addWorksheet(wb, "Index")
writeData(wb, "Index",
          data.frame(
            Sheet = character(0),
            File  = character(0),
            Rows  = integer(0),
            Cols  = integer(0),
            stringsAsFactors = FALSE
          ))
setColWidths(wb, "Index", cols = 1:4, widths = c(28, 60, 10, 10))
addStyle(wb, "Index", s_header, rows = 1, cols = 1:4, gridExpand = TRUE)

index_rows <- list()

# Add each CSV as its own sheet
for (p in sort(csvs)) {
  nm <- tools::file_path_sans_ext(basename(p))
  # Excel sheet names max 31 chars; sanitize
  sheet <- substr(gsub("[^A-Za-z0-9 _-]", "_", nm), 1, 31)
  addWorksheet(wb, sheet)
  DT <- fread(p, showProgress = FALSE)
  
  writeData(wb, sheet, DT, withFilter = TRUE)
  addStyle(wb, sheet, s_header, rows = 1, cols = 1:ncol(DT), gridExpand = TRUE)
  freezePane(wb, sheet, firstActiveRow = 2, firstActiveCol = 1)
  setColWidths(wb, sheet, cols = 1:ncol(DT), widths = "auto")
  
  # Light formatting by guessed types
  fmts <- guess_fmt(DT)
  for (j in seq_along(fmts)) {
    rng <- paste0(int2col(j), 2, ":", int2col(j), nrow(DT) + 1)
    if (fmts[j] == "percent") addStyle(wb, sheet, s_pct, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "numeric") addStyle(wb, sheet, s_num, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "date")    addStyle(wb, sheet, s_date, rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
    if (fmts[j] == "datetime")addStyle(wb, sheet, s_dt,   rows = 2:(nrow(DT)+1), cols = j, gridExpand = TRUE)
  }
  
  # Update index info
  index_rows[[length(index_rows)+1]] <- data.frame(
    Sheet = sheet, File = basename(p), Rows = nrow(DT), Cols = ncol(DT),
    stringsAsFactors = FALSE
  )
}

# Write index table with hyperlinks
idx <- do.call(rbind, index_rows)
writeData(wb, "Index", idx, startRow = 2)
for (i in seq_len(nrow(idx))) {
  writeFormula(wb, "Index",
               x = makeHyperlinkString(sheet = idx$Sheet[i], row = 1, col = 1, text = idx$Sheet[i]),
               startRow = i + 2, startCol = 1)
}
addStyle(wb, "Index", s_header, rows = 1, cols = 1:4, gridExpand = TRUE)

# Save
xlsx_out <- file.path(tbl_dir, sprintf("TablesPack_%s.xlsx", format(Sys.time(), "%Y%m%d_%H%M")))
saveWorkbook(wb, xlsx_out, overwrite = TRUE)
message("??? Wrote Excel pack: ", xlsx_out)












