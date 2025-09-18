# =============================================================================
# Madagascar - Workability & Disease Plots (base R + data.table + arrow)
# Combines legacy "core" analyses with new workability metrics.
# Outputs: PNG figures + CSV tables for both per-village and collapsed summaries.
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(lubridate)
  library(here)
})

# ---- Helper functions ----
open_file <- function(path) {
  if (requireNamespace("berryFunctions", quietly = TRUE)) {
    berryFunctions::openFile(path)
  } else {
    utils::browseURL(paste0("file:///", normalizePath(path)))
  }
}
draw_hgrid <- function(y) abline(h = pretty(y), col = "gray90", lwd = 1)
draw_vgrid <- function(x) abline(v = pretty(x), col = "gray95", lwd = 1)

# Color palette for villages
cols <- c("Mandena" = "#1b9e77", "Sarahandrano" = "#d95f02")

# ---- Paths ----
Sys.setenv("ERA5_STREAM"  = "C:/Users/jordan/Desktop/madagascar/stream")
Sys.setenv("figures_dir"  = "C:/Users/jordan/R_Projects/CHI-Data/analysis/madagascar/outputs/figures")
Sys.setenv("tables_dir"   = "C:/Users/jordan/R_Projects/CHI-Data/analysis/madagascar/outputs/tables")

stream_dir  <- Sys.getenv("ERA5_STREAM")
figures_dir <- Sys.getenv("figures_dir")
tables_dir  <- Sys.getenv("tables_dir")

dir.create(figures_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(tables_dir,  showWarnings = FALSE, recursive = TRUE)

# ---- Focus ----
years_keep   <- 1980:2024
scheme_focus <- "idw4"  # can also set to "nearest1"

# ---- Data location ----
daily_path <- file.path(stream_dir, "daily_agg")

# ---- 1) Read & prep ----
daily_ds <- open_dataset(daily_path, format = "parquet", unify_schemas = TRUE)

daily_all <- daily_ds |>
  mutate(
    year  = year(date),
    month = month(date),
    wbgt28_h = cast(wbgt28_h, float64()),
    wbgt31_h = cast(wbgt31_h, float64()),
    hot35_h  = cast(hot35_h,  float64()),
    # extended workability metrics
    work_p05 = cast(work_p05, float64()),
    work_p25 = cast(work_p25, float64()),
    work_p50 = cast(work_p50, float64()),
    work_p75 = cast(work_p75, float64()),
    work_p95 = cast(work_p95, float64()),
    work_iqr = cast(work_iqr, float64()),
    work_es20 = cast(work_es20, float64()),
    hours_safe    = cast(hours_safe,    float64()),
    hours_caution = cast(hours_caution, float64()),
    hours_high    = cast(hours_high,    float64()),
    dh28_sum = cast(dh28_sum, float64()),
    dh31_sum = cast(dh31_sum, float64()),
    max_block70_h = cast(max_block70_h, int32()),
    work_morn = cast(work_morn, float64()),
    work_mid  = cast(work_mid,  float64()),
    work_aft  = cast(work_aft,  float64()),
    work_nite = cast(work_nite, float64()),
    start4h70 = cast(start4h70, int32()),
    eff_work_min_light    = cast(eff_work_min_light,    float64()),
    eff_work_min_moderate = cast(eff_work_min_moderate, float64()),
    eff_work_min_heavy    = cast(eff_work_min_heavy,    float64()),
    loss_area = cast(loss_area, float64())
  ) |>
  select(
    scheme, village, date, year, month,
    tmean_c, tmax_c, tmin_c, dtr_c, rh_mean, vpd_kpa,
    ssrd_MJ_day, wbgt28_h, wbgt31_h, hot35_h,
    work_day_pct, work_night_pct, work_all_pct,
    palm_hours, anth_hours,
    work_p05, work_p25, work_p50, work_p75, work_p95, work_iqr, work_es20,
    hours_safe, hours_caution, hours_high,
    dh28_sum, dh31_sum,
    max_block70_h,
    work_morn, work_mid, work_aft, work_nite,
    start4h70,
    eff_work_min_light, eff_work_min_moderate, eff_work_min_heavy,
    loss_area
  ) |>
  collect() |>
  as.data.table()

# ---- Filter years ----
daily <- daily_all[scheme == scheme_focus & year %in% years_keep]
comp  <- daily[, .(frac_ok = mean(is.finite(work_all_pct))), by = .(village, year)]
keep_years <- comp[, .(ok = all(frac_ok >= 0.95)), by = year][ok == TRUE, year]
daily <- daily[year %in% keep_years]
setorder(daily, village, date)



# =============================================================================
# 2) CORE SERIES - per village
# =============================================================================

# (A) Phytophthora hours / year
yr_palm <- daily[, .(palm_hours = sum(palm_hours, na.rm = TRUE)), by = .(village, year)]
png1 <- file.path(figures_dir, "P1_palm_hours_yearlines_byvillage.png")
png(png1, 1600, 950, res = 150)
xl <- range(yr_palm$year); yl <- range(pretty(yr_palm$palm_hours, 8))
plot(NA, xlim = xl, ylim = yl, xlab="Year", ylab="Phytophthora-favorable hours / year",
     main="Phytophthora hours per village"); draw_hgrid(yl); draw_vgrid(xl)
for (vil in unique(yr_palm$village)) {
  s <- yr_palm[village == vil]
  lines(s$year, s$palm_hours, lwd=2, col=cols[vil])
  points(s$year, s$palm_hours, pch=16, col=cols[vil])
}
legend("topleft", bty="n", lwd=2, pch=16, col=cols, legend=names(cols))
dev.off(); open_file(png1)

# (B) Workability (all hours) / year
yr_work_all <- daily[, .(work_all = mean(work_all_pct, na.rm=TRUE)), by = .(village, year)]
png2 <- file.path(figures_dir, "P2_workability_all_yearlines_byvillage.png")
png(png2, 1600, 950, res = 150)
xl <- range(yr_work_all$year); yl <- range(pretty(yr_work_all$work_all, 8))
plot(NA, xlim=xl, ylim=yl, xlab="Year", ylab="Workability (%, all hours)",
     main="Workability (all hours) by village"); draw_hgrid(yl); draw_vgrid(xl)
for (vil in unique(yr_work_all$village)) {
  s <- yr_work_all[village==vil]
  lines(s$year, s$work_all, lwd=2, col=cols[vil])
  points(s$year, s$work_all, pch=16, col=cols[vil])
}
legend("bottomleft", bty="n", lwd=2, pch=16, col=cols, legend=names(cols))
dev.off(); open_file(png2)
# 
# # (C) Monthly daytime workability
# mon_work_day <- daily[, .(work_day = mean(work_day_pct, na.rm=TRUE)), by = .(village, month)]
# png3 <- file.path(figures_dir, "P3_workability_monthly_day_byvillage.png")
# png(png3, 1600, 950, res = 150)
# xl <- 1:12; yl <- range(pretty(mon_work_day$work_day, 8))
# plot(NA, xlim=xl, ylim=yl, xaxt="n", xlab="Month", ylab="Workability (%, 09-16)",
#      main="Monthly daytime workability by village")
# axis(1, at=1:12, labels=month.abb); draw_hgrid(yl); abline(v=1:12, col="gray97")
# for (vil in unique(mon_work_day$village)) {
#   s <- mon_work_day[village==vil]
#   lines(s$month, s$work_day, lwd=2, col=cols[vil])
#   points(s$month, s$work_day, pch=16, col=cols[vil])
# }
# legend("bottomleft", bty="n", lwd=2, pch=16, col=cols, legend=names(cols))
# dev.off(); open_file(png3)
# 
# # =============================================================================
# 3) NEW WORKABILITY INSIGHTS - per village
# =============================================================================

# (A) Schedulability
sched_month <- daily[, .(p_days_4h70 = mean(max_block70_h >= 4, na.rm=TRUE)),
                     by=.(village, year, month)]
sched_month <- sched_month[, .(p_days_4h70 = mean(p_days_4h70, na.rm=TRUE)), by=.(village, month)]
fwrite(sched_month, file.path(tables_dir, "T_schedulability_byvillage.csv"))
# ... (plots as in Script 1 above) ...

# (B) Degree-hours above 28/31
dh_month <- daily[, .(dh28 = mean(dh28_sum, na.rm=TRUE),
                      dh31 = mean(dh31_sum, na.rm=TRUE)), by=.(village, year, month)]
dh_month <- dh_month[, .(dh28=mean(dh28), dh31=mean(dh31)), by=.(village, month)]
fwrite(dh_month, file.path(tables_dir, "T_degree_hours_byvillage.csv"))

#############

pngA <- file.path(figures_dir, "P4_schedulability_share_days_ge4h70_by_month.png")
png(pngA, 1700, 950, res = 150)
xl <- c(1,12); yl <- c(0,1)
plot(NA, xlim=xl, ylim=yl, xaxt="n",
     xlab="Month", ylab="Share of days",
     main="Schedulability: days with ???4 contiguous hours at ???70%")
axis(1, at=1:12, labels=month.abb)
draw_hgrid(yl)
for (vil in unique(sched_month$village)) {
  s <- sched_month[village==vil]
  lines(s$month, s$p_days_4h70, lwd=2, col=cols[vil])
  points(s$month, s$p_days_4h70, pch=16, col=cols[vil])
}
legend("topright", bty="n", lwd=2, pch=16, col=cols, legend=names(cols))
dev.off(); open_file(pngA)

# ---- (B) Degree-hours above 28/31 ----
dh_month <- daily[, .(dh28 = mean(dh28_sum, na.rm = TRUE),
                      dh31 = mean(dh31_sum, na.rm = TRUE)),
                  by = .(village, year, month)]
dh_month <- dh_month[, .(dh28 = mean(dh28, na.rm = TRUE),
                         dh31 = mean(dh31, na.rm = TRUE)),
                     by = .(village, month)]
fwrite(dh_month, file.path(tables_dir, "T_degree_hours_climatology_dh28_dh31_byvillage.csv"))

pngB <- file.path(figures_dir, "P5_degree_hours_monthly_byvillage.png")
png(pngB, 1700, 950, res = 150)
yl <- range(0, pretty(c(dh_month$dh28, dh_month$dh31), 8))
plot(NA, xlim=c(1,12), ylim=yl, xaxt="n",
     xlab="Month", ylab="Degree-hours",
     main="Degree-hours above 28/31 by village")
axis(1, at=1:12, labels=month.abb); draw_hgrid(yl)
for (vil in unique(dh_month$village)) {
  s <- dh_month[village==vil]
  lines(s$month, s$dh28, lwd=2, col=cols[vil], lty=1)
  points(s$month, s$dh28, pch=16, col=cols[vil])
  lines(s$month, s$dh31, lwd=2, col=cols[vil], lty=2)
  points(s$month, s$dh31, pch=17, col=cols[vil])
}
legend("topleft", bty="n",
       col=rep(cols, each=2), lwd=2, lty=rep(c(1,2), length(cols)),
       pch=rep(c(16,17), length(cols)),
       legend=paste(rep(names(cols), each=2), c("DH>28","DH>31")))
dev.off(); open_file(pngB)

# # ---- (C) Effective work minutes per day ----
# eff_year <- daily[, .(
#   light    = mean(eff_work_min_light,    na.rm = TRUE),
#   moderate = mean(eff_work_min_moderate, na.rm = TRUE),
#   heavy    = mean(eff_work_min_heavy,    na.rm = TRUE)
# ), by = .(village, year)]
# fwrite(eff_year, file.path(tables_dir, "T_effective_work_minutes_yearly_byvillage.csv"))
# 
# pngC <- file.path(figures_dir, "P6_effective_work_minutes_yearly_byvillage.png")
# png(pngC, 1700, 950, res = 150)
# xl <- range(eff_year$year); yl <- range(pretty(unlist(eff_year[, .(light,moderate,heavy)]), 8))
# plot(NA, xlim=xl, ylim=yl, xlab="Year", ylab="Effective work minutes / day",
#      main="Effective work minutes by workload & village")
# draw_hgrid(yl); draw_vgrid(xl)
# ltys <- c(light=1, moderate=2, heavy=3)
# pchs <- c(light=16, moderate=17, heavy=15)
# for (vil in unique(eff_year$village)) {
#   s <- eff_year[village==vil]
#   for (var in c("light","moderate","heavy")) {
#     lines(s$year, s[[var]], lwd=2, col=cols[vil], lty=ltys[var])
#     points(s$year, s[[var]], pch=pchs[var], col=cols[vil])
#   }
# }
# legend("topleft", bty="n",
#        col=rep(cols, each=3), lwd=2,
#        lty=rep(1:3, length(cols)), pch=rep(c(16,17,15), length(cols)),
#        legend=paste(rep(names(cols), each=3), c("Light","Moderate","Heavy")))
# dev.off(); open_file(pngC)
# 
# # ---- (D) Earliest feasible 4-h start ----
# start_m <- daily[, .(start4h70 = start4h70), by = .(village, month, date)]
# 
# pngD <- file.path(figures_dir, "P7_earliest_start4h70_boxplot_by_month.png")
# png(pngD, 1700, 950, res = 150)
# plot(1, type="n", xlim=c(0.5,12.5), ylim=c(-1,24),
#      xaxt="n", xlab="Month", ylab="Earliest start hour (0-20, NA=none)",
#      main="Earliest feasible 4-h window ???70% by village")
# axis(1, at=1:12, labels=month.abb)
# abline(h = seq(0,24,2), col="gray95")
# for (vil in unique(start_m$village)) {
#   x <- split(start_m[village==vil]$start4h70, start_m$month)
#   for (m in 1:12) {
#     vals <- x[[as.character(m)]]
#     if (all(is.na(vals))) next
#     boxplot(vals, at=m+ (which(names(cols)==vil)-1)*0.25,
#             add=TRUE, outline=FALSE, axes=FALSE,
#             col=adjustcolor(cols[vil], alpha=0.5), border=cols[vil])
#   }
# }
# legend("topright", bty="n", fill=cols, border=cols, legend=names(cols))
# dev.off(); open_file(pngD)

# ---- (E) Workability distribution percentiles ----
wdist <- daily[, .(
  p05 = mean(work_p05, na.rm=TRUE),
  p25 = mean(work_p25, na.rm=TRUE),
  p50 = mean(work_p50, na.rm=TRUE),
  p75 = mean(work_p75, na.rm=TRUE),
  p95 = mean(work_p95, na.rm=TRUE)
), by=.(village, month)]
fwrite(wdist, file.path(tables_dir, "T_workability_distribution_monthly_byvillage.csv"))

pngE <- file.path(figures_dir, "P8_workability_percentiles_monthly_byvillage.png")
png(pngE, 1700, 950, res = 150)
plot(NA, xlim=c(1,12), ylim=c(0,100), xaxt="n",
     xlab="Month", ylab="Workability (%)",
     main="Workability percentiles by village")
axis(1, at=1:12, labels=month.abb); draw_hgrid(c(0,100))
for (vil in unique(wdist$village)) {
  s <- wdist[village==vil]
  lines(s$month, s$p50, lwd=2, col=cols[vil])
  lines(s$month, s$p25, lwd=1, col=cols[vil], lty=2)
  lines(s$month, s$p75, lwd=1, col=cols[vil], lty=2)
}
legend("bottomleft", bty="n",
       col=rep(cols, each=3), lwd=c(2,1,1), lty=c(1,2,2),
       legend=paste(rep(names(cols), each=3), c("P50","P25","P75")))
dev.off(); open_file(pngE)

# # ---- (F) WBGT band composition ----
# bands_month <- daily[, .(
#   safe    = mean(hours_safe,    na.rm=TRUE),
#   caution = mean(hours_caution, na.rm=TRUE),
#   high    = mean(hours_high,    na.rm=TRUE)
# ), by=.(village, month)]
# fwrite(bands_month, file.path(tables_dir, "T_wbgt_band_hours_per_day_monthly_byvillage.csv"))
# 
# pngF <- file.path(figures_dir, "P9_wbgt_band_hours_per_day_monthly_byvillage.png")
# png(pngF, 1700, 950, res = 150)
# yl <- c(0, max(rowSums(bands_month[, .(safe,caution,high)]), na.rm=TRUE)*1.2)
# for (vil in unique(bands_month$village)) {
#   s <- t(as.matrix(bands_month[village==vil, .(safe,caution,high)]))
#   colnames(s) <- month.abb
#   barplot(s, beside=FALSE, col=c("#2E7D32","#F9A825","#C62828"),
#           ylim=yl, main=paste("WBGT bands -", vil),
#           ylab="Mean hours/day")
#   legend("topright", fill=c("#2E7D32","#F9A825","#C62828"),
#          bty="n", legend=c("Safe","Caution","High"))
# }
# dev.off(); open_file(pngF)

# ---- (G) Loss area (risk-weighted deficit) ----
loss_year <- daily[, .(loss = mean(loss_area, na.rm=TRUE)), by=.(village, year)]
fwrite(loss_year, file.path(tables_dir, "T_loss_area_yearly_byvillage.csv"))

pngG <- file.path(figures_dir, "P10_loss_area_yearly_byvillage.png")
png(pngG, 1700, 950, res = 150)
xl <- range(loss_year$year); yl <- range(pretty(loss_year$loss, 8))
plot(NA, xlim=xl, ylim=yl, xlab="Year", ylab="Loss area",
     main="Risk-weighted workability loss by village")
draw_hgrid(yl); draw_vgrid(xl)
for (vil in unique(loss_year$village)) {
  s <- loss_year[village==vil]
  lines(s$year, s$loss, lwd=2, col=cols[vil])
  points(s$year, s$loss, pch=16, col=cols[vil])
}
legend("topleft", bty="n", lwd=2, pch=16, col=cols, legend=names(cols))
dev.off(); open_file(pngG)

# ---- (H) Max contiguous workable block ----
blk <- daily[, .(max_block70_h = max_block70_h), by=.(village, date)]
tbl_blk <- blk[, .N, by=.(village, max_block70_h)][order(village, max_block70_h)]
fwrite(tbl_blk, file.path(tables_dir, "T_max_block70_histogram_byvillage.csv"))

pngH <- file.path(figures_dir, "P11_max_block70_histogram_byvillage.png")
png(pngH, 1500, 950, res = 150)
for (vil in unique(tbl_blk$village)) {
  s <- tbl_blk[village==vil]
  barplot(s$N, names.arg=s$max_block70_h,
          col=cols[vil], border="gray40",
          main=paste("Max contiguous block -", vil),
          xlab="Hours (???70%)", ylab="Count")
}
dev.off(); open_file(pngH)

# ---- (I) Shift-window workability ----
shift_month <- daily[, .(
  morn = mean(work_morn, na.rm=TRUE),
  mid  = mean(work_mid,  na.rm=TRUE),
  aft  = mean(work_aft,  na.rm=TRUE),
  nite = mean(work_nite, na.rm=TRUE)
), by=.(village, month)]
fwrite(shift_month, file.path(tables_dir, "T_shift_window_workability_monthly_byvillage.csv"))

pngI <- file.path(figures_dir, "P12_shift_window_workability_monthly_byvillage.png")
png(pngI, 1700, 950, res = 150)
plot(NA, xlim=c(1,12), ylim=c(0,100), xaxt="n",
     xlab="Month", ylab="Workability (%)",
     main="Shift-window workability by village")
axis(1, at=1:12, labels=month.abb); draw_hgrid(c(0,100))
pchs <- c(morn=16, mid=17, aft=15, nite=18)
ltys <- c(morn=1, mid=2, aft=3, nite=4)
for (vil in unique(shift_month$village)) {
  s <- shift_month[village==vil]
  for (var in c("morn","mid","aft","nite")) {
    lines(s$month, s[[var]], lwd=2, col=cols[vil], lty=ltys[var])
    points(s$month, s[[var]], pch=pchs[var], col=cols[vil])
  }
}
legend("bottomleft", bty="n",
       col=rep(cols, each=4), lwd=2, lty=rep(1:4, length(cols)),
       pch=rep(c(16,17,15,18), length(cols)),
       legend=paste(rep(names(cols), each=4),
                    c("Morning","Midday","Afternoon","Night")))
dev.off(); open_file(pngI)


# =============================================================================
# 4) Save compact CSVs for the core figures (legacy + new)
# =============================================================================
fwrite(yr_palm,              file.path(tables_dir, "T_yearly_palm_hours_mean.csv"))
fwrite(yr_work_all,          file.path(tables_dir, "T_yearly_workability_all_mean.csv"))
fwrite(mon_work_day,         file.path(tables_dir, "T_monthly_workability_day_clim_mean.csv"))
# new already written above.

cat("\n??? Wrote plots & tables to:\n", normalizePath(figures_dir), "\n", normalizePath(tables_dir), "\n")


# (C) Effective work minutes (light, moderate, heavy)
# (D) Earliest feasible 4h start (boxplots by village)
# (E) Workability distribution percentiles (P25/P50/P75)
# (F) WBGT band composition
# (G) Loss area
# (H) Max contiguous block
# (I) Shift-window workability
# >>> (all retained from Script 1, per-village)
