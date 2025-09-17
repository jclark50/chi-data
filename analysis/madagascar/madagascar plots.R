# =============================================================================
# Madagascar - plots & tables from streamed daily parquet (base R only)
# Produces: yearly disease/workability lines, seasonal timing, risk-day bars,
# and "spells" (consecutive risk days). Opens each PNG after saving.
# =============================================================================

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(data.table); library(lubridate)
})

season_start_month <- 8    # Aug 1 season start (adjustable)
thr_7d             <- 24   # 7-day threshold (hours)
quiet_days         <- 15   # length of quiet window
quiet_max_hours    <- 6    # quiet window allowance
season_start <- 8    # Aug 1 season start (adjustable)


# ---- 0) Where is your streamed daily data? ----
stream_dir <- "C:/data/gridded/era5land/madagascar/madagascar_hourly_daily_streamed"
daily_path <- file.path(stream_dir, "daily_agg")  # change to "daily_agg_by_year" if you migrated

# Output folder for this run
viz_out <- file.path(stream_dir, paste0("deliverables_", format(Sys.time(), "%Y%m%d_%H%M")))
dir.create(viz_out, showWarnings = FALSE, recursive = TRUE)

# Focus
years_keep   <- 2000:2022
scheme_focus <- "nearest1"   # change to "idw4" to compare

# ---- 1) Read & prep ----
daily_ds <- open_dataset(daily_path, format = "parquet", unify_schemas = TRUE)

daily_all <- daily_ds |>
  mutate(
    year = year(date),
    month = month(date),
    # upcast counts to be safe across mixed partitions
    wbgt28_h = cast(wbgt28_h, float64()),
    wbgt31_h = cast(wbgt31_h, float64()),
    hot35_h  = cast(hot35_h,  float64())
  ) |>
  select(scheme, village, date, year, month,
         tmean_c, tmax_c, tmin_c, dtr_c, rh_mean, vpd_kpa,
         ssrd_MJ_day, wbgt28_h, wbgt31_h, hot35_h,
         work_day_pct, work_night_pct, work_all_pct,
         palm_hours, anth_hours) |>
  collect() |>
  as.data.table()

# Filter scheme/years and drop partial years (<95% days) to avoid end spikes
daily <- daily_all[scheme == scheme_focus & year %in% years_keep]
comp  <- daily[, .(frac_ok = mean(is.finite(palm_hours))), by = .(village, year)]
keep_years <- comp[, .(ok = all(frac_ok >= 0.95)), by = year][ok == TRUE, year]
daily <- daily[year %in% keep_years]
setorder(daily, village, date)

# Collapse 2 villages ??? single series (mean across villages)
collapse_by_year <- function(dt, value_col, fun = mean) {
  dt[, .(val = fun(get(value_col), na.rm = TRUE)), by = .(year)
  ][order(year)]
}
collapse_by_month <- function(dt, value_col, fun = mean) {
  dt[, .(val = fun(get(value_col), na.rm = TRUE)), by = .(month)
  ][order(month)]
}

# ---- 2) Season-year timing (Aug???Jul), center-of-mass + 30-day peak ----
season_start_month <- 8L

d2 <- copy(daily)
d2[, `:=`(
  season_year  = ifelse(month >= season_start_month, year, year - 1L),
  season_start = as.IDate(sprintf("%d-%02d-01",
                                  ifelse(month >= season_start_month, year, year - 1L),
                                  season_start_month)),
  season_doy   = as.integer(date - season_start) + 1L
)]
setorder(d2, village, date)
# Rolling sums for Phytophthora
d2[, `:=`(
  palm_7d  = frollsum(palm_hours,  7, align = "right", na.rm = TRUE),
  palm_30d = frollsum(palm_hours, 30, align = "right", na.rm = TRUE)
), by = village]

center_of_mass_doy <- function(doy, w) {
  w[!is.finite(w)] <- 0
  if (sum(w) <= 0) return(NA_integer_)
  maxd <- max(doy, na.rm = TRUE)
  ang  <- 2*pi * (doy - 1) / maxd
  x <- sum(w * cos(ang)); y <- sum(w * sin(ang))
  a <- atan2(y, x); if (a < 0) a <- a + 2*pi
  as.integer(round(a / (2*pi) * maxd)) + 1L
}

seasonal_vil <- d2[, .(
  com_doy  = center_of_mass_doy(season_doy, palm_hours),
  peak30_doy = if (all(is.na(palm_30d))) NA_integer_ else season_doy[which.max(palm_30d)]
), by = .(village, season_year)]

# Collapse villages ??? one value per season_year (mean of DOY)
seasonal <- seasonal_vil[, .(
  com_doy   = mean(com_doy,   na.rm = TRUE),
  peak30_doy = mean(peak30_doy, na.rm = TRUE)
), by = season_year][order(season_year)]

# ---- 3) Risk-days & spells (hours ??? days that matter) ----
riskday_min_hours   <- 4    # ???4 risky hours ??? risk day
sustained_7d_thresh <- 24   # 7-day sum ???24h ??? sustained

daily[, risk_day := palm_hours >= riskday_min_hours]
monthly_risk <- daily[, .(risk_days = mean(sum(risk_day), na.rm = TRUE)),
                      by = .(year, month)][, .(risk_days = mean(risk_days)), by = month]

# Spells: consecutive runs of risk days (collapse villages first)
risks_collapsed <- daily[, .(risk_day = any(risk_day)), by = date][order(date)]
rle_risk <- rle(risks_collapsed$risk_day)
spells <- data.table(len = rle_risk$lengths, risk = rle_risk$values)[risk == TRUE, .N, by = len][order(len)]
fwrite(spells, file.path(viz_out, "risk_spells_length_counts.csv"))

# 7-day rolling series (collapsed)
roll7 <- risks_collapsed[, .(date, risk_7d = frollsum(as.integer(risk_day), 7, align="right"))]

# ---- 4) Yearly series (collapse villages) ----
yr_palm <- collapse_by_year(daily, "palm_hours", sum)
yr_work_all <- collapse_by_year(daily, "work_all_pct", mean)
mon_work_day <- collapse_by_month(daily, "work_day_pct", mean)

# ---------- Plot helpers ----------
open_file <- function(path) {
  if (requireNamespace("berryFunctions", quietly = TRUE)) {
    berryFunctions::openFile(path)
  } else {
    utils::browseURL(paste0("file:///", normalizePath(path)))
  }
}
set_plot_theme <- function() {
  par(mar = c(5.2, 6.0, 4.6, 1.6),
      mgp = c(3.1, 1.0, 0),
      tcl = -0.35, las = 1,
      cex.axis = 1.25, cex.lab = 1.5, cex.main = 1.8,
      xaxs = "i", yaxs = "i")
}
draw_hgrid <- function(y) abline(h = pretty(y), col = "gray90", lwd = 1)
draw_vgrid <- function(x) abline(v = pretty(x), col = "gray95", lwd = 1)

# ---- P1) Phytophthora hours per year (averaged across villages) ----
png1 <- file.path(viz_out, "P1_palm_hours_yearlines_mean.png")
png(png1, 1600, 950, res = 150); set_plot_theme()
xl <- range(yr_palm$year); yl <- range(pretty(yr_palm$val, 8))
plot(NA, xlim = xl, ylim = yl, xlab = "Year",
     ylab = "Phytophthora-favorable hours / year",
     main = "Phytophthora hours (village mean)")
draw_hgrid(yl); draw_vgrid(xl)
lines(yr_palm$year, yr_palm$val, lwd = 3, col = "#444C5C"); points(yr_palm$year, yr_palm$val, pch = 16, cex = 1.1)
dev.off(); open_file(png1)

# ---- P2) Workability - all hours (averaged) ----
png2 <- file.path(viz_out, "P2_workability_all_yearlines_mean.png")
png(png2, 1600, 950, res = 150); set_plot_theme()
xl <- range(yr_work_all$year); yl <- range(pretty(yr_work_all$val, 8))
plot(NA, xlim = xl, ylim = yl, xlab = "Year",
     ylab = "Workability (%, all hours)",
     main = "Workability - all hours (village mean)")
draw_hgrid(yl); draw_vgrid(xl)
lines(yr_work_all$year, yr_work_all$val, lwd = 3, col = "#6C757D"); points(yr_work_all$year, yr_work_all$val, pch = 16, cex = 1.1)
dev.off(); open_file(png2)

# ---- P3) Monthly workability (daytime) climatology (averaged) ----
png3 <- file.path(viz_out, "P3_workability_monthly_day_clim_mean.png")
png(png3, 1600, 950, res = 150); set_plot_theme()
xl <- c(1,12); yl <- range(pretty(mon_work_day$val, 8))
plot(NA, xlim = xl, ylim = yl, xaxt = "n",
     xlab = "Month", ylab = "Workability (%, 09-16)",
     main = "Monthly workability - daytime (village mean)")
axis(1, at = 1:12, labels = month.abb, cex.axis = 1.2)
draw_hgrid(yl); abline(v = 1:12, col = "gray97")
lines(mon_work_day$month, mon_work_day$val, lwd = 3, col = "#495057"); points(mon_work_day$month, mon_work_day$val, pch = 16, cex = 1.1)
dev.off(); open_file(png3)

# ---- P4) Seasonal timing (center-of-mass & 30-day peak, averaged) ----
png4 <- file.path(viz_out, "P4_season_timing_mean.png")
png(png4, 1600, 950, res = 150); set_plot_theme()
yl <- c(1,365); xl <- range(seasonal$season_year)
plot(NA, xlim = xl, ylim = yl,
     xlab = "Season-year (Aug???Jul)", ylab = "Day of season (DOY)",
     main = "Timing of Phytophthora season (village mean)")
abline(h = seq(0,360,30), col = "gray92"); draw_vgrid(xl)
points(seasonal$season_year, seasonal$peak30_doy, pch = 16, cex = 1.1, col = "#546A7B")
points(seasonal$season_year, seasonal$com_doy,   pch = 0,  lwd = 2,  col = "#546A7B")
legend("topleft", bty = "n", pch = c(16,0), lwd = c(NA,2),
       col = "#546A7B", legend = c("Peak 30-day", "Center of timing"))
dev.off(); open_file(png4)

# ---- P5) Risk days per month (mean across years, averaged villages) ----
png5 <- file.path(viz_out, "P5_riskdays_per_month_mean.png")
png(png5, 1700, 950, res = 150); set_plot_theme()
yl <- range(pretty(monthly_risk$risk_days, 10))
mid <- barplot(monthly_risk$risk_days, ylim = yl, col = "#94A3B8", border = "gray40",
               names.arg = month.abb, cex.names = 1.2,
               ylab = "Risk days per month (average across years)",
               main = sprintf("Risk days (???%dh/day) - Phytophthora", riskday_min_hours))
abline(h = pretty(yl), col = "gray90"); box()
text(mid, monthly_risk$risk_days + 0.03*diff(yl), labels = round(monthly_risk$risk_days,1), cex = 1.0)
dev.off(); open_file(png5)

# ---- P6) Spells (runs of consecutive risky days) ----
png6 <- file.path(viz_out, "P6_risk_spells_length_hist.png")
png(png6, 1500, 950, res = 150); set_plot_theme()
barplot(spells$N, names.arg = spells$len, col = "#6B9080", border = "gray40",
        xlab = "Spell length (consecutive risk days)",
        ylab = "Count (2000-2022, village-collapsed)",
        main = "Distribution of risk-day spells")
abline(h = pretty(range(spells$N)), col = "gray90"); box()
dev.off(); open_file(png6)

# ---- Save compact CSVs for the figures ----
fwrite(yr_palm,       file.path(viz_out, "T_yearly_palm_hours_mean.csv"))
fwrite(yr_work_all,   file.path(viz_out, "T_yearly_workability_all_mean.csv"))
fwrite(mon_work_day,  file.path(viz_out, "T_monthly_workability_day_clim_mean.csv"))
fwrite(seasonal,      file.path(viz_out, "T_season_timing_mean.csv"))
fwrite(monthly_risk,  file.path(viz_out, "T_riskdays_per_month_mean.csv"))

cat("\n??? Wrote plots & tables to:\n", normalizePath(viz_out), "\n")
