# =============================================================================
# Climate-change diagnostics: 5-year period contrasts and long-term trends
# =============================================================================
#




# =============================================================================
# Climate-change diagnostics (quick guide)
# =============================================================================
#
# What this does:
#   . Compares the most recent 5 years (2018-22) to earlier 5-year blocks
#     (2003-07, 2008-12, 2013-17).
#   . Looks at both temperature / humidity / radiation and WBGT-based metrics.
#   . Checks for long-term trends (1980-2024).
#
# What comes out:
#   . Tables (CSV) with average differences, effect sizes, p-values, and trends.
#   . Figures (PNG) showing:
#       (1) Sen's slope per month (are things warming / drying over decades?).
#       (2) Heatmaps of recent vs baseline changes (see where months stand out).
#       (3) Bar charts with uncertainty bars (95% CI) for VPD changes.
#       (4) Annual time-series lines with regression slopes.
#
# How to read the figures:
#   . Upward bars or red colors ??? hotter, drier, more WBGT risk.
#   . Downward bars or blue colors ??? cooler, more humid, less WBGT risk.
#   . If error bars cross zero ??? difference is not significant.
#   . Long-term lines + slopes show whether trends are persistent or noisy.
#
# Why it matters:
#   . Quick way to see if the "new normal" (2018-22) is really different from
#     past blocks.
#   . Highlights months/seasons most affected by warming or drying.
#   . Helps tell the story: is change steady, accelerating, or seasonal?
#
# Outputs land under: stream_dir/figs_cc
# =============================================================================




# Purpose:
# --------
# This script analyzes daily ERA5-derived climate metrics for Mandena and
# Sarahandrano (schemes: nearest1, idw4). It evaluates recent conditions
# (2018-2022) against three earlier 5-year baseline blocks (2003-07, 2008-12,
# 2013-17), and quantifies long-term linear and non-parametric trends (1980-2024).
#
# Data:
# -----
# Input parquet files: stream_dir/daily_agg/
#   . Daily aggregated fields include: temperature (tmean_c, tmax_c, tmin_c),
#     humidity (rh_mean, vpd_kpa), radiation (ssrd_MJ_day), and WBGT-based risk
#     indicators (wbgt28_h, wbgt31_h, hot35_h).
#
# Output folders:
#   . Tables (CSV): period contrasts and trend statistics
#   . Figures (PNG): heatmaps, bar charts, annual time series
#   . All written under stream_dir/figs_cc/
#
# Comparisons:
# ------------
#   . Period contrasts: mean(2018-22) minus mean(baseline period), with
#     Welch's t-test p-values, Hedges' g effect sizes, and bootstrap CIs.
#   . Trends: ordinary least squares (per decade), Mann-Kendall tau, and
#     Sen's slope estimates, both annually and by calendar month.
#
# Tables produced:
# ----------------
#   . period_contrasts_monthly.csv - ?? (recent vs baselines), p-values,
#     effect sizes, and bootstrap confidence intervals at monthly resolution.
#   . period_contrasts_annual.csv - same, but annual means pooled across months.
#   . trends_monthly_1980_2024.csv - Sen's slope, MK tau, and linear slope by
#     month for 1980-2024.
#   . trends_annual_1980_2024.csv - annual mean trends for 1980-2024.
#
# Figures produced (per scheme × village group):
# ----------------------------------------------
# (1) trend_sen_slope_by_month_<group>.png
#     . Barplot grid: Sen's slope (per decade) by variable and month.
#     . Interpretation: Positive bars = upward trends (warming, more risk hours).
#       Negative bars = declines. Consistency across months indicates pervasive
#       change; variability indicates seasonality of change.
#
# (2) heatmap_delta_<var>_vs_<baseline>_<group>.png
#     . Heatmap: Mean difference (2018-22 - baseline) by month.
#     . Interpretation: Colors show magnitude/direction of change relative to
#       one baseline period. Rows = baseline block, cols = months.
#       Blue = cooler/less risk; red = warmer/more risk.
#
# (3) bars_delta_vpd_vs_baselines_<group>.png
#     . Grouped bars: Monthly change in VPD (2018-22 vs each baseline) with
#       95% bootstrap confidence intervals.
#     . Interpretation: Tall positive bars = recent drying; negative = more
#       humid. Error bars spanning 0 imply non-significant difference.
#
# (4) annual_timeseries_ols_<group>.png
#     . Grid of line plots (one per variable): annual means (1980-2024),
#       with dashed OLS regression line.
#     . Interpretation: Long-term trajectories of key climate metrics.
#       Slopes highlight gradual warming, drying, or radiation shifts.
#
# Usage notes:
# ------------
#   . Adjust `scheme_sel`, `village_sel`, or `vars_state/vars_sum` to expand to
#     additional locations or metrics.
#   . Baseline periods must remain non-overlapping.
#   . Bootstrapping of confidence intervals is block-based by year, preserving
#     intra-annual structure.
#
# Audience:
# ---------
# Intended as a diagnostics tool for collaborators and publication figures.
# Provides both scientific rigor (effect sizes, non-parametric tests) and
# clear visualizations for heat-related climate change narratives.
#
# =============================================================================

library(arrow)
library(data.table)
library(lubridate)
library(ggplot2)

# ---------- CONFIG ----------
daily_dir    <- file.path(stream_dir, "daily_agg")
scheme_sel   <- c("nearest1","idw4")          # choose one or both
village_sel  <- c("Mandena","Sarahandrano")
years_keep   <- 2003:2022                     # covers all 5-yr blocks below
vars_keep    <- c("tmean_c","tmax_c","rh_mean","vpd_kpa",
                  "ssrd_MJ_day","wbgt28_h","wbgt31_h","hot35_h")
# out_dir      <- file.path(stream_dir, "figs_period_diffs")
# dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
# 
# out_dir
# 


# ---- Paths ----
Sys.setenv("ERA5_STREAM"  = "C:/Users/jordan/Desktop/madagascar/stream")
Sys.setenv("figures_dir"  = "C:/Users/jordan/R_Projects/CHI-Data/analysis/madagascar/outputs/figures")
Sys.setenv("tables_dir"   = "C:/Users/jordan/R_Projects/CHI-Data/analysis/madagascar/outputs/tables")

stream_dir  <- Sys.getenv("ERA5_STREAM")
figures_dir <- Sys.getenv("figures_dir")
tables_dir  <- Sys.getenv("tables_dir")





# 5-year periods (labels must not overlap)
periods <- list(
  `2018-2022` = 2018:2022,
  `2013-2017` = 2013:2017,
  `2008-2012` = 2008:2012,
  `2003-2007` = 2003:2007
)
recent_name <- "2018-2022"
baselines   <- setdiff(names(periods), recent_name)

# ---------- LOAD & MONTHLY PER-YEAR AGG ----------
ds <- open_dataset(daily_dir, format = "parquet")
DT <- ds |>
  dplyr::filter(scheme %in% scheme_sel, village %in% village_sel) |>
  dplyr::select(scheme, village, date, dplyr::all_of(vars_keep)) |>
  dplyr::collect()
setDT(DT)
DT[, date := as.Date(date)]
DT[, `:=`(year = year(date), month = month(date))]
DT <- DT[year %in% years_keep]

# monthly totals/means per year (pool days within month-year)
# sums for counts/hours; means for state variables
sum_vars  <- c("ssrd_MJ_day","wbgt28_h","wbgt31_h","hot35_h")
mean_vars <- setdiff(vars_keep, sum_vars)

mon <- DT[, c(
  lapply(.SD[, ..mean_vars], function(x) mean(x, na.rm = TRUE)),
  lapply(.SD[, ..sum_vars],  function(x) sum(x,  na.rm = TRUE))
), by = .(scheme, village, year, month)]
setcolorder(mon, c("scheme","village","year","month", vars_keep))
mon[, month_lab := factor(month.abb[month], levels = month.abb)]

# ---------- HELPERS ----------
welch_t <- function(x, y) {
  # returns p-value; NAs tolerated
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  if (length(x) < 3L || length(y) < 3L) return(NA_real_)
  tryCatch(t.test(x, y, var.equal = FALSE)$p.value, error = function(e) NA_real_)
}

hedges_g <- function(x, y) {
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  nx <- length(x); ny <- length(y)
  if (nx < 3L || ny < 3L) return(NA_real_)
  mx <- mean(x); my <- mean(y)
  sx <- stats::var(x); sy <- stats::var(y)
  sp <- sqrt(((nx-1)*sx + (ny-1)*sy) / (nx+ny-2))
  if (!is.finite(sp) || sp == 0) return(NA_real_)
  d  <- (mx - my) / sp
  J  <- 1 - (3/(4*(nx+ny)-9))  # small-sample correction
  J * d
}

block_boot_ci <- function(x, y, B = 2000L, conf = 0.95) {
  # x = recent (vector), y = baseline (vector); resample YEARS (blocks)
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  nx <- length(x); ny <- length(y)
  if (nx < 3L || ny < 3L) return(c(NA_real_, NA_real_))
  deltas <- numeric(B)
  for (b in seq_len(B)) {
    xb <- sample(x, nx, replace = TRUE)
    yb <- sample(y, ny, replace = TRUE)
    deltas[b] <- mean(xb) - mean(yb)
  }
  alpha <- (1 - conf) / 2
  stats::quantile(deltas, probs = c(alpha, 1 - alpha), names = FALSE, type = 6)
}

# ---------- COMPARISONS ----------
do_compare <- function(df, var, recent_name, base_name) {
  rec_years <- periods[[recent_name]]
  base_years<- periods[[base_name]]
  xr <- df[year %in% rec_years, get(var)]
  xb <- df[year %in% base_years, get(var)]
  mu_r <- mean(xr, na.rm = TRUE)
  mu_b <- mean(xb, na.rm = TRUE)
  delta <- mu_r - mu_b
  pct   <- if (isTRUE(all.equal(mu_b, 0))) NA_real_ else 100 * delta / mu_b
  pval  <- welch_t(xr, xb)
  g     <- hedges_g(xr, xb)
  ci    <- block_boot_ci(xr, xb, B = 2000L)
  data.table(
    period_recent = recent_name,
    period_base   = base_name,
    variable      = var,
    mean_recent   = mu_r,
    mean_base     = mu_b,
    delta         = delta,
    pct_change    = pct,
    hedges_g      = g,
    p_welch       = pval,
    ci_low        = ci[1],
    ci_high       = ci[2],
    n_recent      = sum(!is.na(xr)),
    n_base        = sum(!is.na(xb))
  )
}

# run comparisons by scheme × village × month × variable (monthly, year-level)
res_monthly <- mon[
  , {
    out_list <- lapply(baselines, function(bn) {
      rbindlist(lapply(vars_keep, function(v)
        do_compare(.SD, v, recent_name, bn)))
    })
    rbindlist(out_list)
  },
  by = .(scheme, village, month, month_lab)
]

# Also compute ANNUAL (pooled over months) comparisons
ann <- mon[, lapply(.SD, mean, na.rm = TRUE),
           by = .(scheme, village, year), .SDcols = vars_keep]

res_annual <- ann[
  , {
    out_list <- lapply(baselines, function(bn) {
      rbindlist(lapply(vars_keep, function(v)
        do_compare(.SD, v, recent_name, bn)))
    })
    rbindlist(out_list)
  },
  by = .(scheme, village)
]

# ---------- EXPORT TABLES ----------
fwrite(res_monthly,
       file.path(tables_dir, "period_diffs_monthly_2018-22_vs_blocks.csv"))
fwrite(res_annual,
       file.path(tables_dir, "period_diffs_annual_2018-22_vs_blocks.csv"))


# ---------- FIGURES (BASE R ONLY) ----------

open_file <- function(path) {
  if (requireNamespace("berryFunctions", quietly = TRUE)) {
    berryFunctions::openFile(path)
  } else {
    utils::browseURL(paste0("file:///", normalizePath(path)))
  }
}
draw_hgrid <- function(y) abline(h = pretty(y), col = "gray90", lwd = 1)

# (1) Heatmap of ?? for a chosen variable by month, per group (one PNG per group)
var_show <- "tmean_c"
hm <- res_monthly[variable == var_show]
hm[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(hm$grp)
cols <- colorRampPalette(c("#2166AC", "#67A9CF", "#F7F7F7", "#F4A582", "#B2182B"))(101)

for (g in groups) {
  h <- hm[grp == g]
  # rows = baselines (keep original order), cols = months 1..12
  bls <- unique(h$period_base)
  M <- matrix(NA_real_, nrow = length(bls), ncol = 12,
              dimnames = list(bls, month.abb))
  for (i in seq_along(bls)) {
    row <- h[period_base == bls[i]]
    M[i, as.integer(row$month)] <- row$delta
  }
  # color scale
  z <- M
  rng <- range(z, na.rm = TRUE); brks <- seq(rng[1], rng[2], length.out = 102)
  
  fpath <- file.path(figures_dir, paste0("heatmap_delta_", var_show, "_", g, ".png"))
  png(fpath, 1400, 800, res = 150)
  par(mar = c(6, 6, 5, 6))
  image(1:12, 1:nrow(M), t(M[nrow(M):1, ]), axes = FALSE,
        xlab = "Month", ylab = "Baseline period",
        main = paste0("?? ", var_show, " (", recent_name, " ??? baseline)\n", g),
        col = cols, breaks = brks, useRaster = TRUE)
  axis(1, at = 1:12, labels = month.abb)
  axis(2, at = 1:nrow(M), labels = rev(rownames(M)), las = 1)
  # color legend
  par(xpd = NA)
  zseq <- seq(rng[1], rng[2], length.out = 101)
  rect(13.2, 1, 13.6, nrow(M), col = cols, border = NA)
  axis(4, at = c(1, nrow(M)), labels = round(c(rng[2], rng[1]), 2), las = 1)
  mtext("?? (units)", side = 4, line = 3)
  box()
  dev.off(); open_file(fpath)
}

# (2) Bars with 95% CI by month for each baseline (example: VPD)
var_show2 <- "rh_mean"
bars <- res_monthly[variable == var_show2]
bars[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(bars$grp)
baseline_levels <- unique(bars$period_base)
col_map <- setNames(c("#1B9E77", "#D95F02", "#7570B3"), baseline_levels)

for (g in groups) {
  b <- bars[grp == g][order(month)]
  fpath <- file.path(figures_dir, paste0("bars_delta_", var_show2, "_", g, ".png"))
  png(fpath, 1700, 950, res = 150)
  par(mar = c(6, 6, 5, 1))
  # matrix of deltas: rows=baselines, cols=months
  M <- sapply(1:12, function(m) {
    x <- b[month == m]
    setNames(x$delta, x$period_base)[baseline_levels]
  })
  rownames(M) <- baseline_levels
  mids <- barplot(M, beside = TRUE, col = col_map[rownames(M)], border = "gray40",
                  ylim = range(pretty(range(M, na.rm = TRUE))), xlab = "", ylab = "",
                  main = paste0("?? ", var_show2, " (", recent_name, " vs baselines)\n", g),
                  names.arg = month.abb, cex.names = 1.1)
  abline(h = 0, col = "gray40", lty = 3)
  draw_hgrid(range(M, na.rm = TRUE))
  mtext("Month", side = 1, line = 4)
  mtext("?? (recent ??? baseline)", side = 2, line = 4)
  # error bars
  for (j in seq_len(ncol(M))) {
    xj <- b[month == j]
    xj <- xj[match(baseline_levels, xj$period_base)]
    ciL <- xj$ci_low; ciU <- xj$ci_high
    arrows(x0 = mids[, j], y0 = ciL, x1 = mids[, j], y1 = ciU, angle = 90, code = 3, length = 0.04)
  }
  legend("topleft", fill = col_map, bty = "n", title = "Baseline",
         legend = names(col_map))
  dev.off(); open_file(fpath)
}

##########################################################################################
##########################################################################################
##########################################################################################

##########################################################################################
##########################################################################################
##########################################################################################

# =============================================================================
# Climate-change diagnostics: 5-yr period contrasts + long-term trends
# Inputs: stream_dir/daily_agg (partitioned by scheme/village/year)
# Outputs: CSV tables + PNG figures under stream_dir/figs_cc
# =============================================================================
library(arrow)
library(data.table)
library(lubridate)
library(ggplot2)

if (!requireNamespace("trend", quietly = TRUE)) install.packages("trend")
library(trend)

# ---------- CONFIG ----------
daily_dir   <- file.path(stream_dir, "daily_agg")
# out_dir     <- file.path(stream_dir, "figs_cc"); dir.create(out_dir, TRUE, FALSE)

# variables available in daily_agg; adjust if you've added more
vars_state  <- c("tmean_c","tmax_c","tmin_c","rh_mean","vpd_kpa")  # averaged within month
vars_sum    <- c("ssrd_MJ_day","wbgt28_h","wbgt31_h","hot35_h")     # summed within month
vars_all    <- c(vars_state, vars_sum)

years_full  <- 1980:2024
periods     <- list(
  `2018-2022` = 2018:2022,
  `2013-2017` = 2013:2017,
  `2008-2012` = 2008:2012,
  `2003-2007` = 2003:2007
)
recent_name <- "2018-2022"
baseline_names <- setdiff(names(periods), recent_name)

scheme_sel  <- c("nearest1","idw4")
village_sel <- c("Mandena","Sarahandrano")

# ---------- LOAD + BUILD MONTHLY / ANNUAL SERIES ----------
ds <- open_dataset(daily_dir, format = "parquet")
DT <- ds |>
  dplyr::filter(scheme %in% scheme_sel, village %in% village_sel) |>
  dplyr::select(scheme, village, date, dplyr::all_of(vars_all)) |>
  dplyr::collect()

setDT(DT)
DT[, date := as.Date(date)]
DT[, `:=`(year = year(date), month = month(date))]
DT <- DT[year %in% years_full]

# Monthly totals/means per year-month
mon <- DT[, c(
  lapply(.SD[, ..vars_state], function(x) mean(x, na.rm = TRUE)),
  lapply(.SD[, ..vars_sum],   function(x) sum(x,  na.rm = TRUE))
), by = .(scheme, village, year, month)]
mon[, month_lab := factor(month.abb[month], levels = month.abb)]

# Annual means (pool months within year)
ann <- mon[, lapply(.SD, mean, na.rm = TRUE),
           by = .(scheme, village, year), .SDcols = vars_all]

# ---------- HELPERS ----------
welch_p <- function(x, y) {
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  if (length(x) < 3L || length(y) < 3L) return(NA_real_)
  tryCatch(t.test(x, y, var.equal = FALSE)$p.value, error = function(e) NA_real_)
}
hedges_g <- function(x, y) {
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  nx <- length(x); ny <- length(y)
  if (nx < 3L || ny < 3L) return(NA_real_)
  sp <- sqrt(((nx-1)*var(x) + (ny-1)*var(y)) / (nx+ny-2))
  if (!is.finite(sp) || sp == 0) return(NA_real_)
  J  <- 1 - (3/(4*(nx+ny)-9))
  J * ((mean(x) - mean(y)) / sp)
}
boot_ci <- function(x, y, B = 2000L, conf = 0.95) {
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  if (length(x) < 3L || length(y) < 3L) return(c(NA_real_, NA_real_))
  deltas <- replicate(B, mean(sample(x, length(x), TRUE)) -
                        mean(sample(y, length(y), TRUE)))
  qs <- c((1-conf)/2, 1-(1-conf)/2)
  as.numeric(quantile(deltas, qs, names = FALSE, type = 6))
}
trend_stats <- function(df, yvar = "val", xvar = "year") {
  df <- df[is.finite(get(yvar))]
  if (nrow(df) < 5) {
    return(data.table(slope_decade_lm = NA_real_, p_lm = NA_real_,
                      tau_mk = NA_real_, p_mk = NA_real_,
                      sens_slope_decade = NA_real_))
  }
  fit <- lm(get(yvar) ~ get(xvar), data = df)
  s_lm <- unname(coef(fit)[2]) * 10
  p_lm <- summary(fit)$coefficients[2,4]
  mk   <- try(mk.test(df[[yvar]]), silent = TRUE)
  se   <- try(sens.slope(df[[yvar]] ~ df[[xvar]]), silent = TRUE)
  data.table(
    slope_decade_lm     = s_lm,
    p_lm                = p_lm,
    tau_mk              = if (inherits(mk,"try-error")) NA_real_ else unname(mk$estimates[["tau"]]),
    p_mk                = if (inherits(mk,"try-error")) NA_real_ else mk$p.value,
    sens_slope_decade   = if (inherits(se,"try-error")) NA_real_ else unname(se$estimates[["slope"]]) * 10
  )
}

# ---------- PERIOD CONTRASTS: 2018-22 vs prior 5-yr blocks ----------
compare_block <- function(df, var, recent_lbl, base_lbl) {
  xr <- df[year %in% periods[[recent_lbl]], get(var)]
  xb <- df[year %in% periods[[base_lbl]],   get(var)]
  mu_r <- mean(xr, na.rm = TRUE); mu_b <- mean(xb, na.rm = TRUE)
  delta <- mu_r - mu_b
  pct   <- if (isTRUE(all.equal(mu_b, 0))) NA_real_ else 100 * delta / mu_b
  ci    <- boot_ci(xr, xb, B = 2000L)
  data.table(
    period_recent = recent_lbl, period_base = base_lbl, variable = var,
    mean_recent = mu_r, mean_base = mu_b, delta = delta, pct_change = pct,
    p_welch = welch_p(xr, xb), hedges_g = hedges_g(xr, xb),
    ci_low = ci[1], ci_high = ci[2],
    n_recent = sum(is.finite(xr)), n_base = sum(is.finite(xb))
  )
}

# Monthly contrasts (by scheme × village × month)
res_monthly <- mon[
  , {
    rbindlist(lapply(baseline_names, function(bn) {
      rbindlist(lapply(vars_all, function(v) compare_block(.SD, v, recent_name, bn)))
    }))
  }, by = .(scheme, village, month, month_lab)
]
fwrite(res_monthly, file.path(tables_dir, "period_contrasts_monthly.csv"))

# Annual contrasts (pooled months per year, then compare)
res_annual <- ann[
  , {
    rbindlist(lapply(baseline_names, function(bn) {
      rbindlist(lapply(vars_all, function(v) compare_block(.SD, v, recent_name, bn)))
    }))
  }, by = .(scheme, village)
]
fwrite(res_annual, file.path(tables_dir, "period_contrasts_annual.csv"))

# ---------- LONG-TERM TRENDS (1980-2024) ----------
# Monthly trend per month (separate slope by calendar month)
trend_monthly <- mon[
  , {
    rbindlist(lapply(vars_all, function(v) {
      df <- data.table(year = year, val = get(v))
      cbind(variable = v, trend_stats(df))
    }))
  }, by = .(scheme, village, month, month_lab)
]
fwrite(trend_monthly, file.path(tables_dir, "trends_monthly_1980_2024.csv"))

# Annual trend (yearly means)
trend_annual <- ann[
  , {
    rbindlist(lapply(vars_all, function(v) {
      df <- data.table(year = year, val = get(v))
      cbind(variable = v, trend_stats(df))
    }))
  }, by = .(scheme, village)
]
fwrite(trend_annual, file.path(tables_dir, "trends_annual_1980_2024.csv"))


# ---------- FIGURES (BASE R ONLY) ----------

open_file <- function(path) {
  if (requireNamespace("berryFunctions", quietly = TRUE)) {
    berryFunctions::openFile(path)
  } else {
    utils::browseURL(paste0("file:///", normalizePath(path)))
  }
}
draw_hgrid <- function(y) abline(h = pretty(y), col = "gray90", lwd = 1)

# 1) Bars: Sen's slope by month for each variable (one PNG per group)
tm <- copy(trend_monthly)
tm[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(tm$grp)
var_levels <- vars_all
# 
# for (g in groups) {
#   d <- tm[grp == g]
#   # Build matrix: rows = variables, cols = months
#   M <- sapply(1:12, function(m) {
#     x <- d[month == m]
#     vals <- setNames(x$sens_slope_decade, x$variable)[var_levels]
#     vals
#   })
#   rownames(M) <- var_levels
#   
#   # Skip if everything is NA
#   if (all(!is.finite(M))) {
#     message("Skipping group ", g, ": all slopes NA")
#     next
#   }
#   
#   fpath <- file.path(figures_dir, paste0("trend_sen_slope_by_month_", g, ".png"))
#   png(fpath, 1800, 950, res = 150)
#   par(mar = c(6, 6, 5, 1))
#   
#   mids <- barplot(M,
#                   beside = TRUE,
#                   col = rep_len(c("#1B9E77","#D95F02","#7570B3","#757575",
#                                   "#A6CEE3","#FB9A99","#CAB2D6","#B2DF8A"),
#                                 nrow(M)),
#                   border = "gray40",
#                   names.arg = month.abb, cex.names = 1.0,
#                   main = paste0("Long-term trend by month (Sen's slope per decade)\n", g),
#                   ylab = "Slope / decade")
#   abline(h = 0, col = "gray40", lty = 3)
#   draw_hgrid(range(M, na.rm = TRUE))
#   
#   legend("topleft", bty = "n",
#          fill = rep_len(c("#1B9E77","#D95F02","#7570B3","#757575",
#                           "#A6CEE3","#FB9A99","#CAB2D6","#B2DF8A"),
#                         nrow(M)),
#          legend = rownames(M))
#   
#   dev.off(); open_file(fpath)
# }
# 


# 2) Heatmap: ??(2018-22 minus baseline) for a chosen baseline & variable (per group)
show_baseline <- baseline_names[1]
show_var      <- "tmean_c"
hm <- res_monthly[variable == show_var & period_base == show_baseline]
hm[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(hm$grp)
cols <- colorRampPalette(c("#2166AC", "#67A9CF", "#F7F7F7", "#F4A582", "#B2182B"))(101)

for (g in groups) {
  h <- hm[grp == g]
  z <- rep(NA_real_, 12); z[h$month] <- h$delta
  rng <- range(z, na.rm = TRUE); brks <- seq(rng[1], rng[2], length.out = 102)
  fpath <- file.path(figures_dir, paste0("heatmap_delta_", show_var, "_vs_", show_baseline, "_", g, ".png"))
  png(fpath, 1400, 500, res = 150)
  par(mar = c(6, 6, 5, 6))
  # image(1:12, 1, matrix(z, nrow = 1), axes = FALSE,
  #       xlab = "Month", ylab = "", main = paste0("?? ", show_var,
  #                                                " (2018-22 - ", show_baseline, ")\n", g),
  #       col = cols, breaks = brks, useRaster = TRUE)
  
  image(1:12, 1, matrix(z, ncol = 1), axes = FALSE,
        xlab = "Month", ylab = "", 
        main = paste0("?? ", show_var, " (2018-22 - ", show_baseline, ")\n", g),
        col = cols, breaks = brks, useRaster = TRUE)
  
  axis(1, at = 1:12, labels = month.abb)
  mtext("?? (units)", side = 4, line = 3)
  # color legend
  rect(13.2, 1, 13.6, 1.9, col = cols, border = NA)
  axis(4, at = c(1.05, 1.85), labels = round(c(rng[1], rng[2]), 2), las = 1)
  box()
  dev.off(); open_file(fpath)
}

# 3) Bars with 95% CI: ?? by month for VPD across baselines (per group)
bars <- res_monthly[variable == "vpd_kpa"]
bars[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(bars$grp)
baseline_levels <- unique(bars$period_base)
col_map <- setNames(c("#1B9E77", "#D95F02", "#7570B3"), baseline_levels)

for (g in groups) {
  b <- bars[grp == g][order(month)]
  fpath <- file.path(figures_dir, paste0("bars_delta_vpd_vs_baselines_", g, ".png"))
  png(fpath, 1700, 950, res = 150)
  par(mar = c(6, 6, 5, 1))
  # matrix of deltas: rows=baselines, cols=months
  M <- sapply(1:12, function(m) {
    x <- b[month == m]
    setNames(x$delta, x$period_base)[baseline_levels]
  })
  rownames(M) <- baseline_levels
  mids <- barplot(M, beside = TRUE, col = col_map[rownames(M)], border = "gray40",
                  ylim = range(pretty(range(M, na.rm = TRUE))), xlab = "", ylab = "",
                  main = paste0("Change in VPD (2018-22 vs baselines) with 95% CI\n", g),
                  names.arg = month.abb, cex.names = 1.1)
  abline(h = 0, col = "gray40", lty = 3); draw_hgrid(range(M, na.rm = TRUE))
  mtext("Month", side = 1, line = 4)
  mtext("?? VPD (kPa)", side = 2, line = 4)
  # error bars from CI
  for (j in seq_len(ncol(M))) {
    xj <- b[month == j]
    xj <- xj[match(baseline_levels, xj$period_base)]
    ciL <- xj$ci_low; ciU <- xj$ci_high
    arrows(x0 = mids[, j], y0 = ciL, x1 = mids[, j], y1 = ciU, angle = 90, code = 3, length = 0.04)
  }
  legend("topleft", fill = col_map, bty = "n", title = "Baseline",
         legend = names(col_map))
  dev.off(); open_file(fpath)
}

# 4) Annual time series by variable with OLS line (one PNG per group; 2×4 grid)
ann_long <- melt(ann, id.vars = c("scheme","village","year"),
                 measure.vars = vars_all, variable.name = "variable", value.name = "val")
ann_long[, grp := paste(scheme, village, sep = " . ")]
groups <- unique(ann_long$grp)

for (g in groups) {
  d <- ann_long[grp == g]
  fpath <- file.path(figures_dir, paste0("annual_timeseries_ols_", g, ".png"))
  png(fpath, 1800, 1200, res = 150)
  par(mfrow = c(4, 2), mar = c(4.5, 5.0, 3.5, 1.5))
  for (v in vars_all) {
    dv <- d[variable == v][order(year)]
    yl <- range(pretty(dv$val, 8))
    plot(dv$year, dv$val, type = "l", lwd = 2, col = "#444C5C",
         xlab = "Year", ylab = v, main = paste(v, "-", g), ylim = yl)
    draw_hgrid(yl)
    # OLS line
    if (sum(is.finite(dv$val)) >= 3) {
      fit <- lm(val ~ year, data = dv)
      abline(fit, col = "#B2182B", lwd = 2, lty = 2)
    }
  }
  dev.off(); open_file(fpath)
}



# 
# 
