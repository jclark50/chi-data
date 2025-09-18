# helpers.R  (add at top)

# ---- Portable paths ----
safeload <- function(pkg) { if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg); TRUE }
safeload("here"); safeload("fs")

data_root <- function() {
  dr <- Sys.getenv("CHI_DATA_ROOT", unset = NA_character_)
  if (is.na(dr) || dr == "") stop("CHI_DATA_ROOT is not set. Add it to .Renviron.", call. = FALSE)
  fs::path_norm(dr)
}

# Build paths from CHI_DATA_ROOT and/or project root
path_data   <- function(...) fs::path(data_root(), ...)
path_proj   <- function(...) fs::path(here::here(), ...)
dir_ensure  <- function(...) { fs::dir_create(fs::path(...)); fs::path(...) }

# ---- Project-specific convenience ----
# ERA5 root on disk (under CHI_DATA_ROOT)
era5_root <- function() path_data("gridded", "era5-africa", "processed")

# Central location for small "publishable" outputs (in-repo)
outputs_root <- function() path_proj("analytics", "madagascar", "outputs")

# Optional streaming directory (also under CHI_DATA_ROOT if it's large)
stream_root <- function() path_data("gridded", "era5land", "madagascar", "madagascar_hourly_daily_streamed")


