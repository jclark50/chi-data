# =============================================================================
# Map the ERA5 pixels used by "nearest1" vs "idw4"
# Inputs:
#   - weights_dt: data.table with columns
#       scheme ??? {"nearest1","idw4"}, village, lat_idx_x4, lon_idx_x4, weight
#   - villages : data.table with columns village, lon, lat (WGS84)
# Output: a two-panel PNG and an sf object of polygons (for reuse/export)
# =============================================================================
suppressPackageStartupMessages({
  library(data.table)
  library(sf)
})

# --- 0) Helpers ---------------------------------------------------------------
idx_to_center <- function(i) i / 4            # 0.25° grid => center at idx/4
half_cell <- 0.25 / 2                         # 0.125° half-width

cell_poly <- function(lon_c, lat_c) {
  # returns an st_polygon with WGS84 lon/lat corners around the center
  x1 <- lon_c - half_cell; x2 <- lon_c + half_cell
  y1 <- lat_c - half_cell; y2 <- lat_c + half_cell
  m <- matrix(
    c(x1,y1,  x2,y1,  x2,y2,  x1,y2,  x1,y1),
    ncol = 2, byrow = TRUE
  )
  st_polygon(list(m))
}

# --- 1) Build polygons from weights_dt ---------------------------------------
# (Assumes weights_dt exists; if not, create via make_weights_centers(villages))
stopifnot(all(c("scheme","village","lat_idx_x4","lon_idx_x4","weight") %in% names(weights_dt)))

wdt <- as.data.table(weights_dt)
wdt[, `:=`(
  lat_c = idx_to_center(lat_idx_x4),
  lon_c = idx_to_center(lon_idx_x4)
)]

# one polygon per (scheme, village, cell); keep weight for idw shading
cells <- unique(wdt[, .(scheme, village, lat_idx_x4, lon_idx_x4, lon_c, lat_c, weight)])

# build sf geometry
geom_list <- vector("list", nrow(cells))
for (i in seq_len(nrow(cells))) {
  geom_list[[i]] <- cell_poly(cells$lon_c[i], cells$lat_c[i])
}
cells_sf <- st_sf(cells, geometry = st_sfc(geom_list, crs = 4326))

# villages -> sf points (optional; assumed you already have this data.table)
# villages must have columns: village, lon, lat
if (exists("villages")) {
  villages_sf <- st_as_sf(villages, coords = c("lon","lat"), crs = 4326)
} else {
  villages_sf <- NULL
}

# --- 2) Optional: Madagascar outline (offline if package installed) ----------
mdg_sf <- NULL
if (requireNamespace("rnaturalearth", quietly = TRUE)) {
  mdg_sf <- try({
    rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
  }, silent = TRUE)
  if (!inherits(mdg_sf, "try-error")) {
    mdg_sf <- mdg_sf[mdg_sf$admin == "Madagascar", ]
    mdg_sf <- st_transform(mdg_sf, 4326)
  } else mdg_sf <- NULL
}

# --- 3) Plot (base R; two panels: nearest1 and idw4) -------------------------
# derive plotting extent from used cells (pad a bit)
bb <- st_bbox(cells_sf)
pad <- 0.5
xlim <- c(bb["xmin"] - pad, bb["xmax"] + pad)
ylim <- c(bb["ymin"] - pad, bb["ymax"] + pad)

# simple color scale for idw weights
w_col <- function(w) {
  # darker = higher weight
  pal <- colorRampPalette(c("#E3F2FD", "#90CAF9", "#1565C0"))
  cuts <- cut(w, breaks = seq(0, 1, by = 0.1), include.lowest = TRUE)
  pal(10)[as.integer(cuts)]
}


########################################################
########################################################
########################################################

png_map <- file.path(figures_dir, "map_era5_cells_nearest_vs_idw.png")
png(png_map, width = 1600, height = 900, res = 150)
par(mfrow = c(1, 2), mar = c(4, 4, 4, 2))

# --- Panel 1: nearest1 (single cell per village) ---
plot(NA, xlim = xlim, ylim = ylim, xlab = "Longitude", ylab = "Latitude",
     main = "Pixels used - nearest1", asp = 1)
if (!is.null(mdg_sf)) plot(st_geometry(mdg_sf), add = TRUE, col = NA, border = "gray70")

near_sf <- cells_sf[cells_sf$scheme == "nearest1", ]
# outline polygons
plot(st_geometry(near_sf), add = TRUE, col = "#FFFDE7", border = "#BDB76B", lwd = 2)
# village points + labels
if (!is.null(villages_sf)) {
  plot(st_geometry(villages_sf), add = TRUE, pch = 21, bg = "tomato", cex = 1.2)
  text(st_coordinates(villages_sf), labels = villages_sf$village, pos = 3, cex = 0.9)
}
box()

# --- Panel 2: idw4 (up to 4 cells per village; shade by weight) ---
plot(NA, xlim = xlim, ylim = ylim, xlab = "Longitude", ylab = "Latitude",
     main = "Pixels used - idw4 (fill = weight)", asp = 1)
if (!is.null(mdg_sf)) plot(st_geometry(mdg_sf), add = TRUE, col = NA, border = "gray70")

idw_sf <- cells_sf[cells_sf$scheme == "idw4", ]
# plot filled by weight, bordered for visibility
if (nrow(idw_sf)) {
  col_vec <- w_col(idw_sf$weight)
  plot(st_geometry(idw_sf), add = TRUE, col = col_vec, border = "gray25", lwd = 1.2)
  # per-village outlines to show the 3-4-cell set
  vnames <- unique(idw_sf$village)
  for (vn in vnames) {
    vsub <- st_union(idw_sf[idw_sf$village == vn, ])
    plot(st_geometry(vsub), add = TRUE, border = "black", lwd = 2, lty = 3)
  }
}
# village points + labels
if (!is.null(villages_sf)) {
  plot(st_geometry(villages_sf), add = TRUE, pch = 21, bg = "tomato", cex = 1.2)
  text(st_coordinates(villages_sf), labels = villages_sf$village, pos = 3, cex = 0.9)
}
# legend for weight
legend("topleft", title = "IDW weight", bty = "n",
       fill = colorRampPalette(c("#E3F2FD", "#90CAF9", "#1565C0"))(5),
       legend = sprintf("%.1f-%.1f", seq(0.0, 0.8, by = 0.2), seq(0.2, 1.0, by = 0.2)))
box()

dev.off(); open_file(png_map)

