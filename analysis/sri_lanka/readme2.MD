Got it — here’s both pieces you asked for:

---

## 1) **README for GitHub**

````markdown
# CHI-Data

This repository hosts analysis scripts and outputs for the Climate and Health Initiative (CHI).  
Projects currently include Madagascar and Sri Lanka.

## Getting Started

Clone the repository to your local machine:

```bash
git clone https://github.com/[your-org]/CHI-Data.git
cd CHI-Data
````

## Running Analyses

### Sri Lanka

* **Step 1**: Run `analysis/sri_lanka/initial_processing.R`
  → Parses epidemiological reports, compiles climate datasets, and produces standardized weekly district-level files.
* **Step 2**: Run `analysis/sri_lanka/post_processing.R`
  → Links processed climate data with health outcomes, generates exploratory plots and tables.

⚡ If you want to *immediately* explore data and outputs without the full preprocessing, simply start from:

```r
source("analysis/sri_lanka/post_processing.R")
```

### Madagascar

* Scripts under `analysis/madagascar/` process ERA5 climate data and generate workability/disease-related figures.
* Outputs are written to `analysis/madagascar/outputs/figures/` and `analysis/madagascar/outputs/tables/`.

## Repository Structure

```
CHI-Data/
│
├── analysis/
│   ├── madagascar/        # Climate workability & crop disease analyses
│   └── sri_lanka/         # Climate-health epidemiology linkage
│
├── data/                  # (Optional) Shared datasets
├── helpers/               # Shared R helper functions
├── docs/                  # Background literature and project documents
├── outputs/               # Central outputs (per project)
└── README.md              # This file
```

```

---

## 2) **Formal Write-Up (1–2 pages)**

### Linking Hourly Climate Data with Weekly Health Data: Challenges and Approaches

The integration of climate and health datasets is central to understanding how environmental exposures shape population-level disease dynamics. Yet the task is far from straightforward. Climate data are typically available at high temporal resolution (hourly, daily) and moderate spatial scales (grid cells ~10–30 km), while health data are most often aggregated to coarser temporal (weekly or monthly) and spatial (district, province) units. This temporal and spatial misalignment introduces methodological and interpretive challenges that must be navigated carefully.

**Challenges:**
1. **Temporal aggregation mismatch**  
   - Climate drivers (temperature, humidity, rainfall, solar radiation) operate on hourly to daily scales, influencing pathogen growth cycles or human exposures within narrow windows.  
   - Health outcomes (e.g., leptospirosis or dengue cases) are reported weekly, obscuring exact exposure timing. This complicates lagged analyses and may dilute signals of acute events such as heatwaves or heavy rainfall.

2. **Spatial scale disparities**  
   - Reanalysis products like ERA5 report climate variables at fixed grid resolutions. These may not align neatly with district boundaries used in health data.  
   - In Sri Lanka, districts often span multiple climate zones, requiring methods like inverse distance weighting or area-weighted averaging across grid cells.

3. **Lag structures and incubation periods**  
   - Different diseases respond to climate drivers over varying lags. For example, heavy rainfall may influence leptospirosis cases within 1–2 weeks, while vector-borne diseases like dengue may reflect climate conditions over several weeks. Selecting biologically plausible lag structures is critical but uncertain.

4. **Noise and reporting bias in health data**  
   - Weekly counts may mask underreporting, delays, or sudden backlogs. When combined with precise climate metrics, this can create the illusion of weak associations even when strong causal mechanisms exist.

5. **Non-linear and interacting effects**  
   - Climate exposures rarely act alone. Heat and humidity interact, rainfall can both increase and flush vector breeding sites, and solar radiation influences pathogen viability. Capturing these complexities requires flexible models.

**Approach Taken:**
- **Standardized climate summaries**: Hourly ERA5 data were aggregated to weekly means, maxima, or totals (e.g., weekly average temperature, cumulative rainfall). These were aligned with the epidemiological week calendar.  
- **Area-weighted climate exposures**: For each district, climate variables were computed using either the nearest grid cell (“nearest cell”) or weighted averages of neighboring cells (IDW or areal weighting), ensuring robust representation across diverse geographies.  
- **Lag exploration**: Multiple lagged climate variables (e.g., 1–4 weeks prior) were constructed to test associations across plausible biological windows.  
- **Sensitivity analyses**: Both simple linear models and more flexible approaches (e.g., GAMs, GBMs) were considered to explore non-linear responses and interactions.  
- **Transparency of uncertainty**: Rather than forcing a single “best” alignment, the project documented the spectrum of methods and assumptions, highlighting how methodological choices affect results.

**Why This Matters:**  
By explicitly grappling with temporal misalignment, spatial aggregation, and biological lag structures, this work illustrates the core methodological challenges of climate–health linkage. For newcomers to the field, it highlights that there is rarely a single “correct” way to link datasets. Instead, robust analyses require:  
- Testing multiple approaches,  
- Documenting assumptions, and  
- Being transparent about the limits imposed by data resolution.  

The Sri Lanka case study provides an instructive example. Starting from high-resolution climate inputs and aligning them to weekly health outcomes, the project demonstrates both the potential insights and the pitfalls. It underscores the importance of continued investment in **climate-health observatories** that can generate harmonized, high-resolution datasets across both domains — a central goal of the DGHI Climate and Health Initiative.

---

Would you like me to polish the **formal write-up** further into a journal-style **background + methods** section (with citations) so it could serve as a ready manuscript excerpt, or keep it more as an internal training note?
```
