# CHI-Data

**Climate & Health Initiative (CHI) Data Repository**  
This repository provides a shared structure for storing, processing, and analyzing climate and health datasets used across CHI projects. It contains reproducible pipelines, helper functions, and project-specific analyses (e.g., Madagascar, Sri Lanka), each with their own outputs (tables, figures).

---

## Repository Goals
- **Centralize**: Keep climate and health data processing code, helper functions, and outputs in one place.
- **Reproducibility**: Ensure analyses can be rerun from raw inputs to final figures/tables.
- **Transparency**: Document data sources, processing steps, and outputs for collaborators.
- **Scalability**: Support multiple projects under the same structure (new country/region analyses can be slotted in easily).

---

## Repository Structure

```

CHI-Data/
│
├── analysis/                 # Project-specific scripts & outputs
│   ├── madagascar/           # Madagascar workability & disease analyses
│   │   ├── era5\_madagascar.R # Data processing script
│   │   ├── plots.R           # Scripts to generate outputs
│   │   └── outputs/          # Results specific to Madagascar
│   │       ├── figures/      # PNGs, PDFs, visualizations
│   │       └── tables/       # CSVs, summary tables
│   │
│   └── srilanka/             # Sri Lanka analyses (in progress / placeholder)
│       └── outputs/          # (same structure as above)
│           ├── figures/
│           └── tables/
│
├── helpers/                  # Shared utility functions
│   ├── helpers.R             # Data wrangling, transformations
│   └── plot\_helpers.R        # Consistent plotting utilities
│
├── pipelines/                # Automated ingestion/ETL pipelines
│   ├── extract.py
│   └── pipeline.yaml
│
├── analytics/                # Exploratory notebooks (R, Python, Jupyter)
│   └── madagascar/
│
├── CHI-Data.Rproj            # RStudio project file
├── DESCRIPTION.txt           # Project metadata
└── README.md                 # This file

````

---

## Workflow

1. **Data ingestion**  
   - Raw ERA5, ERA5-Land, and related datasets are stored outside GitHub (Google Drive, S3, etc.).  
   - Pipelines in `/pipelines/` handle downloading and preprocessing.  

2. **Shared helpers**  
   - Common functions for data wrangling, plotting, and transformations are in `/helpers/`.  

3. **Project analyses**  
   - Each project (e.g., `/analysis/madagascar/`) has scripts for processing and analysis.  
   - Outputs are automatically written to `/analysis/<project>/outputs/`, separated into:
     - `figures/` — all generated plots, maps, and diagnostics  
     - `tables/` — all derived CSVs and summary statistics  

4. **Exploration**  
   - Notebooks under `/analytics/` are used for exploratory analysis, quick checks, and prototypes.  

---

## Conventions

- **File naming**  
  - Plots: `P_<number>_<description>.png`  
  - Tables: `T_<description>.csv`  

- **Environment variables**  
  - Use `.Renviron` to set project-specific data paths, e.g.:  
    ```
    ERA5_MADAGASCAR=C:/path/to/era5_madagascar
    ```

- **Code style**  
  - R scripts primarily use `data.table` for performance.  
  - Functions that may be reused belong in `/helpers/`.  

---

## Current Projects
- **Madagascar**: Workability metrics, crop disease (Phytophthora), climate comparisons (ERA5, ERA5-Land).  
- **Sri Lanka**: [placeholder / planned].

---

## License
[MIT License](LICENSE) unless otherwise noted.  
Data usage may be restricted by source—see project-level READMEs.

---

## Contact
For questions, open an issue or contact project leads.
