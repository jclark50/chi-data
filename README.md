# CHI-Data

**Climate & Health Initiative (CHI) Data Repository**  
This repository provides a shared structure for storing, processing, and analyzing climate and health datasets used across CHI projects. It contains reproducible pipelines, helper functions, and project-specific analyses (e.g., Madagascar, Sri Lanka) that build on a common foundation.

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
├── analysis/          # Project-specific scripts & outputs
│   ├── madagascar/    # Madagascar workability/disease analyses
│   └── srilanka/      # Sri Lanka analyses (in progress / placeholder)
│
├── analytics/         # Exploratory notebooks (R, Python, Jupyter)
│   └── madagascar/    # Example exploratory notebooks
│
├── pipelines/         # Automated data extraction / transformation
│   ├── extract.py     # Example ETL scripts
│   └── pipeline.yaml  # Pipeline configuration
│
├── helpers/           # Shared utility functions
│   ├── helpers.R      # Data wrangling, transformations
│   └── plot\_helpers.R # Plotting utilities (consistent styles)
│
├── outputs/           # (Optional) Shared space for derived datasets or figures
│
├── CHI-Data.Rproj     # RStudio project file
├── README.md          # This file
└── DESCRIPTION.txt    # High-level project description / metadata

````

---

## Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/CHI-Data.git
cd CHI-Data
````

### 2. Set Up Environment

* Install required R packages (`arrow`, `data.table`, `lubridate`, `ggplot2`, etc.)
* Optional: use a project-level `.Renviron` to point to data paths:

  ```
  ERA5_MADAGASCAR=C:/path/to/era5_madagascar
  ```

### 3. Run Scripts

* **Project analyses**: Run scripts in `analysis/<project>/`
* **Notebooks**: Explore data interactively in `analytics/<project>/`
* **Pipelines**: Reproduce data ingestion via `pipelines/`

---

## Conventions

* **Data location**: Raw ERA5 or other inputs are stored outside GitHub (Google Drive, S3, etc.). Scripts assume you set local paths via `.Renviron`.
* **Outputs**: Project outputs (figures, tables, compact CSVs) live in `analysis/<project>/outputs/` and are not tracked in Git.
* **Coding style**:

  * R scripts use `data.table` for performance.
  * Functions that may be reused belong in `helpers/`.
  * File naming: `P_` prefix for plots, `T_` prefix for tables.

---

## Current Projects

* **Madagascar**: Workability metrics, crop disease (Phytophthora), climate comparisons (ERA5, ERA5-Land).
* **Sri Lanka**: \[planned/placeholder].

---

## Contributing

1. Fork the repo and create a feature branch (`git checkout -b feature/my-feature`).
2. Add or modify scripts in the relevant subdir.
3. Submit a pull request for review.

---

## License

[MIT License](LICENSE) unless otherwise noted.
Data usage may be restricted by source—check project-specific READMEs for details.

---

## Contact

For questions, reach out to project leads or open an issue in this repo.
