# COVID-19 GeoInsights Mobility Pipeline

A data pipeline that generated **automated, targeted COVID-19 mobility report templates**
for decision makers across Colombia during the pandemic. The pipeline ingests human-mobility
data from **Facebook's GeoInsights / Movement Range** products together with reported case data,
processes it through a staged geospatial/epidemiological workflow, and produces maps, indicators
(effective reproductive number, mobility thresholds, alerts) and prediction outputs. These
machine-generated templates were then reviewed and finished by experts before being handed off
to decision makers, and were published on a **weekly** basis.

Developed by the **DataLama** team, 2020–2021.

## Citation

> 📌 _Citation pending._ The reference for the associated paper will be added
> here once available.

## Architecture

The pipeline is **CSV-driven**: a single configuration file declares which scripts run, for which
location, in which stage, and in what order. The orchestrator reads that file and executes the
scripts in sequence.

### Execution flow

```
                    ┌──────────────────────────────────────────────────────────────────┐
   PRE  ───────────▶│  per location:                                                   │──────────▶  POS
 (global steps)     │  EXTRACT → UNIFY → AGGLOMERATE → CONSTRUCT → ANALYSE → WRAPPING   │       (global steps)
                    └──────────────────────────────────────────────────────────────────┘
```

`PRE` scripts run once globally (e.g. downloading Facebook data). Then, for **each location**, the
ordered stages run in sequence. Finally, `POS` scripts run once globally (e.g. assembling report
files and the website).

### Data-stage flow

In parallel with the execution stages, data for each location flows through a fixed folder layout
(created automatically by `GenericUnifier`):

```
data_stages/<location>/raw  →  unified  →  agglomerated  →  constructed
                                                                  │
                                                                  ▼
                                                      analysis/  →  report/
```

- **raw** – source data as extracted (cases, movement, polygons).
- **unified** – normalized to a common schema across all countries/locations.
- **agglomerated** – nodes aggregated into analysis units (by radius, community, or geometry).
- **constructed** – derived datasets such as daily graphs and prediction databases.
- **analysis / report** – figures, indicators and the artifacts that feed report templates.

> These data trees live **outside this repository** in a sibling `data_repo/` directory (see
> [External data](#external-data)). Only the code is versioned here.

The pipeline is a **mixed Python and R** codebase. The orchestrator runs each script with either
`python` or `Rscript` depending on its declared `script_type`.

---

## Repository layout

```
.
├── excecute_pipeline.sh           # Entry point: sets PYTHONPATH and runs the orchestrator
├── encrypt_file.sh                # Helper: encrypt a sensitive source file
├── requirements_excecution.txt    # Python dependencies
├── global_config/                 # Global configuration (paths, credentials, keys)
├── notes/                         # Research notebooks / scratch
└── pipeline_scripts/
    ├── excecute_all.py            # Orchestrator (reads the run plan, runs stages)
    ├── encrypt_file.py            # Fernet encryption of sensitive case files
    ├── configuration/             # CSV files that drive the pipeline (see Configuration)
    ├── functions/                 # Shared library used by all stages
    ├── PRE/                       # Global pre-steps
    ├── extractors/                # Pull source data from external sources
    ├── unifiers/                  # Normalize raw data into the unified schema
    ├── agglomerators/             # Aggregate nodes into analysis units
    ├── constructors/              # Build daily graphs / prediction databases
    ├── analysis/                  # Maps, Rt, mobility thresholds, predictions, TDA, …
    ├── wrappers/                  # Multi-step report/analysis orchestration
    ├── POS/                       # Global post-steps (copy report files, build website)
    └── special_reports/           # Bespoke, location-specific reports (Bogotá)
```

### What each directory does

| Directory                                  | Role                                                                                                                                                                                                                                                                              |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline_scripts/PRE/`                    | Global pre-steps run before any location, e.g. `download_facebook.py`, `extrapolate_movement_range.py`.                                                                                                                                                                           |
| `pipeline_scripts/extractors/`             | Pull source data from external providers: Johns Hopkins, USA, Italy, Bogotá, Google Trends, generic web crawlers, and single-file git extractors.                                                                                                                                 |
| `pipeline_scripts/unifiers/`               | Normalize each location's raw data (cases, polygons, movement) into a shared schema. Backed by `functions/generic_unifier_class.py` (`GenericUnifier`) and per-country `Unifier` classes in `functions/locations/`, resolved through `functions/constants.py::get_unifier_class`. |
| `pipeline_scripts/agglomerators/`          | Aggregate geographic nodes into analysis units. Methods: `radial`, `community` (graph communities), and `geometry`.                                                                                                                                                               |
| `pipeline_scripts/constructors/`           | Build derived datasets: daily mobility graphs and prediction databases.                                                                                                                                                                                                           |
| `pipeline_scripts/analysis/`               | The analytics layer (Python + R): choropleth and graph maps, effective reproductive number (Rt), mobility thresholds and alerts, movement-range plots, polygon/ML predictions, topological data analysis (TDA), and socio-economic analysis.                                      |
| `pipeline_scripts/wrappers/`               | Orchestrate multi-step analyses for a polygon or report, e.g. `report_bogota.py`, `single_polygon_wrapper.py`, and the prediction / Rt / socio-economic wrappers.                                                                                                                 |
| `pipeline_scripts/POS/`                    | Global post-steps: `copy_report_files.py` and `generate_website_files.py`.                                                                                                                                                                                                        |
| `pipeline_scripts/special_reports/bogota/` | Bespoke Bogotá analyses: superspreading analysis, localidad boxplots, housing super-spreaders, Gini, and cases/deaths plots.                                                                                                                                                      |
| `pipeline_scripts/functions/`              | Shared library imported across stages: Facebook (`fb_functions.py`), geospatial (`geo_functions.py`), time series, prediction, BigQuery, Rt estimation, movement range, Google Trends, agglomeration, and constants.                                                              |
| `global_config/`                           | Global configuration loaded by `config.py` (see Configuration).                                                                                                                                                                                                                   |
| `notes/`                                   | Research notebooks and exploratory material.                                                                                                                                                                                                                                      |

---

## Configuration

### Global config (`global_config/`)

The file has the following pattern:

```csv
name,value
data_dir,/absolute/path/to/data_repo/data/
analysis_dir,/absolute/path/to/data_repo/analysis/
report_dir,/absolute/path/to/data_repo/report/
website_dir,/absolute/path/to/web_page/
mapper_dir,/absolute/path/to/mapperKD/
fb_user,<your-facebook-account-email>
fb_pwd,<your-facebook-account-password>
key_string,<fernet-encryption-key>
```

### Pipeline driver files (`pipeline_scripts/configuration/`)

| File                       | Purpose                                                                                 |
| -------------------------- | --------------------------------------------------------------------------------------- |
| `selected_excecutions.csv` | **The run plan.** One row per script to execute (see columns below).                    |
| `facebook_extraction.csv`  | Facebook GeoInsights dataset names/IDs and start dates per location.                    |
| `selected_polygons.csv`    | The focus polygons (`poly_id`, `location_name`, `agglomeration`) reports are built for. |
| `export_files.csv`         | Maps produced artifacts (figures/tables) into named report slots.                       |

**`selected_excecutions.csv` columns:**

| Column              | Meaning                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------- |
| `location_name`     | Location the script runs for (e.g. `Bogota`, `Colombia`); `PRE`/`POS` rows may use `All`.   |
| `locations_folder`  | Folder name for that location's data (e.g. `bogota`).                                       |
| `stage`             | One of `PRE`, `extract`, `unify`, `agglomerate`, `construct`, `analyse`, `wrapping`, `POS`. |
| `folder_location`   | Directory under `pipeline_scripts/` where the script lives.                                 |
| `position`          | Order within the stage (ascending).                                                         |
| `excecute`          | `YES` / `NO` toggle — only `YES` rows run.                                                  |
| `script_name`       | Script filename (the `.py`/`.R` extension is optional).                                     |
| `script_type`       | `python` or `R`.                                                                            |
| `script_parameters` | Space-separated arguments passed to the script.                                             |

---

## External data

The configuration points `data_dir`, `analysis_dir` and `report_dir` at a sibling **`data_repo/`**
directory that is **not part of this repository**. It contains the input case/mobility data and is
where the pipeline writes the `data_stages/`, `analysis/` and `report/` outputs described in
[Architecture](#architecture). You must provide this data layout (and the matching `description.csv`
each unifier expects per location) for the pipeline to run end to end.

---

## Installation

**Requirements:** Python 3 and R (with `Rscript` on the `PATH`), since stages are split across both.

```bash
# Python dependencies
python3 -m venv env
source env/bin/activate
pip install -r requirements_excecution.txt
```

Key Python libraries include pandas, geopandas/shapely/rtree, scikit-learn, **pymc3** (Rt
estimation), **python-igraph** (graph communities), selenium (Facebook download automation),
google-cloud-bigquery, pytrends / googletrans, POT and fastdtw. R stages additionally require an R
installation with the geospatial/plotting packages they import.

---

## Running the pipeline

1. Create `global_config/config_file.csv` (see [Configuration](#configuration)) and make sure
   `data_repo/` is populated.
2. Edit `pipeline_scripts/configuration/selected_excecutions.csv` and set `excecute` to `YES` for the
   steps you want to run (and `NO` for the rest).
3. Run the orchestrator:

```bash
bash excecute_pipeline.sh
```

This sets `PYTHONPATH` to include `pipeline_scripts/` and `pipeline_scripts/functions/`, then runs
`pipeline_scripts/excecute_all.py`. The orchestrator runs all enabled `PRE` scripts, then iterates
each location through the ordered stages, then runs the `POS` scripts.

**Progress log.** Each executed script appends a row to `excecution_progress.csv` (timestamp, script
name, parameters, return code, elapsed seconds). The log is reset at the start of every run.

---

## Adding a new location

1. Implement a `Unifier` subclass for the location under
   `pipeline_scripts/functions/locations/` (mirror an existing one, e.g. `colombia_functions.py`),
   providing at least `build_cases_geo` / `build_polygons` as documented in
   `functions/generic_unifier_class.py`.
2. Register it in `functions/constants.py::get_unifier_class`.
3. Provide the location's data folder (with its `description.csv`) under `data_repo/.../data_stages/`.
4. Add the location's rows to `configuration/selected_excecutions.csv` (and, for Facebook data,
   `facebook_extraction.csv` and `selected_polygons.csv`).

---

## Special reports

`pipeline_scripts/special_reports/bogota/` contains analyses written specifically for Bogotá that go
beyond the standard pipeline outputs — superspreading analysis, per-_localidad_ boxplots, housing
super-spreaders, Gini coefficients, and cases/deaths plots. These are driven through the Bogotá
wrappers (e.g. `wrappers/report_bogota.py`).

---

## Credits & license

Developed by the **DataLama** team, 2020–2021, as part of the COVID-19 mobility reporting effort for
Colombia. Released under the **MIT License** — see [LICENSE](LICENSE).
