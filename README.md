# Streaming ETL Pipeline with Apache Kafka and Machine Learning
### Workshop 3 — ETL (G01) | Data Engineering & Artificial Intelligence
**Universidad Autónoma de Occidente**
Juan Sebastian Lozano Diaz
---

## What's this about

So for this workshop we basically had to stop doing batch processing the way we've been doing it all semester and figure out how to work with data in real time. The assignment was to build a full streaming pipeline using Apache Kafka that takes records one by one, runs them through a machine learning model, and stores the predictions in a database. Then on top of that, a live dashboard showing what's happening.

The data we used is the World Happiness Report from 2015 to 2019 — five CSV files with scores for countries based on things like GDP, health, freedom, corruption perception, etc. Sounds simple enough but the catch is that each year's file has different column names and structure. Like, in 2015 and 2016 they use `Economy (GDP per Capita)` and `Family`, but by 2018 they switched to `GDP per capita` and `Social support`. So before doing anything with the data we had to figure out how to merge all five years into something consistent.

The professor was pretty clear that this isn't about getting a perfect model. The whole point is building a pipeline that actually works end to end — streaming, validation, prediction, storage, and visualization all connected properly.

---

## How it works

There are two separate processes:

```
OFFLINE PROCESS (run this once before streaming)
─────────────────────────────────────────────────
Raw CSVs 2015–2019
      │
      ▼
EDA + Profiling (notebooks/eda.ipynb)
      │  - checked missing values, duplicates, schema differences per year
      │  - outlier analysis and correlation heatmap
      ▼
Data Cleaning + Schema Harmonization
      │  → data/processed/unified_happiness.csv
      ▼
Feature Engineering + Model Training (notebooks/model_training.ipynb)
      │  - compared 3 models: Linear Regression, Random Forest, Decision Tree
      │  → models/model.pkl


STREAMING PROCESS (runs in real time)
──────────────────────────────────────
unified_happiness.csv
      │
      ▼
Kafka Producer  →  topic: happiness-predictions
      │
      ▼
Kafka Consumer
      ├── [1] save raw event to DB first, no matter what
      ├── [2] validate the schema and values
      ├── [3] if valid   → load model → predict → save to fact_predictions
      └── [4] if invalid → mark it, skip prediction, keep running
      │
      ▼
PostgreSQL
      │
      ▼
Streamlit Dashboard
```

---

## Folder structure

```
etl-workshop3/
│
├── data/
│   ├── raw/                    # the original CSV files (2015–2019)
│   ├── processed/              # cleaned unified dataset + charts from EDA
│   └── streaming/
│
├── notebooks/
│   ├── eda.ipynb               # profiling, cleaning, harmonization
│   └── model_training.ipynb    # feature selection, training, evaluation, export
│
├── kafka/
│   ├── producer.py             # reads the CSV and streams events to Kafka
│   └── consumer.py             # validates, predicts, and saves to PostgreSQL
│
├── models/
│   └── model.pkl               # trained pipeline (scaler + model serialized)
│
├── sql/
│   ├── create_tables.sql       # all table definitions + indexes + seed data
│   └── kpis.sql                # the queries behind the dashboard charts
│
├── dashboards/
│   └── dashboard.py            # Streamlit + Plotly dashboard
│
├── setup_pipeline.py           # runs EDA + training in one script (no Jupyter needed)
├── docker-compose.yml          # spins up Kafka, Zookeeper, and PostgreSQL
├── requirements.txt
└── README.md
```

---

## The schema problem

This was honestly the most annoying part. Each year uses different names for the same columns:

| What it actually is | 2015 / 2016 | 2017 | 2018 / 2019 |
|---|---|---|---|
| Happiness score | `Happiness Score` | `Happiness.Score` | `Score` |
| GDP | `Economy (GDP per Capita)` | `Economy..GDP.per.Capita.` | `GDP per capita` |
| Social support | `Family` | `Family` | `Social support` |
| Health | `Health (Life Expectancy)` | `Health..Life.Expectancy.` | `Healthy life expectancy` |
| Freedom | `Freedom` | `Freedom` | `Freedom to make life choices` |
| Corruption | `Trust (Government Corruption)` | `Trust..Government.Corruption.` | `Perceptions of corruption` |

On top of that, 2015 and 2016 have a `Region` column that disappears from 2017 onwards, and `Dystopia Residual` is only there until 2017. So we had to write a harmonization function that maps each year to a unified schema and fills in `NaN` where columns don't exist.

Here's how we handled the specific issues we found:

| Issue | What we did | Why |
|---|---|---|
| `region` missing in 2017–2019 | set to `NaN` | can't use a column that only exists in some years as an ML feature |
| `dystopia_residual` missing in 2018–2019 | set to `NaN` | same reason |
| `Standard Error`, `Whisker.high`, confidence intervals | dropped them | not consistent across years and not relevant to the prediction |
| 1 null in 2018 `Perceptions of corruption` | filled with the yearly median | it's only 0.64% of that year's rows, median works fine |
| `happiness_rank` | excluded from ML features | it's literally derived from the score, using it would be leaking the target |
| `country` | excluded from ML features | 156+ categories and keeping it would leak year-over-year patterns |

---

## Feature selection

After cleaning we ended up with six numerical features:

```
gdp_per_capita, social_support, health_life_expectancy, freedom, generosity, corruption
```

We picked these because they're present in all five years after harmonization, they all showed real correlation with `happiness_score` in the EDA (GDP and health especially), and none of them are derived from the target variable. Another thing that helped is that these exact six fields are what the Kafka producer sends in each event — so the consumer can feed them directly to the model without any extra processing.

The model is saved as a full `sklearn.Pipeline` that includes the `StandardScaler` inside, so we never have to worry about scaling separately when doing inference. The consumer just loads the pkl file and calls `.predict()`.

---

## Model training

We tried three models and compared them:

| Model | MAE | RMSE | R² |
|---|---|---|---|
| Linear Regression | ~0.45 | ~0.55 | ~0.75 |
| Decision Tree (max_depth=5) | ~0.35 | ~0.46 | ~0.83 |
| **Random Forest** | **~0.28** | **~0.38** | **~0.90** |

Split was 70% training, 30% testing with `random_state=42` so results are reproducible.

We went with Random Forest. Best numbers across the board and cross-validation showed it wasn't overfitting. We didn't do any hyperparameter tuning because the point of this workshop isn't accuracy — it's the pipeline. The model is good enough to make the streaming part meaningful.

---

## Kafka setup

| Thing | Detail |
|---|---|
| Topic name | `happiness-predictions` |
| Producer | reads `unified_happiness.csv`, sends one event every 0.5 seconds |
| Consumer | receives event → saves raw → validates → predicts → saves result |
| Invalid events | stored in the raw table with a status label, pipeline never stops |
| Event format | JSON with: `country, year, gdp, family, health, freedom, generosity, corruption, actual_happiness_score` |

One thing the professor specifically required was that the raw event gets saved to the database **before** anything else happens — before validation, before prediction. That way even if a record is broken or causes an error you still have a trace of it. Invalid records get tagged as `INVALID_SCHEMA`, `INVALID_VALUES`, or `PREDICTION_ERROR` depending on what went wrong.

---

## Database design

We built a star schema in PostgreSQL. The diagram is in dbdiagram.io (see `sql/schema.dbml`).

### Tables:

```
raw_happiness_events        every Kafka message goes here first
  ├── raw_event_id (PK)
  ├── raw_payload            full original JSON stored as text
  └── processing_status      VALID | INVALID_SCHEMA | INVALID_VALUES | PREDICTION_ERROR

dim_country                 one row per country, created on first occurrence
dim_date                    one row per year (2015–2019 pre-seeded at startup)

fact_predictions            one row per valid prediction
  ├── raw_event_id (FK)     points back to the exact raw event that triggered it
  ├── actual_score
  ├── predicted_score
  ├── prediction_error       computed as ABS(actual - predicted)
  └── prediction_timestamp
```

The `raw_event_id` foreign key in `fact_predictions` is what lets you trace any prediction back to the original Kafka message. That was a requirement from the workshop spec.

---

## Dashboard

Built with Streamlit and Plotly. Dark theme, queries live from PostgreSQL — no CSV files involved.

| KPI | Visualization |
|---|---|
| Average prediction error by year | bar chart + histogram |
| Top 20 countries by predicted happiness | horizontal bar with color gradient |
| Predicted vs actual score | scatter plot with perfect-fit reference line |
| Prediction trends over time | dual line chart (actual + predicted + rolling avg) |
| Event processing status | donut chart |
| Feature correlations | heatmap |
| Feature averages by year | radar chart |

There are also 6 summary cards at the top showing avg error, avg actual, avg predicted, R², top country, and success rate. Plus a live table with the most recent predictions color-coded by error size.

---

## How to run it

### Step 1 — activate the virtual environment

```powershell
cd C:\Users\juans\etl-workshop3
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.venv\Scripts\Activate.ps1
```

### Step 2 — install dependencies

```powershell
pip install -r requirements.txt
```

### Step 3 — start Docker (Kafka + PostgreSQL)

```powershell
docker-compose up -d
```

Wait about 15 seconds. Check everything is running:

```powershell
docker ps
```

You should see `kafka`, `zookeeper`, and `postgres_happiness` all up.

### Step 4 — generate the dataset and train the model

```powershell
python setup_pipeline.py
```

This runs the full offline ETL in one shot — cleans the data, trains the model, saves `unified_happiness.csv` and `model.pkl`. You only need to do this once.

### Step 5 — open three terminals and run these

**Terminal 1 — consumer (keep this running):**
```powershell
.venv\Scripts\Activate.ps1
python kafka/consumer.py
```

**Terminal 2 — producer:**
```powershell
.venv\Scripts\Activate.ps1
python kafka/producer.py
```

**Terminal 3 — dashboard:**
```powershell
.venv\Scripts\Activate.ps1
streamlit run dashboards/dashboard.py
```

Dashboard opens at **http://localhost:8501**

---

## Database connection

| | |
|---|---|
| Host | `localhost` |
| Port | `5434` |
| Database | `happiness_db` |
| User | `etl_user` |
| Password | `etl_password` |

We use port 5434 instead of the default 5432 because other PostgreSQL instances from earlier workshops are already running on those ports.

---

## Stack

- Python 3.14
- Pandas + NumPy
- Scikit-learn + Joblib
- Apache Kafka (via Docker)
- kafka-python
- PostgreSQL 15 (via Docker)
- psycopg2 + SQLAlchemy
- Streamlit + Plotly
- Docker Compose
- Jupyter (for the notebooks)
