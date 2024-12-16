# NYC Open Data Analysis Project

## Overview

This project focuses on analyzing New York City's open data to derive meaningful
insights and patterns. It processes and analyzes various datasets from NYC Open
Data portal to help understand urban trends and patterns.

## Features

- Data collection from NYC Open Data portal
- Data preprocessing and cleaning
- Statistical analysis and visualization
- Comprehensive reporting of findings

## Prerequisites

- Python 3.8+
- Poetry package manager
- Git

## Installation

1. Clone the repository

```bash
git clone https://github.com/lokeshpillar/final-project-bigdata
cd final-project-bigdata
```

2. Setup poetry

```bash
poetry install
```

3. Setup config file

We need to setup the config file with the correct credentials for the MongoDB
database. Update the `config.py` file with the correct credentials.

## Project Structure

```bash
.
├── README.md
├── nyc_opendata
│   ├── __init__.py
│   ├── config.py
│   ├── data_cleaning.py
│   ├── data_ingestion.py
│   ├── database.py
│   ├── gold_layer.py
│   ├── plots.ipynb
│   └── reset.py
├── poetry.lock
├── pyproject.toml
└── tests
    ├── __init__.py
    ├── test_cleaner.py
    ├── test_database.py
    ├── test_gold_layer.py
    └── test_ingestion.py
```

## Running the project

1. Ingest data

```bash
poetry run ingest
```

2. Clean data

```bash
poetry run clean
```

3. Gold layer

```bash
poetry run gold
```

4. Reset

```bash
poetry run reset
```

## Testing

```bash
poetry run pytest
```
