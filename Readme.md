# ETL Pipeline - Winter Wheat Condition (South Dakota)

ETL pipeline for analyzing winter wheat conditions in South Dakota using USDA QuickStats API data.

## Features

- **Extract**: Download historical wheat condition data from USDA QuickStats API
- **Transform**: Clean data and compute condition indices
- **Load**: Save cleaned datasets
- **Analyze**: Generate comprehensive visualizations and statistical reports

## Project Structure

```
etl-winter-wheat/
├── src/
│   ├── Pipeline.py      # Main ETL pipeline
│   ├── analysis.py      # Data visualization functions
│   └── My_logger.py     # Logging configuration
├── data/                # Raw and cleaned data (created at runtime)
├── logs/                # Log files (created at runtime)
├── analytics/           # Generated plots (created at runtime)
├── params.json          # Pipeline configuration
├── pyproject.toml       # Poetry dependencies
└── README.md
```

## Setup

### 1. Install Dependencies

Using Poetry (recommended):
```bash
poetry install
```

Using pip:
```bash
pip install -r requirements.txt
```

### 2. Get API Key

1. Register at https://quickstats.nass.usda.gov/api
2. Get your API key from your account

## Usage

Run the pipeline:

```bash
# Using Poetry
poetry run python src/Pipeline.py --api_key YOUR_API_KEY

# Using regular Python
python src/Pipeline.py --api_key YOUR_API_KEY
```

## Configuration

Edit `params.json` to modify:
- State, commodity, and condition parameters
- Year range for data extraction
- API endpoint

## Output

The pipeline generates:

1. **Data files** in `data/`:
   - `cww-sd_raw_data_DDMMYYYY.csv` - Raw API data
   - `cww-sd_cleaned_data_DDMMYYYY.csv` - Cleaned dataset

2. **Visualizations** in `analytics/`:
   - `dashboard_Winter-Wheat-Condition-South-Dakota_DDMMYYYY.png` - Descriptive statistics
   - `KPI_Winter-Wheat-Condition-South-Dakota_DDMMYYYY.png` - Deep analysis

3. **Logs** in `logs/`:
   - `wwcsd_metrics.log` - Pipeline metrics and statistics
   - `wwcsd_error.log` - Error messages

## Development

### Code Style

Format code with Black:
```bash
poetry run black src/
```

Lint with Flake8:
```bash
poetry run flake8 src/
```
