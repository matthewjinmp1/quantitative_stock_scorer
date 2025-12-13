# Quantitative Stock Scorer

A quantitative analysis tool for evaluating stocks based on various financial metrics and forward returns.

## Project Structure

```
quantitative_stock_scorer/
├── scripts/
│   ├── data_collection/     # Scripts for fetching stock data
│   │   ├── get_data.py
│   │   ├── get_metrics.py
│   │   ├── get_nasdaq_stocks.py
│   │   ├── get_nyse_stocks.py
│   │   └── get_sp500_2000.py
│   ├── analysis/            # Analysis and correlation scripts
│   │   ├── correlations.py
│   │   ├── analyze_data_coverage.py
│   │   ├── market_cap_stats.py
│   │   ├── scorer.py
│   │   └── graph_returns.py
│   ├── backtesting/         # Backtesting scripts
│   │   ├── backtester.py
│   │   └── rebalancing_backtester.py
│   ├── scrapers/            # Web scraping scripts
│   │   ├── scrape_glassdoor.py      # Scrape Glassdoor Best Places to Work (any year 2009-2025)
│   │   └── scrape_glassdoor_2009.py # Legacy scraper for 2009 only
│   └── utils/               # Utility and debug scripts
│       ├── check_credits.py
│       ├── check_ticker_match.py
│       ├── calculate_avg_forward_period.py
│       ├── debug_dates.py
│       ├── debug_dates2.py
│       ├── debug_ebit_ppe.py
│       └── simple_test.py
├── data/                    # Data files (JSON, JSONL, TXT)
│   ├── nasdaq.json
│   ├── nyse.json
│   ├── sp500_2000.json
│   ├── nasdaq_data.jsonl
│   ├── nyse_data.jsonl
│   ├── metrics.json
│   ├── scores.json
│   ├── ebit_ppe_portfolio_results.json
│   ├── available_metrics.txt
│   └── glassdoor/           # Glassdoor Best Places to Work data
│       ├── glassdoor_2009_companies.json
│       ├── glassdoor_2009_companies.txt
│       └── ... (other years)
├── tests/                   # Test files
│   ├── test_correlations.py
│   ├── test_get_metrics.py
│   ├── test_single_ticker.py
│   ├── run_tests.py
│   └── run_coverage.py
├── backtest_results/        # Backtest visualization results
├── rebalancing_backtest_results/  # Rebalancing backtest results
├── htmlcov/                 # Test coverage HTML reports
├── config.py               # Configuration file (API keys, etc.)
├── config.example.py       # Example configuration
├── requirements.txt        # Python dependencies
└── notes                   # Project notes

```

## Usage

### Data Collection

1. **Get stock lists:**
   ```bash
   python scripts/data_collection/get_nasdaq_stocks.py
   python scripts/data_collection/get_nyse_stocks.py
   ```

2. **Fetch stock data:**
   ```bash
   python scripts/data_collection/get_data.py
   ```

3. **Calculate metrics:**
   ```bash
   python scripts/data_collection/get_metrics.py
   ```

### Analysis

```bash
python scripts/analysis/correlations.py
```

### Backtesting

```bash
python scripts/backtesting/backtester.py
python scripts/backtesting/rebalancing_backtester.py
```

### Web Scraping

```bash
# Scrape Glassdoor Best Places to Work list for a specific year (2009-2025)
python scripts/scrapers/scrape_glassdoor.py 2009
python scripts/scrapers/scrape_glassdoor.py 2015
python scripts/scrapers/scrape_glassdoor.py 2020
```

### Running Tests

```bash
python tests/run_tests.py
python tests/run_coverage.py
```

## Configuration

Copy `config.example.py` to `config.py` and fill in your API keys and configuration.

## Dependencies

See `requirements.txt` for required Python packages.

