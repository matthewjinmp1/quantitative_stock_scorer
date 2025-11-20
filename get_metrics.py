"""
Program to calculate metrics (total return, forward return, ROA, EBIT/PPE, EBIT/PPE TTM) from data.jsonl
and save results to metrics.json
EBIT/PPE = Operating Income / PPE (Property, Plant, and Equipment) - quarterly
EBIT/PPE TTM = Sum of Operating Income from quarters t, t-1, t-2, t-3 / Sum of PPE from quarters t, t-1, t-2, t-3
"""
import json
import os
from typing import Dict, List, Optional

def load_data_from_jsonl(filename: str = "data.jsonl") -> List[Dict]:
    """
    Load stock data from JSONL file (one JSON object per line)
    
    Args:
        filename: Path to JSONL file
    
    Returns:
        List of dictionaries containing stock data
    """
    if not os.path.exists(filename):
        print(f"Error: {filename} not found")
        return []
    
    stocks = []
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                try:
                    stock = json.loads(line)
                    stocks.append(stock)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
                    continue
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    return stocks

def extract_quarterly_data(stock_data: Dict) -> Optional[Dict]:
    """
    Extract and process quarterly data from stock data dictionary
    
    Args:
        stock_data: Dictionary containing stock data from data.jsonl
    
    Returns:
        Dictionary containing processed quarterly data with total_return, forward_return, ROA, EBIT/PPE, and EBIT/PPE TTM
        EBIT/PPE = Operating Income / PPE (quarterly)
        EBIT/PPE TTM = Sum of Operating Income from quarters t, t-1, t-2, t-3 / Sum of PPE from quarters t, t-1, t-2, t-3
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    
    # Extract arrays from the data dictionary
    # Try different possible key names for dates
    period_dates = None
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            period_dates = data[date_key]
            break
    
    if not period_dates:
        return None
    
    prices = data.get("period_end_price", [])
    dividends = data.get("dividends", [])
    roa = data.get("roa", [])
    operating_income = data.get("operating_income", [])
    ppe_net = data.get("ppe_net", [])
    
    if not prices:
        return None
    
    # Process the data into quarterly entries
    quarterly_data = []
    for j in range(len(period_dates)):
        current_price = prices[j] if j < len(prices) and prices[j] is not None else 0.0
        current_dividend = dividends[j] if j < len(dividends) and dividends[j] is not None else 0.0
        current_roa = roa[j] if j < len(roa) and roa[j] is not None else None
        
        # Calculate operating income / PPE (EBIT/PPE metric)
        ebit_ppe = None
        if (j < len(operating_income) and j < len(ppe_net) and 
            operating_income[j] is not None and ppe_net[j] is not None and 
            ppe_net[j] != 0):
            ebit_ppe = operating_income[j] / ppe_net[j]
        
        # Calculate total return for the quarter (compared to previous quarter)
        # Formula: Total Return = ((Ending Price - Beginning Price + Dividends) / Beginning Price) * 100
        total_return = None
        if j > 0 and j - 1 < len(prices):
            prev_price = prices[j-1] if prices[j-1] is not None else 0.0
            if prev_price > 0 and current_price > 0:
                total_return = ((current_price - prev_price + current_dividend) / prev_price) * 100
        
        quarterly_data.append({
            "period": period_dates[j],
            "price": current_price,
            "dividends": current_dividend,
            "roa": current_roa,
            "ebit_ppe": ebit_ppe,  # Operating income / PPE
            "total_return": total_return
        })
    
    # Calculate forward returns (annualized)
    # Forward return = cumulative total return from period j+1 to most recent period, annualized
    for j in range(len(quarterly_data)):
        forward_return = None
        if j < len(quarterly_data) - 1:
            # Start with 100% and compound each future period's return
            cumulative_value = 100.0
            valid_returns = True
            num_quarters = 0
            
            # Compound returns from period j+1 to the most recent period
            for k in range(j + 1, len(quarterly_data)):
                period_return = quarterly_data[k].get("total_return")
                if period_return is not None:
                    # Compound: multiply by (1 + return/100)
                    cumulative_value = cumulative_value * (1 + period_return / 100.0)
                    num_quarters += 1
                else:
                    # If any return is missing, we can't calculate forward return
                    valid_returns = False
                    break
            
            if valid_returns and num_quarters > 0:
                # Calculate cumulative return: (final_value - 100)
                cumulative_return = (cumulative_value - 100.0)
                
                # Annualize the return
                # Formula: annualized_return = ((1 + cumulative_return/100)^(1/years) - 1) * 100
                # Where years = num_quarters / 4
                years = num_quarters / 4.0
                if years > 0:
                    # Convert cumulative return to decimal (e.g., 50% -> 0.50)
                    cumulative_return_decimal = cumulative_return / 100.0
                    # Annualize: (1 + cumulative_return)^(1/years) - 1
                    annualized_return_decimal = (1 + cumulative_return_decimal) ** (1.0 / years) - 1.0
                    # Convert back to percentage
                    forward_return = annualized_return_decimal * 100.0
        
        quarterly_data[j]["forward_return"] = forward_return
    
    # Calculate TTM (Trailing Twelve Months) EBIT/PPE
    # TTM EBIT/PPE = Sum of operating income from quarters t, t-1, t-2, t-3 / Sum of PPE from quarters t, t-1, t-2, t-3
    for j in range(len(quarterly_data)):
        ebit_ppe_ttm = None
        
        # Need at least 4 quarters of data (j >= 3) to calculate TTM
        if j >= 3:
            # Sum operating income from quarters j, j-1, j-2, j-3
            ttm_operating_income = 0.0
            ttm_ppe = 0.0
            valid_data = True
            
            # Sum the 4 trailing quarters (t, t-1, t-2, t-3)
            for k in range(j - 3, j + 1):
                if k < len(operating_income) and k < len(ppe_net):
                    oi = operating_income[k] if k < len(operating_income) else None
                    ppe = ppe_net[k] if k < len(ppe_net) else None
                    
                    if oi is not None and ppe is not None:
                        ttm_operating_income += float(oi)
                        ttm_ppe += float(ppe)
                    else:
                        valid_data = False
                        break
                else:
                    valid_data = False
                    break
            
            # Calculate TTM EBIT/PPE if we have valid data and PPE is not zero
            if valid_data and ttm_ppe != 0:
                ebit_ppe_ttm = ttm_operating_income / ttm_ppe
        
        quarterly_data[j]["ebit_ppe_ttm"] = ebit_ppe_ttm
    
    return {
        "symbol": symbol,
        "company_name": company_name,
        "data": quarterly_data
    }

def calculate_metrics_for_all_stocks(stocks: List[Dict]) -> tuple:
    """
    Calculate metrics (total_return, forward_return) for all stocks
    
    Args:
        stocks: List of stock data dictionaries from data.jsonl
    
    Returns:
        Tuple of (results list, statistics dictionary)
    """
    results = []
    stats = {
        "total_stocks": len(stocks),
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "total_quarters": 0,
        "quarters_per_stock": [],
        "roa_data_points": 0,
        "ebit_ppe_data_points": 0,
        "ebit_ppe_ttm_data_points": 0,
        "forward_return_data_points": 0
    }
    
    for stock_data in stocks:
        symbol = stock_data.get("symbol", "Unknown")
        try:
            processed_data = extract_quarterly_data(stock_data)
            if processed_data:
                results.append(processed_data)
                stats["processed"] += 1
                num_quarters = len(processed_data.get("data", []))
                stats["total_quarters"] += num_quarters
                stats["quarters_per_stock"].append(num_quarters)
                
                # Count data completeness
                for entry in processed_data.get("data", []):
                    if entry.get("roa") is not None:
                        stats["roa_data_points"] += 1
                    if entry.get("ebit_ppe") is not None:
                        stats["ebit_ppe_data_points"] += 1
                    if entry.get("ebit_ppe_ttm") is not None:
                        stats["ebit_ppe_ttm_data_points"] += 1
                    if entry.get("forward_return") is not None:
                        stats["forward_return_data_points"] += 1
            else:
                stats["skipped"] += 1
        except Exception as e:
            stats["errors"] += 1
    
    return results, stats

def save_metrics_to_json(metrics_data: List[Dict], filename: str = "metrics.json"):
    """
    Save calculated metrics to JSON file
    
    Args:
        metrics_data: List of dictionaries containing metrics for each stock
        filename: Output filename
    """
    try:
        # Create output data with period, total_return, forward_return, roa, ebit_ppe, and ebit_ppe_ttm
        output_data = []
        for stock_data in metrics_data:
            output_stock = {
                "symbol": stock_data.get("symbol"),
                "company_name": stock_data.get("company_name"),
                "data": []
            }
            
            # Include period, total_return, forward_return, roa, ebit_ppe, and ebit_ppe_ttm in the output
            for entry in stock_data.get("data", []):
                output_entry = {
                    "period": entry.get("period"),
                    "total_return": entry.get("total_return"),
                    "forward_return": entry.get("forward_return"),
                    "roa": entry.get("roa"),
                    "ebit_ppe": entry.get("ebit_ppe"),  # Operating income / PPE (quarterly)
                    "ebit_ppe_ttm": entry.get("ebit_ppe_ttm")  # TTM Operating income / TTM PPE (4 trailing quarters)
                }
                output_stock["data"].append(output_entry)
            
            output_data.append(output_stock)
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nMetrics saved to {filename}")
        print(f"Saved metrics for {len(output_data)} stock(s)")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def main():
    """
    Main function to load data from data.jsonl, calculate metrics, and save to metrics.json
    """
    print("Calculating Metrics from data.jsonl")
    print("=" * 80)
    
    # Load data from data.jsonl
    print("\nLoading data from data.jsonl...")
    stocks = load_data_from_jsonl("data.jsonl")
    
    if not stocks:
        print("No stock data found in data.jsonl")
        return
    
    print(f"Found {len(stocks)} stock(s) in data.jsonl\n")
    
    # Calculate metrics for all stocks
    print("Calculating metrics (total_return, forward_return, ROA, EBIT/PPE, EBIT/PPE TTM)...")
    metrics_data, stats = calculate_metrics_for_all_stocks(stocks)
    
    if not metrics_data:
        print("\nNo metrics were successfully calculated.")
        return
    
    # Save to metrics.json
    save_metrics_to_json(metrics_data, "metrics.json")
    
    # Print summary statistics
    print(f"\n{'='*80}")
    print("PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total stocks in input: {stats['total_stocks']}")
    print(f"Successfully processed: {stats['processed']}")
    print(f"Skipped (no valid data): {stats['skipped']}")
    print(f"Errors: {stats['errors']}")
    
    if stats['processed'] > 0:
        print(f"\nQuarterly Data Statistics:")
        print(f"  Total quarters across all stocks: {stats['total_quarters']:,}")
        if stats['quarters_per_stock']:
            quarters_list = stats['quarters_per_stock']
            avg_quarters = sum(quarters_list) / len(quarters_list)
            print(f"  Average quarters per stock: {avg_quarters:.1f}")
            print(f"  Min quarters per stock: {min(quarters_list)}")
            print(f"  Max quarters per stock: {max(quarters_list)}")
        
        print(f"\nData Completeness (data points across all stocks/quarters):")
        print(f"  ROA: {stats['roa_data_points']:,}")
        print(f"  EBIT/PPE (quarterly): {stats['ebit_ppe_data_points']:,}")
        print(f"  EBIT/PPE (TTM): {stats['ebit_ppe_ttm_data_points']:,}")
        print(f"  Forward Return: {stats['forward_return_data_points']:,}")
    
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
