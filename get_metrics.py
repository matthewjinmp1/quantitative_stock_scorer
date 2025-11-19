"""
Program to calculate metrics (total return, forward return, ROA, EBIT/PPE) from data.jsonl
and save results to metrics.json
EBIT/PPE = Operating Income / PPE (Property, Plant, and Equipment)
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
        Dictionary containing processed quarterly data with total_return, forward_return, ROA, and EBIT/PPE
        EBIT/PPE = Operating Income / PPE
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
        print(f"  Warning: No date data found for {symbol}")
        return None
    
    prices = data.get("period_end_price", [])
    dividends = data.get("dividends", [])
    roa = data.get("roa", [])
    operating_income = data.get("operating_income", [])
    ppe_net = data.get("ppe_net", [])
    
    if not prices:
        print(f"  Warning: No price data found for {symbol}")
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
    
    return {
        "symbol": symbol,
        "company_name": company_name,
        "data": quarterly_data
    }

def calculate_metrics_for_all_stocks(stocks: List[Dict]) -> List[Dict]:
    """
    Calculate metrics (total_return, forward_return) for all stocks
    
    Args:
        stocks: List of stock data dictionaries from data.jsonl
    
    Returns:
        List of dictionaries containing calculated metrics for each stock
    """
    results = []
    
    for stock_data in stocks:
        symbol = stock_data.get("symbol", "Unknown")
        try:
            processed_data = extract_quarterly_data(stock_data)
            if processed_data:
                results.append(processed_data)
                num_quarters = len(processed_data.get("data", []))
                print(f"  Processed {symbol}: {num_quarters} quarters")
            else:
                print(f"  Skipped {symbol}: No valid data")
        except Exception as e:
            print(f"  Error processing {symbol}: {e}")
    
    return results

def save_metrics_to_json(metrics_data: List[Dict], filename: str = "metrics.json"):
    """
    Save calculated metrics to JSON file
    
    Args:
        metrics_data: List of dictionaries containing metrics for each stock
        filename: Output filename
    """
    try:
        # Create output data with only period, total_return, forward_return, and roa
        output_data = []
        for stock_data in metrics_data:
            output_stock = {
                "symbol": stock_data.get("symbol"),
                "company_name": stock_data.get("company_name"),
                "data": []
            }
            
            # Include period, total_return, forward_return, roa, and ebit_ppe in the output
            for entry in stock_data.get("data", []):
                output_entry = {
                    "period": entry.get("period"),
                    "total_return": entry.get("total_return"),
                    "forward_return": entry.get("forward_return"),
                    "roa": entry.get("roa"),
                    "ebit_ppe": entry.get("ebit_ppe")  # Operating income / PPE
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
    print("Calculating metrics (total_return, forward_return, ROA, EBIT/PPE)...")
    metrics_data = calculate_metrics_for_all_stocks(stocks)
    
    if not metrics_data:
        print("\nNo metrics were successfully calculated.")
        return
    
    # Save to metrics.json
    save_metrics_to_json(metrics_data, "metrics.json")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Successfully calculated metrics for {len(metrics_data)} stock(s):")
    for stock_data in metrics_data:
        num_quarters = len(stock_data.get("data", []))
        print(f"  - {stock_data['company_name']} ({stock_data['symbol']}): {num_quarters} quarters")

if __name__ == "__main__":
    main()
