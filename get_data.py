"""
Program to fetch quarterly price and dividend data from QuickFS API
for stocks listed in tickers.json
"""
import json
from quickfs import QuickFS
from typing import Dict, List, Optional
from config import QUICKFS_API_KEY

# QuickFS API Configuration
API_KEY = QUICKFS_API_KEY

def load_tickers(filename: str = "tickers.json") -> List[str]:
    """
    Load ticker symbols from JSON file
    
    Args:
        filename: Path to JSON file containing tickers
    
    Returns:
        List of ticker symbols
    """
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            return data.get("tickers", [])
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return []

def format_symbols(tickers: List[str]) -> List[str]:
    """
    Format ticker symbols for QuickFS (add :US for US stocks)
    
    Args:
        tickers: List of ticker symbols
    
    Returns:
        List of formatted ticker symbols
    """
    formatted = []
    for ticker in tickers:
        if ":" not in ticker:
            formatted.append(f"{ticker}:US")
        else:
            formatted.append(ticker)
    return formatted

def process_quarterly_data(data: Dict, symbol: str) -> Optional[Dict]:
    """
    Process quarterly data from QuickFS response
    
    Args:
        data: Full data response from QuickFS
        symbol: Original ticker symbol
    
    Returns:
        Dictionary containing processed quarterly data
    """
    if not data or "financials" not in data:
        return None
    
    quarterly = data["financials"].get("quarterly")
    if not quarterly:
        return None
    
    # Extract relevant data
    period_dates = quarterly.get("period_end_date", [])
    prices = quarterly.get("period_end_price", [])
    dividends = quarterly.get("dividends", [])
    
    # Get company name from metadata
    metadata = data.get("metadata", {})
    company_name = metadata.get("name", symbol)
    
    # Process the data
    quarterly_data = []
    for j in range(len(period_dates)):
        if j < len(prices) and j < len(dividends):
            current_price = prices[j] if prices[j] else 0.0
            current_dividend = dividends[j] if dividends[j] else 0.0
            
            # Calculate total return (compared to previous period)
            total_return = None
            if j > 0:
                prev_price = prices[j-1] if prices[j-1] else 0.0
                if prev_price > 0 and current_price > 0:
                    total_return = ((current_price - prev_price + current_dividend) / prev_price) * 100
            
            quarterly_data.append({
                "period": period_dates[j],
                "price": current_price,
                "dividends": current_dividend,
                "total_return": total_return
            })
    
    # Calculate forward returns
    for j in range(len(quarterly_data)):
        forward_return = None
        if j < len(quarterly_data) - 1:
            cumulative_value = 100.0
            valid_returns = True
            
            for k in range(j + 1, len(quarterly_data)):
                period_return = quarterly_data[k].get("total_return")
                if period_return is not None:
                    cumulative_value = cumulative_value * (1 + period_return / 100.0)
                else:
                    valid_returns = False
                    break
            
            if valid_returns:
                forward_return = (cumulative_value - 100.0)
        
        quarterly_data[j]["forward_return"] = forward_return
    
    return {
        "symbol": symbol,
        "company_name": company_name,
        "data": quarterly_data
    }

def process_batch_data(batch_data: Dict, formatted_symbol: str, original_symbol: str) -> Optional[Dict]:
    """
    Process batch data response from QuickFS get_data_batch
    
    Batch response format: {metric: {ticker: [values], ...}, ...}
    
    Args:
        batch_data: Full batch data response from QuickFS
        formatted_symbol: Formatted ticker symbol (e.g., 'GOOGL:US')
        original_symbol: Original ticker symbol (e.g., 'GOOGL')
    
    Returns:
        Dictionary containing processed quarterly data
    """
    if not batch_data:
        return None
    
    # Batch response format: {metric: {ticker: [values], ...}, ...}
    # Extract the metrics for this specific ticker
    period_dates = batch_data.get("period_end_date", {}).get(formatted_symbol, [])
    prices = batch_data.get("period_end_price", {}).get(formatted_symbol, [])
    dividends = batch_data.get("dividends", {}).get(formatted_symbol, [])
    roa = batch_data.get("roa", {}).get(formatted_symbol, [])
    
    if not period_dates:
        return None
    
    # Get company name - might need to fetch separately or from metadata
    # For now, use symbol as company name
    company_name = original_symbol
    
    # Process the data
    # Note: QuickFS returns data in chronological order (oldest to newest)
    # Each index j represents a quarter, with j=0 being the oldest quarter
    quarterly_data = []
    for j in range(len(period_dates)):
        current_price = prices[j] if j < len(prices) and prices[j] is not None else 0.0
        current_dividend = dividends[j] if j < len(dividends) and dividends[j] is not None else 0.0
        current_roa = roa[j] if j < len(roa) and roa[j] is not None else None
        
        # Calculate total return for the quarter (compared to previous quarter)
        # Formula: Total Return = ((Ending Price - Beginning Price + Dividends) / Beginning Price) * 100
        # Where:
        #   - Beginning Price = price at end of previous quarter (prev_price)
        #   - Ending Price = price at end of current quarter (current_price)
        #   - Dividends = dividends paid during current quarter (current_dividend)
        total_return = None
        if j > 0 and j - 1 < len(prices):
            prev_price = prices[j-1] if prices[j-1] is not None else 0.0
            if prev_price > 0 and current_price > 0:
                # Total return includes both price appreciation and dividends
                total_return = ((current_price - prev_price + current_dividend) / prev_price) * 100
        
        quarterly_data.append({
            "period": period_dates[j],
            "price": current_price,
            "dividends": current_dividend,
            "roa": current_roa,
            "total_return": total_return
        })
    
    # Calculate forward returns
    # Forward return = cumulative total return from period j+1 to most recent period
    # This represents what the return would be if you held from period j+1 to the present
    for j in range(len(quarterly_data)):
        forward_return = None
        if j < len(quarterly_data) - 1:
            # Start with 100% and compound each future period's return
            cumulative_value = 100.0
            valid_returns = True
            
            # Compound returns from period j+1 to the most recent period
            for k in range(j + 1, len(quarterly_data)):
                period_return = quarterly_data[k].get("total_return")
                if period_return is not None:
                    # Compound: multiply by (1 + return/100)
                    cumulative_value = cumulative_value * (1 + period_return / 100.0)
                else:
                    # If any return is missing, we can't calculate forward return
                    valid_returns = False
                    break
            
            if valid_returns:
                # Convert back to percentage return: (final_value - 100)
                forward_return = (cumulative_value - 100.0)
        
        quarterly_data[j]["forward_return"] = forward_return
    
    return {
        "symbol": original_symbol,
        "company_name": company_name,
        "data": quarterly_data
    }

def fetch_quarterly_data_batch(tickers: List[str]) -> List[Optional[Dict]]:
    """
    Fetch quarterly price and dividend data from QuickFS API for multiple tickers
    Uses get_data_batch to fetch all tickers in a single API call
    
    Args:
        tickers: List of stock ticker symbols (e.g., ['GOOGL', 'MSFT'])
    
    Returns:
        List of dictionaries containing quarterly data for each ticker
    """
    # Format symbols for QuickFS
    formatted_symbols = format_symbols(tickers)
    
    try:
        print(f"Fetching quarterly data for {len(tickers)} ticker(s)...")
        client = QuickFS(API_KEY)
        
        # Define metrics and period for batch request
        # Metrics explanation:
        # - period_end_price: Stock price at the end of each fiscal quarter
        # - dividends: Total dividends paid during each fiscal quarter (quarterly, not cumulative)
        # - period_end_date: Date marking the end of each fiscal quarter
        # - roa: Return on Assets (quarterly)
        # 
        # Total Return Calculation:
        #   Total Return = ((Ending Price - Beginning Price + Dividends) / Beginning Price) * 100
        #   Where: Beginning Price = period_end_price of previous quarter
        #          Ending Price = period_end_price of current quarter
        #          Dividends = dividends paid during current quarter
        metrics = ['period_end_price', 'dividends', 'period_end_date', 'roa']
        period = "FQ-100:FQ"  # Last 100 fiscal quarters to current
        
        # Fetch all tickers in a single batch request
        batch_data = client.get_data_batch(formatted_symbols, metrics, period)
        
        # Process the batch response
        # Batch response format: {metric: {ticker: [values], ...}, ...}
        results = []
        for i, formatted_symbol in enumerate(formatted_symbols):
            original_symbol = tickers[i]
            try:
                processed_data = process_batch_data(batch_data, formatted_symbol, original_symbol)
                if processed_data:
                    results.append(processed_data)
                else:
                    print(f"  No data found for {original_symbol}")
                    results.append(None)
            except Exception as e:
                print(f"  Error processing {original_symbol}: {e}")
                results.append(None)
        
        return results
    
    except Exception as e:
        print(f"  Error in batch fetch: {e}")
        return [None] * len(tickers)

def fetch_quarterly_price_dividends(symbol: str) -> Optional[Dict]:
    """
    Fetch quarterly price and dividend data from QuickFS API (individual request)
    This function is kept for backward compatibility but batch is preferred
    
    Args:
        symbol: Stock ticker symbol (e.g., 'GOOGL')
    
    Returns:
        Dictionary containing quarterly data with dates, prices, and dividends
    """
    # Format symbol for QuickFS (add :US for US stocks)
    if ":" not in symbol:
        symbol_formatted = f"{symbol}:US"
    else:
        symbol_formatted = symbol
    
    try:
        print(f"Fetching quarterly data for {symbol}...")
        client = QuickFS(API_KEY)
        data = client.get_data_full(symbol_formatted)
        
        if not data or "financials" not in data:
            print(f"  Error: No financial data found for {symbol}")
            return None
        
        quarterly = data["financials"].get("quarterly")
        if not quarterly:
            print(f"  Error: No quarterly data found for {symbol}")
            return None
        
        # Extract relevant data
        period_dates = quarterly.get("period_end_date", [])
        prices = quarterly.get("period_end_price", [])
        dividends = quarterly.get("dividends", [])
        
        # Get company name from metadata
        metadata = data.get("metadata", {})
        company_name = metadata.get("name", symbol)
        
        # Combine data and calculate total return for each period
        quarterly_data = []
        for i in range(len(period_dates)):
            if i < len(prices) and i < len(dividends):
                current_price = prices[i] if prices[i] else 0.0
                current_dividend = dividends[i] if dividends[i] else 0.0
                
                # Calculate total return (compared to previous period)
                total_return = None
                if i > 0:
                    prev_price = prices[i-1] if prices[i-1] else 0.0
                    if prev_price > 0 and current_price > 0:
                        # Total return = (current_price - prev_price + dividend) / prev_price
                        total_return = ((current_price - prev_price + current_dividend) / prev_price) * 100
                
                quarterly_data.append({
                    "period": period_dates[i],
                    "price": current_price,
                    "dividends": current_dividend,
                    "total_return": total_return
                })
        
        # Calculate forward return for each period (from t+1 to most recent)
        # Forward return is the cumulative total return from the next period to the most recent period
        for i in range(len(quarterly_data)):
            forward_return = None
            if i < len(quarterly_data) - 1:  # Not the last period
                # Calculate cumulative return from period i+1 to the end (most recent)
                cumulative_value = 100.0  # Start at 100%
                valid_returns = True
                
                for j in range(i + 1, len(quarterly_data)):
                    period_return = quarterly_data[j].get("total_return")
                    if period_return is not None:
                        # Compound the return: multiply by (1 + return/100)
                        cumulative_value = cumulative_value * (1 + period_return / 100.0)
                    else:
                        # If we hit a None return, we can't calculate forward return
                        valid_returns = False
                        break
                
                if valid_returns:
                    # Convert back to percentage return: (final_value - 100)
                    forward_return = (cumulative_value - 100.0)
            
            quarterly_data[i]["forward_return"] = forward_return
        
        return {
            "symbol": symbol,
            "company_name": company_name,
            "data": quarterly_data
        }
    
    except Exception as e:
        print(f"  Error fetching data for {symbol}: {e}")
        return None

def format_price(value: float) -> str:
    """Format price value"""
    if value is None or value == 0:
        return "N/A"
    return f"${value:.2f}"

def format_dividend(value: float) -> str:
    """Format dividend value"""
    if value is None or value == 0:
        return "$0.00"
    return f"${value:.4f}"

def print_quarterly_data(stock_data: Dict):
    """
    Print quarterly price and dividend data for a stock
    
    Args:
        stock_data: Dictionary containing stock quarterly data
    """
    if not stock_data or "data" not in stock_data:
        print(f"\nNo data available for {stock_data.get('symbol', 'Unknown')}")
        return
    
    symbol = stock_data["symbol"]
    company_name = stock_data.get("company_name", symbol)
    quarterly_data = stock_data["data"]
    
    print(f"\n{'='*80}")
    print(f"{company_name} ({symbol})")
    print(f"{'='*80}")
    print(f"\n{'Period':<15} {'Price':<15} {'Dividends':<15} {'Total Return':<15} {'Forward Return':<15}")
    print("-" * 95)
    
    # Print in reverse order (most recent first)
    for entry in reversed(quarterly_data):
        period = entry["period"]
        price = format_price(entry["price"])
        dividend = format_dividend(entry["dividends"])
        total_return = entry.get("total_return")
        forward_return = entry.get("forward_return")
        
        if total_return is not None:
            return_str = f"{total_return:.2f}%"
        else:
            return_str = "N/A"
        
        if forward_return is not None:
            forward_str = f"{forward_return:.2f}%"
        else:
            forward_str = "N/A"
        
        print(f"{period:<15} {price:<15} {dividend:<15} {return_str:<15} {forward_str:<15}")
    
    print(f"\nTotal quarters: {len(quarterly_data)}")

def save_to_json(all_data: List[Dict], filename: str = "data.json"):
    """
    Save total return and forward return data to JSON file
    
    Args:
        all_data: List of dictionaries containing quarterly data for all stocks
        filename: Output filename
    """
    try:
        # Create output data with only period and total_return
        output_data = []
        for stock_data in all_data:
            output_stock = {
                "symbol": stock_data.get("symbol"),
                "company_name": stock_data.get("company_name"),
                "data": []
            }
            
            # Include period, total_return, forward_return, and roa in the output
            for entry in stock_data.get("data", []):
                output_entry = {
                    "period": entry.get("period"),
                    "total_return": entry.get("total_return"),
                    "forward_return": entry.get("forward_return"),
                    "roa": entry.get("roa")
                }
                output_stock["data"].append(output_entry)
            
            output_data.append(output_stock)
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nData saved to {filename}")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def main():
    """
    Main function to fetch quarterly price and dividend data,
    calculate total return, and save only total return to JSON
    """
    print("Fetching Quarterly Price and Dividend Data")
    print("=" * 80)
    
    # Load tickers
    tickers = load_tickers("tickers.json")
    if not tickers:
        print("No tickers found. Please check tickers.json")
        return
    
    print(f"\nFound {len(tickers)} ticker(s): {', '.join(tickers)}\n")
    
    # Fetch data for all tickers in batch
    all_results = fetch_quarterly_data_batch(tickers)
    
    # Filter out None results and print data
    all_data = []
    for stock_data in all_results:
        if stock_data:
            all_data.append(stock_data)
            print_quarterly_data(stock_data)
    
    # Save to file
    if all_data:
        save_to_json(all_data)
        
        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Successfully fetched data for {len(all_data)} stock(s):")
        for stock_data in all_data:
            num_quarters = len(stock_data.get("data", []))
            print(f"  - {stock_data['company_name']} ({stock_data['symbol']}): {num_quarters} quarters")
    else:
        print("\nNo data was successfully fetched.")

if __name__ == "__main__":
    main()

