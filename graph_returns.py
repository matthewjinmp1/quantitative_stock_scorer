"""
Program to graph total return over time for each ticker from data.json
"""
import json
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Dict, List
import numpy as np

def load_data(filename: str = "data.json") -> List[Dict]:
    """
    Load total return data from JSON file
    
    Args:
        filename: Path to JSON file containing total return data
    
    Returns:
        List of dictionaries containing stock data
    """
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return []

def parse_period(period_str: str) -> datetime:
    """
    Parse period string (e.g., "2020-03") to datetime object
    
    Args:
        period_str: Period string in format "YYYY-MM"
    
    Returns:
        datetime object representing the period end date
    """
    try:
        year, month = period_str.split('-')
        # Set day to last day of the month
        if month == '03':
            day = 31
        elif month == '06':
            day = 30
        elif month == '09':
            day = 30
        elif month == '12':
            day = 31
        else:
            day = 28
        return datetime(int(year), int(month), day)
    except:
        return datetime(2000, 1, 1)

def calculate_cumulative_returns(returns: List[float]) -> List[float]:
    """
    Calculate cumulative returns from period returns
    
    Args:
        returns: List of period returns (as percentages)
    
    Returns:
        List of cumulative returns (as percentages)
    """
    cumulative = []
    cumulative_value = 100.0  # Start at 100%
    
    for ret in returns:
        if ret is not None:
            # Convert percentage return to multiplier
            cumulative_value = cumulative_value * (1 + ret / 100.0)
        cumulative.append(cumulative_value)
    
    return cumulative

def graph_total_returns(data: List[Dict]):
    """
    Graph cumulative total return over time for each ticker (one graph per ticker)
    Displays graphs as popups sequentially - close one to see the next
    
    Args:
        data: List of dictionaries containing stock data
    """
    if not data:
        print("No data to graph")
        return
    
    for stock_data in data:
        symbol = stock_data.get("symbol", "Unknown")
        company_name = stock_data.get("company_name", symbol)
        quarterly_data = stock_data.get("data", [])
        
        if not quarterly_data:
            print(f"Skipping {symbol}: No data")
            continue
        
        # Extract periods and returns
        periods = []
        returns = []
        
        for entry in quarterly_data:
            period_str = entry.get("period")
            total_return = entry.get("total_return")
            
            if period_str:
                periods.append(parse_period(period_str))
                returns.append(total_return)
        
        if not periods:
            print(f"Skipping {symbol}: No valid periods")
            continue
        
        # Calculate cumulative returns
        cumulative_returns = calculate_cumulative_returns(returns)
        
        # Create a new figure for each ticker
        plt.figure(figsize=(14, 8))
        
        # Plot the line
        plt.plot(periods, cumulative_returns, label=f"{symbol} ({company_name})", 
                color='#1f77b4', linewidth=2.5, marker='o', markersize=4)
        
        # Format the graph
        plt.title(f"Cumulative Total Return Over Time - {symbol} ({company_name})", 
                 fontsize=16, fontweight='bold', pad=20)
        plt.xlabel("Date", fontsize=12)
        plt.ylabel("Cumulative Return (%)", fontsize=12)
        plt.grid(True, alpha=0.3, linestyle='--')
        
        # Format x-axis dates
        plt.gcf().autofmt_xdate()
        
        # Add a horizontal line at 100% (starting value)
        plt.axhline(y=100, color='gray', linestyle=':', alpha=0.5, linewidth=1)
        
        # Add statistics text box
        valid_returns = [r for r in returns if r is not None]
        if valid_returns:
            final_return = cumulative_returns[-1] - 100
            avg_return = np.mean(valid_returns)
            max_return = max(valid_returns)
            min_return = min(valid_returns)
            
            stats_text = f"Final Return: {final_return:.1f}%\n"
            stats_text += f"Avg Quarterly Return: {avg_return:.2f}%\n"
            stats_text += f"Best Quarter: {max_return:.1f}%\n"
            stats_text += f"Worst Quarter: {min_return:.1f}%"
            
            plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes,
                    fontsize=10, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Display the graph (blocks until window is closed)
        print(f"Displaying graph for {symbol} - Close the window to see the next one...")
        plt.show()
        
        # Close the figure after window is closed
        plt.close()

def graph_period_returns(data: List[Dict]):
    """
    Graph period-by-period total return for each ticker (one graph per ticker)
    Displays graphs as popups sequentially - close one to see the next
    
    Args:
        data: List of dictionaries containing stock data
    """
    if not data:
        print("No data to graph")
        return
    
    for stock_data in data:
        symbol = stock_data.get("symbol", "Unknown")
        company_name = stock_data.get("company_name", symbol)
        quarterly_data = stock_data.get("data", [])
        
        if not quarterly_data:
            continue
        
        # Extract periods and returns
        periods = []
        returns = []
        
        for entry in quarterly_data:
            period_str = entry.get("period")
            total_return = entry.get("total_return")
            
            if period_str and total_return is not None:
                periods.append(parse_period(period_str))
                returns.append(total_return)
        
        if not periods:
            continue
        
        # Create a new figure for each ticker
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Create bar plot
        ax.bar(periods, returns, color='#1f77b4', alpha=0.7, width=60)
        
        # Format the graph
        ax.set_title(f"Quarterly Total Return by Period - {symbol} ({company_name})", 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Total Return (%)", fontsize=12)
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')
        ax.axhline(y=0, color='black', linestyle='-', alpha=0.3, linewidth=1)
        
        # Format x-axis dates
        plt.gcf().autofmt_xdate()
        
        plt.tight_layout()
        
        # Display the graph (blocks until window is closed)
        print(f"Displaying graph for {symbol} - Close the window to see the next one...")
        plt.show()
        
        # Close the figure after window is closed
        plt.close()

def main():
    """
    Main function to load data and create graphs
    """
    print("Loading data from data.json...")
    data = load_data("data.json")
    
    if not data:
        print("No data found. Please run data_getter.py first.")
        return
    
    print(f"Loaded data for {len(data)} stock(s)")
    for stock in data:
        symbol = stock.get("symbol", "Unknown")
        num_periods = len(stock.get("data", []))
        print(f"  - {symbol}: {num_periods} periods")
    
    # Create cumulative returns graph for each ticker
    print("\nCreating cumulative returns graphs (one per ticker)...")
    graph_total_returns(data)
    
    # Optionally create period returns graph for each ticker
    # print("\nCreating period returns graphs (one per ticker)...")
    # graph_period_returns(data)

if __name__ == "__main__":
    main()

