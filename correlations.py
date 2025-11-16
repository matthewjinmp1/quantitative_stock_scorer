"""
Calculate correlation between ROA and forward return across all stocks and periods
"""
import json
import numpy as np
from scipy.stats import pearsonr, spearmanr
from typing import List, Tuple

def load_data(filename: str = "data.json") -> List[dict]:
    """
    Load stock data from JSON file
    
    Args:
        filename: Path to JSON file
        
    Returns:
        List of stock data dictionaries
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

def extract_roa_forward_return_pairs(data: List[dict]) -> Tuple[List[float], List[float]]:
    """
    Extract all (ROA, forward_return) pairs from all stocks and periods
    Filters out None/null values and zero ROA values that might be invalid
    
    Args:
        data: List of stock data dictionaries
        
    Returns:
        Tuple of (roa_values, forward_return_values) lists
    """
    roa_values = []
    forward_return_values = []
    
    for stock in data:
        symbol = stock.get("symbol", "Unknown")
        for entry in stock.get("data", []):
            roa = entry.get("roa")
            forward_return = entry.get("forward_return")
            
            # Filter: both must be non-null and numeric
            # For ROA, also filter out 0 values that might be missing data
            # (though some companies might legitimately have 0 ROA)
            if (roa is not None and forward_return is not None and 
                isinstance(roa, (int, float)) and isinstance(forward_return, (int, float))):
                # Include all valid numeric pairs
                roa_values.append(float(roa))
                forward_return_values.append(float(forward_return))
    
    return roa_values, forward_return_values

def calculate_correlations(roa_values: List[float], forward_return_values: List[float]) -> dict:
    """
    Calculate correlation statistics between ROA and forward return
    
    Args:
        roa_values: List of ROA values
        forward_return_values: List of forward return values
        
    Returns:
        Dictionary with correlation statistics
    """
    if len(roa_values) != len(forward_return_values):
        raise ValueError("ROA and forward return lists must have the same length")
    
    if len(roa_values) < 2:
        return {
            "n_pairs": len(roa_values),
            "pearson_correlation": None,
            "pearson_pvalue": None,
            "spearman_correlation": None,
            "spearman_pvalue": None,
            "error": "Insufficient data points for correlation"
        }
    
    # Convert to numpy arrays
    roa_array = np.array(roa_values)
    forward_return_array = np.array(forward_return_values)
    
    # Calculate Pearson correlation (linear relationship)
    pearson_corr, pearson_p = pearsonr(roa_array, forward_return_array)
    
    # Calculate Spearman correlation (monotonic relationship, handles non-linear)
    spearman_corr, spearman_p = spearmanr(roa_array, forward_return_array)
    
    # Calculate basic statistics
    roa_mean = np.mean(roa_array)
    roa_std = np.std(roa_array)
    forward_return_mean = np.mean(forward_return_array)
    forward_return_std = np.std(forward_return_array)
    
    return {
        "n_pairs": len(roa_values),
        "pearson_correlation": float(pearson_corr),
        "pearson_pvalue": float(pearson_p),
        "spearman_correlation": float(spearman_corr),
        "spearman_pvalue": float(spearman_p),
        "roa_mean": float(roa_mean),
        "roa_std": float(roa_std),
        "roa_min": float(np.min(roa_array)),
        "roa_max": float(np.max(roa_array)),
        "forward_return_mean": float(forward_return_mean),
        "forward_return_std": float(forward_return_std),
        "forward_return_min": float(np.min(forward_return_array)),
        "forward_return_max": float(np.max(forward_return_array))
    }

def print_statistics(stats: dict):
    """
    Print correlation statistics in a readable format
    
    Args:
        stats: Dictionary with correlation statistics
    """
    print("\n" + "="*80)
    print("ROA vs Forward Return Correlation Analysis")
    print("="*80)
    print(f"\nNumber of data points: {stats['n_pairs']:,}")
    print(f"\nROA Statistics:")
    print(f"  Mean: {stats['roa_mean']:.4f}")
    print(f"  Std Dev: {stats['roa_std']:.4f}")
    print(f"  Min: {stats['roa_min']:.4f}")
    print(f"  Max: {stats['roa_max']:.4f}")
    print(f"\nForward Return Statistics:")
    print(f"  Mean: {stats['forward_return_mean']:.2f}%")
    print(f"  Std Dev: {stats['forward_return_std']:.2f}%")
    print(f"  Min: {stats['forward_return_min']:.2f}%")
    print(f"  Max: {stats['forward_return_max']:.2f}%")
    print(f"\nCorrelation Results:")
    print(f"  Pearson Correlation: {stats['pearson_correlation']:.4f}")
    print(f"  Pearson p-value: {stats['pearson_pvalue']:.4e}")
    if stats['pearson_pvalue'] < 0.05:
        print(f"    -> Statistically significant (p < 0.05)")
    else:
        print(f"    -> Not statistically significant (p >= 0.05)")
    print(f"\n  Spearman Correlation: {stats['spearman_correlation']:.4f}")
    print(f"  Spearman p-value: {stats['spearman_pvalue']:.4e}")
    if stats['spearman_pvalue'] < 0.05:
        print(f"    -> Statistically significant (p < 0.05)")
    else:
        print(f"    -> Not statistically significant (p >= 0.05)")
    print("\n" + "="*80)

def main():
    """
    Main function to calculate and display ROA vs forward return correlation
    """
    print("Loading data...")
    data = load_data("data.json")
    
    if not data:
        print("No data loaded. Exiting.")
        return
    
    print(f"Loaded data for {len(data)} stock(s)")
    
    # Extract all (ROA, forward_return) pairs
    print("\nExtracting ROA and forward return pairs...")
    roa_values, forward_return_values = extract_roa_forward_return_pairs(data)
    
    print(f"Found {len(roa_values):,} valid (ROA, forward_return) pairs")
    
    if len(roa_values) == 0:
        print("No valid data pairs found. Exiting.")
        return
    
    # Calculate correlations
    print("\nCalculating correlations...")
    stats = calculate_correlations(roa_values, forward_return_values)
    
    # Print statistics
    print_statistics(stats)

if __name__ == "__main__":
    main()

