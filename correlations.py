"""
Calculate correlation between ROA and forward return across all stocks and periods
"""
import json
import numpy as np
from scipy.stats import pearsonr, spearmanr, rankdata
from typing import List, Tuple
import matplotlib.pyplot as plt

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
            "ranked_correlation": None,
            "ranked_pvalue": None,
            "spearman_correlation": None,
            "spearman_pvalue": None,
            "roa_ranks": [],
            "forward_return_ranks": [],
            "error": "Insufficient data points for correlation"
        }
    
    # Convert to numpy arrays
    roa_array = np.array(roa_values)
    forward_return_array = np.array(forward_return_values)
    
    # Rank the data before correlating
    # Rankdata assigns ranks from 1 to n (where n = number of data points)
    # With 1,019 data points, ranks will range from 1 (lowest value) to 1,019 (highest value)
    # Average ranks are used for tied values
    roa_ranks = rankdata(roa_array, method='average')
    forward_return_ranks = rankdata(forward_return_array, method='average')
    
    # Verify rank ranges (should be 1 to n)
    assert roa_ranks.min() == 1 and roa_ranks.max() == len(roa_array), \
        f"ROA ranks should range from 1 to {len(roa_array)}"
    assert forward_return_ranks.min() == 1 and forward_return_ranks.max() == len(forward_return_array), \
        f"Forward return ranks should range from 1 to {len(forward_return_array)}"
    
    # Calculate Pearson correlation on original values (linear relationship)
    pearson_corr, pearson_p = pearsonr(roa_array, forward_return_array)
    
    # Calculate correlation on ranked data (this is equivalent to Spearman)
    ranked_corr, ranked_p = pearsonr(roa_ranks, forward_return_ranks)
    
    # Also calculate Spearman for verification (should match ranked correlation)
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
        "ranked_correlation": float(ranked_corr),
        "ranked_pvalue": float(ranked_p),
        "spearman_correlation": float(spearman_corr),
        "spearman_pvalue": float(spearman_p),
        "roa_mean": float(roa_mean),
        "roa_std": float(roa_std),
        "roa_min": float(np.min(roa_array)),
        "roa_max": float(np.max(roa_array)),
        "forward_return_mean": float(forward_return_mean),
        "forward_return_std": float(forward_return_std),
        "forward_return_min": float(np.min(forward_return_array)),
        "forward_return_max": float(np.max(forward_return_array)),
        "roa_ranks": roa_ranks.tolist(),
        "forward_return_ranks": forward_return_ranks.tolist()
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
    print(f"  Pearson Correlation (on original values): {stats['pearson_correlation']:.4f}")
    print(f"  Pearson p-value: {stats['pearson_pvalue']:.4e}")
    if stats['pearson_pvalue'] < 0.05:
        print(f"    -> Statistically significant (p < 0.05)")
    else:
        print(f"    -> Not statistically significant (p >= 0.05)")
    print(f"\n  Ranked Correlation (on ranked values): {stats['ranked_correlation']:.4f}")
    print(f"  Ranked p-value: {stats['ranked_pvalue']:.4e}")
    if stats['ranked_pvalue'] < 0.05:
        print(f"    -> Statistically significant (p < 0.05)")
    else:
        print(f"    -> Not statistically significant (p >= 0.05)")
    print(f"\n  Spearman Correlation (for verification): {stats['spearman_correlation']:.4f}")
    print(f"  Spearman p-value: {stats['spearman_pvalue']:.4e}")
    print(f"    -> Note: Ranked correlation should match Spearman correlation")
    print("\n" + "="*80)

def plot_ranks(roa_ranks: List[float], forward_return_ranks: List[float], 
               stats: dict, output_file: str = "roa_forward_return_ranks.png"):
    """
    Create a scatter plot of ranked ROA vs ranked forward return
    
    Args:
        roa_ranks: List of ROA ranks (1 to n)
        forward_return_ranks: List of forward return ranks (1 to n)
        stats: Dictionary with correlation statistics
        output_file: Output filename for the plot
    """
    plt.figure(figsize=(12, 8))
    
    # Create scatter plot
    plt.scatter(roa_ranks, forward_return_ranks, alpha=0.5, s=20)
    
    # Add labels and title
    plt.xlabel('ROA Rank (1 = lowest, ' + str(stats['n_pairs']) + ' = highest)', fontsize=12)
    plt.ylabel('Forward Return Rank (1 = lowest, ' + str(stats['n_pairs']) + ' = highest)', fontsize=12)
    plt.title('Ranked ROA vs Ranked Forward Return\n(All Stocks, All Periods)', fontsize=14, fontweight='bold')
    
    # Add correlation statistics as text
    stats_text = f"Ranked Correlation: {stats['ranked_correlation']:.4f}\n"
    stats_text += f"p-value: {stats['ranked_pvalue']:.4e}\n"
    stats_text += f"Number of data points: {stats['n_pairs']:,}\n"
    stats_text += f"Ranks range from 1 to {stats['n_pairs']}"
    
    plt.text(0.05, 0.95, stats_text, transform=plt.gca().transAxes,
             fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    # Add diagonal reference line (perfect correlation would follow this)
    max_rank = stats['n_pairs']
    plt.plot([1, max_rank], [1, max_rank], 'r--', alpha=0.3, label='Perfect Correlation')
    plt.legend()
    
    # Set equal aspect ratio and limits
    plt.xlim(0, max_rank + 50)
    plt.ylim(0, max_rank + 50)
    plt.gca().set_aspect('equal', adjustable='box')
    
    # Add grid
    plt.grid(True, alpha=0.3)
    
    # Tight layout
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to {output_file}")
    
    # Show plot
    plt.show()

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
    
    # Create visualization of ranks
    print("\nCreating scatter plot of ranks...")
    try:
        plot_ranks(stats['roa_ranks'], stats['forward_return_ranks'], stats)
    except Exception as e:
        print(f"Warning: Could not create plot: {e}")
        print("Continuing without visualization...")

if __name__ == "__main__":
    main()

