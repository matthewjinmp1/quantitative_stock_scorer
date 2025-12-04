"""
Create comparison charts showing how much each metric beats the benchmark annually
"""
import json
import os
import glob
import matplotlib.pyplot as plt
import numpy as np

def load_summary_data(folder: str) -> list:
    """Load all summary JSON files from a folder"""
    summary_files = glob.glob(os.path.join(folder, "summary_*.json"))
    results = []
    
    for file in summary_files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
                results.append(data)
        except Exception as e:
            print(f"Error loading {file}: {e}")
    
    return results

def create_comparison_chart(results: list, output_folder: str, chart_type: str):
    """Create a comparison chart showing excess returns vs benchmark"""
    if not results:
        print(f"No summary data found in {output_folder}")
        return
    
    # Sort by excess return (how much they beat the benchmark)
    results.sort(key=lambda x: x.get('excess_return', 0), reverse=True)
    
    metrics = [r['metric_name'] for r in results]
    excess_returns = [r.get('excess_return', 0) for r in results]
    metric_returns = [r.get('metric_weighted_annualized', 0) for r in results]
    benchmark_returns = [r.get('revenue_weighted_annualized', 0) for r in results]
    
    # Create the chart
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create bar chart
    x_pos = np.arange(len(metrics))
    colors = ['green' if x > 0 else 'red' for x in excess_returns]
    
    bars = ax.barh(x_pos, excess_returns, color=colors, alpha=0.7, edgecolor='black', linewidth=1)
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, excess_returns)):
        width = bar.get_width()
        label_x = width + (0.5 if width >= 0 else -0.5)
        ax.text(label_x, bar.get_y() + bar.get_height()/2, 
                f'{val:+.2f}%', 
                ha='left' if width >= 0 else 'right',
                va='center', fontweight='bold', fontsize=10)
    
    # Add metric and benchmark return labels
    for i, (metric_ret, bench_ret) in enumerate(zip(metric_returns, benchmark_returns)):
        ax.text(-0.5, i, f'{metric_ret:.1f}% / {bench_ret:.1f}%', 
                va='center', fontsize=9, style='italic', color='gray')
    
    # Customize chart
    ax.set_yticks(x_pos)
    ax.set_yticklabels(metrics, fontsize=11)
    ax.set_xlabel('Excess Annualized Return vs Revenue-Weighted Benchmark (%)', fontsize=12, fontweight='bold')
    ax.set_title(f'Metric Performance Comparison ({chart_type})\n'
                 f'Annualized Excess Return: Metric Portfolio vs Revenue-Weighted Benchmark',
                 fontsize=14, fontweight='bold', pad=20)
    ax.axvline(x=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    ax.grid(True, alpha=0.3, axis='x')
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='green', alpha=0.7, label='Outperforms Benchmark'),
        Patch(facecolor='red', alpha=0.7, label='Underperforms Benchmark')
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=10)
    
    # Add text annotation for metric/benchmark format
    ax.text(0.02, 0.98, 'Labels: Metric Return / Benchmark Return', 
            transform=ax.transAxes, fontsize=9, 
            verticalalignment='top', style='italic', color='gray',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.tight_layout()
    
    # Save chart
    chart_filename = os.path.join(output_folder, f'metric_comparison_{chart_type.lower()}.png')
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"   Comparison chart saved to {chart_filename}")
    plt.close()

def main():
    """Main function"""
    print("=" * 80)
    print("Creating Metric Comparison Charts")
    print("=" * 80)
    
    # Process static backtest results
    print("\n1. Processing static backtest results...")
    static_folder = "backtest_results"
    if os.path.exists(static_folder):
        static_results = load_summary_data(static_folder)
        if static_results:
            print(f"   Found {len(static_results)} metrics")
            create_comparison_chart(static_results, static_folder, "Static")
        else:
            print(f"   No summary data found in {static_folder}")
    else:
        print(f"   Folder {static_folder} does not exist")
    
    # Process rebalancing backtest results
    print("\n2. Processing rebalancing backtest results...")
    rebalancing_folder = "rebalancing_backtest_results"
    if os.path.exists(rebalancing_folder):
        rebalancing_results = load_summary_data(rebalancing_folder)
        if rebalancing_results:
            print(f"   Found {len(rebalancing_results)} metrics")
            create_comparison_chart(rebalancing_results, rebalancing_folder, "Rebalancing")
        else:
            print(f"   No summary data found in {rebalancing_folder}")
    else:
        print(f"   Folder {rebalancing_folder} does not exist")
    
    print("\n" + "=" * 80)
    print("Comparison charts created!")
    print("=" * 80)

if __name__ == "__main__":
    main()

