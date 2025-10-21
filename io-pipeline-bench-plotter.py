#!/usr/bin/env python3

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

def parse_results_file(filename):
    """Parse the benchmark results file and return a DataFrame."""
    data = []
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip header lines and empty lines
            if line.startswith('#') or not line:
                continue
            
            # Split by tabs and extract relevant columns
            parts = line.split('\t')
            if len(parts) >= 11:  # Ensure we have enough columns
                method = parts[2]        # 3rd column (0-indexed)
                concurrency = int(parts[3])  # 4th column (0-indexed)
                throughput = float(parts[10])  # Final column (aggregate overall throughput)
                gil_enabled = parts[1]   # 2nd column (0-indexed)
                
                data.append({
                    'method': method,
                    'concurrency': concurrency,
                    'throughput': throughput,
                    'gil_enabled': gil_enabled
                })
    
    return pd.DataFrame(data)

def create_clustered_bar_chart(df, output_filename, gil_status):
    """Create a clustered bar chart from the DataFrame."""
    # Get unique methods and concurrency levels
    all_methods = sorted(df['method'].unique())
    concurrency_levels = sorted(df['concurrency'].unique())
    
    # Reorder methods to put 'sequential' first if it exists
    methods = []
    if 'sequential' in all_methods:
        methods.append('sequential')
        methods.extend([m for m in all_methods if m != 'sequential'])
    else:
        methods = all_methods
    
    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Set up bar positions
    x = np.arange(len(concurrency_levels))
    width = 0.2  # Width of each bar
    
    # Colors for different methods
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    # Create bars for each method
    for i, method in enumerate(methods):
        method_data = df[df['method'] == method]
        throughput_values = []
        
        for concurrency in concurrency_levels:
            method_concurrency_data = method_data[method_data['concurrency'] == concurrency]
            if not method_concurrency_data.empty:
                throughput_values.append(method_concurrency_data['throughput'].iloc[0])
            else:
                throughput_values.append(0)  # No data for this combination
        
        # Create bars for this method
        bars = ax.bar(x + i * width, throughput_values, width, 
                     label=method, color=colors[i % len(colors)], alpha=0.8)
        
        # Add value labels on top of bars
        for bar, value in zip(bars, throughput_values):
            if value > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                       f'{value:.2f}', ha='center', va='bottom', fontsize=8)
    
    # Customize the plot
    ax.set_xlabel('Concurrency Level', fontsize=12)
    ax.set_ylabel('Aggregate Throughput (MiB/s)', fontsize=12)
    title = f'I/O Pipeline Benchmark Results\nThroughput by Method and Concurrency\nGIL enabled: {gil_status}'
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x + width * (len(methods) - 1) / 2)
    ax.set_xticklabels(concurrency_levels)
    ax.legend(title='Method', loc='upper left')
    ax.grid(True, alpha=0.3, axis='y')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {output_filename}")
    
    # Close the figure to free memory
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Generate clustered bar chart from I/O pipeline benchmark results')
    parser.add_argument('input_file', help='Input results file (e.g., results.dat)')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return 1
    
    # Generate output filename
    base_name = os.path.splitext(args.input_file)[0]
    output_filename = f"{base_name}.png"
    
    print(f"Reading results from: {args.input_file}")
    
    # Parse the results file
    try:
        df = parse_results_file(args.input_file)
        if df.empty:
            print("Error: No valid data found in the input file.")
            return 1
        
        print(f"Found {len(df)} benchmark results")
        print(f"Methods: {sorted(df['method'].unique())}")
        print(f"Concurrency levels: {sorted(df['concurrency'].unique())}")
        
        # Check GIL status consistency
        gil_values = df['gil_enabled'].unique()
        if len(gil_values) == 1:
            gil_status = gil_values[0]
        else:
            gil_status = "Mixed"
        
        print(f"GIL status: {gil_status}")
        
        # Create the plot
        create_clustered_bar_chart(df, output_filename, gil_status)
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
