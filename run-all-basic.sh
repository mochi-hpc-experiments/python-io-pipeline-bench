#!/bin/bash

# Script to run all basic pipeline methods with varying concurrency levels
# Usage: ./run-all-basic.sh [venv_path]
# If no venv_path is provided, defaults to ./.venv

set -e  # Exit on any error

# Get the virtual environment path
VENV_PATH="${1:-./.venv}"

# Check if the virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    echo "Usage: $0 [venv_path]"
    echo "If no venv_path is provided, defaults to ./.venv"
    exit 1
fi

# Check if the activation script exists
if [ ! -f "$VENV_PATH/bin/activate" ]; then
    echo "Error: Virtual environment activation script not found at $VENV_PATH/bin/activate"
    exit 1
fi

echo "Using virtual environment: $VENV_PATH"
echo "Activating virtual environment..."

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Verify Python is available
if ! command -v python &> /dev/null; then
    echo "Error: Python not found in virtual environment"
    exit 1
fi

echo "Virtual environment activated successfully"
echo "Python version: $(python --version)"
echo ""

# Define the methods to test
METHODS=("sequential" "asyncio" "multiprocess" "threading")

# Define concurrency levels to test
CONCURRENCY_LEVELS=(1 2 4 8)

# Fixed parameters
DURATION=15
BUFFER_SIZE=256
RECV_DELAY=0.1

# Create results file with PID
RESULTS_FILE="results.$$.dat"
echo "Results will be written to: $RESULTS_FILE"

echo "Starting pipeline benchmarks..."
echo "Duration: ${DURATION}s, Buffer Size: ${BUFFER_SIZE}KB, Recv Delay: ${RECV_DELAY}s"
echo ""

# Run tests for each concurrency level and method
for concurrency in "${CONCURRENCY_LEVELS[@]}"; do
    echo "=== Testing concurrency level: $concurrency ==="
    
    for method in "${METHODS[@]}"; do
        # Skip sequential method for concurrency > 1
        if [ "$method" = "sequential" ] && [ "$concurrency" -gt 1 ]; then
            echo "Skipping sequential method with concurrency $concurrency (sequential only supports concurrency=1)"
            continue
        fi
        
        echo "Running $method with concurrency $concurrency..."
        
        # Create output file name
        output_file="bench_${method}_c${concurrency}"
        
        # Run the benchmark and redirect output to results file
        python io-pipeline-bench.py \
            --method "$method" \
            --duration "$DURATION" \
            --buffer-size "$BUFFER_SIZE" \
            --concurrency "$concurrency" \
            --recv-delay "$RECV_DELAY" \
            --output-file "$output_file" >> "$RESULTS_FILE"
        
        echo "Completed $method with concurrency $concurrency"
        echo ""
    done
    
    echo "=== Completed all methods for concurrency $concurrency ==="
    echo ""
done

echo "All benchmarks completed successfully!"
echo "Output files created with prefix 'bench_'"
echo "All benchmark results have been written to: $RESULTS_FILE"
