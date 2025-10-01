#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Performance monitoring script for DAML builds
# This script monitors build performance and provides optimization suggestions

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check system resources
check_resources() {
    print_status $BLUE "Checking system resources..."
    
    # CPU info
    local cpu_count=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "unknown")
    print_status $GREEN "CPU cores: $cpu_count"
    
    # Memory info
    local memory_gb=$(free -g 2>/dev/null | awk '/^Mem:/{print $2}' || sysctl -n hw.memsize 2>/dev/null | awk '{print int($0/1024/1024/1024)}' || echo "unknown")
    print_status $GREEN "Memory: ${memory_gb}GB"
    
    # Disk space
    local disk_free=$(df -h . | awk 'NR==2{print $4}')
    print_status $GREEN "Available disk space: $disk_free"
}

# Function to check Bazel cache
check_bazel_cache() {
    print_status $BLUE "Checking Bazel cache status..."
    
    if [ -d ".bazel-cache" ]; then
        local cache_size=$(du -sh .bazel-cache 2>/dev/null | cut -f1 || echo "unknown")
        print_status $GREEN "Bazel cache size: $cache_size"
        
        # Check cache hit rate if possible
        if command -v bazel >/dev/null 2>&1; then
            print_status $YELLOW "Run 'bazel info' to see detailed cache statistics"
        fi
    else
        print_status $YELLOW "No local Bazel cache found"
    fi
}

# Function to provide optimization suggestions
provide_suggestions() {
    print_status $BLUE "Performance optimization suggestions:"
    
    echo "1. Build Performance:"
    echo "   - Use 'bazel build --jobs=auto' for optimal parallelization"
    echo "   - Enable disk cache: 'bazel build --disk_cache=.bazel-cache/disk'"
    echo "   - Use remote cache when available"
    
    echo "2. Resource Management:"
    echo "   - Set BAZEL_BUILD_OPTS='--local_cpu_resources=HOST_CPUS-1'"
    echo "   - Monitor memory usage during builds"
    echo "   - Clean up old cache files regularly"
    
    echo "3. Development Workflow:"
    echo "   - Use incremental builds: 'bazel build //...'"
    echo "   - Run tests in parallel: 'bazel test //...'"
    echo "   - Use 'bazel query' to understand dependencies"
}

# Main execution
main() {
    print_status $GREEN "DAML Performance Monitor"
    print_status $GREEN "========================"
    
    check_resources
    echo ""
    check_bazel_cache
    echo ""
    provide_suggestions
    
    print_status $GREEN "Performance monitoring complete!"
}

# Run main function
main "$@"
