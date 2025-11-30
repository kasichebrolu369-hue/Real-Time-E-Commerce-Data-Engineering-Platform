"""
Master Data Generator Runner
============================
Runs all data generators in sequence to create the complete dataset.

This script orchestrates the generation of:
1. User profiles
2. Product catalog
3. Transactions
4. Clickstream events

Execute this to generate all synthetic data for the project.
"""

import os
import sys
import time
from datetime import datetime

def run_generator(script_name, description):
    """
    Run a data generator script
    
    Args:
        script_name: Name of the Python script to run
        description: Human-readable description
    """
    print("\n" + "=" * 80)
    print(f"🚀 STARTING: {description}")
    print("=" * 80)
    
    start_time = time.time()
    
    # Run the script
    exit_code = os.system(f"python {script_name}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    if exit_code == 0:
        print(f"\n✅ COMPLETED: {description}")
        print(f"   Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    else:
        print(f"\n❌ FAILED: {description}")
        print(f"   Exit code: {exit_code}")
        sys.exit(1)
    
    return duration


def main():
    """Main execution function"""
    print("=" * 80)
    print("E-COMMERCE DATA GENERATION - MASTER RUNNER")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    overall_start = time.time()
    
    # List of generators to run in order
    generators = [
        ("generate_users.py", "User Profiles Generation"),
        ("generate_products.py", "Product Catalog Generation"),
        ("generate_transactions.py", "Transaction Data Generation"),
        ("generate_clickstream.py", "Clickstream Events Generation")
    ]
    
    durations = {}
    
    # Run each generator
    for script, description in generators:
        duration = run_generator(script, description)
        durations[description] = duration
    
    overall_end = time.time()
    overall_duration = overall_end - overall_start
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 GENERATION SUMMARY")
    print("=" * 80)
    
    for description, duration in durations.items():
        print(f"✓ {description:.<50} {duration:>8.2f}s ({duration/60:>6.2f}m)")
    
    print("-" * 80)
    print(f"{'TOTAL TIME':.<50} {overall_duration:>8.2f}s ({overall_duration/60:>6.2f}m)")
    
    print("\n" + "=" * 80)
    print("✅ ALL DATA GENERATION COMPLETE!")
    print("=" * 80)
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n📁 Generated Datasets:")
    print("   • User Profiles: 100,000 records")
    print("   • Product Catalog: 10,000 records")
    print("   • Transactions: 500,000 records")
    print("   • Clickstream Events: 1,000,000+ records")
    
    print("\n📂 Output Locations:")
    print("   • data/raw/users/")
    print("   • data/raw/products/")
    print("   • data/raw/transactions/")
    print("   • data/raw/clickstream/")
    print("   • data/samples/ (sample datasets)")
    
    print("\n🎯 Next Steps:")
    print("   1. Review sample data in data/samples/")
    print("   2. Run Pig ETL scripts to clean data")
    print("   3. Load data into Hive warehouse")
    print("   4. Execute Impala queries for analytics")
    print("   5. Run Spark ML pipelines")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
