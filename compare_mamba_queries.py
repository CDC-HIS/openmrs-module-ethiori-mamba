import os
import time
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

QUERIES_DIR = 'omod/src/main/resources/_etl/derived/line_list/facts/line_list_queries'

def get_all_sp_names():
    sp_names = []
    if os.path.exists(QUERIES_DIR):
        for file in os.listdir(QUERIES_DIR):
            if file.endswith('.sql') and file.startswith('sp_fact_'):
                sp_names.append(file.replace('.sql', ''))
    return sorted(sp_names)

def compare_queries(db_url, sp_name, test_date, results_log):
    print(f"\n" + "="*60)
    print(f" Testing {sp_name} vs {sp_name}_v2")
    print(f" Test Date: {test_date}")
    print("="*60)

    engine = create_engine(db_url)
    
    # 1. RUN ORIGINAL (V1)
    print(f"\n[1/2] Running original {sp_name}...")
    start_v1 = time.time()
    try:
        df_v1 = pd.read_sql(f"CALL {sp_name}('{test_date}')", con=engine)
    except Exception as e:
        print(f" [ERROR] Could not execute original {sp_name}: {e}")
        return
    v1_time = time.time() - start_v1
    print(f" -> V1 took {v1_time:.3f} seconds. Rows fetched: {len(df_v1)}")

    # 2. RUN OPTIMIZED (V2)
    print(f"\n[2/2] Running optimized {sp_name}_v2...")
    start_v2 = time.time()
    try:
        df_v2 = pd.read_sql(f"CALL {sp_name}_v2('{test_date}')", con=engine)
    except Exception as e:
        if "PROCEDURE" in str(e) and "does not exist" in str(e):
            print(f" [SKIP] Optimized version {sp_name}_v2 not deployed yet. Skipping...\n")
        else:
            print(f" [ERROR] Could not execute optimized {sp_name}_v2: {e}")
        return
    v2_time = time.time() - start_v2
    print(f" -> V2 took {v2_time:.3f} seconds. Rows fetched: {len(df_v2)}")

    # 3. VERIFICATION
    print(f"\n" + "-"*60)
    print(f" PERFORMANCE COMPARISON")
    print("-"*60)
    speed_diff = v1_time - v2_time
    if speed_diff > 0:
        print(f" [SPEED] V2 is {speed_diff:.3f} seconds FASTER ({(v1_time/v2_time):.1f}x speedup)")
    else:
        print(f" [SPEED] V2 is {abs(speed_diff):.3f} seconds SLOWER")

    print(f"\n" + "-"*60)
    print(f" DATA VALIDATION")
    print("-"*60)
    
    status = "PASS"
    notes = "100% Identical"
    
    if len(df_v1) != len(df_v2):
        print(f" [FAIL] Row count mismatch! V1: {len(df_v1)}, V2: {len(df_v2)}")
        status = "FAIL"
        notes = f"Row mismatch: {len(df_v1)} vs {len(df_v2)}"
        if 'UUID' in df_v1.columns and 'UUID' in df_v2.columns:
            missing_in_v2 = set(df_v1['UUID']) - set(df_v2['UUID'])
            missing_in_v1 = set(df_v2['UUID']) - set(df_v1['UUID'])
            if missing_in_v2: print(f"  -> UUIDs missing in V2: {list(missing_in_v2)[:5]}...")
            if missing_in_v1: print(f"  -> UUIDs unexpected in V2: {list(missing_in_v1)[:5]}...")
    else:
        # Align column orders just in case V2 shifted column positions slightly
        common_cols = [c for c in df_v1.columns if c in df_v2.columns]
        missing_cols_v1 = set(df_v2.columns) - set(df_v1.columns)
        missing_cols_v2 = set(df_v1.columns) - set(df_v2.columns)
        
        if missing_cols_v2 or missing_cols_v1:
            print(f" [WARN] Column schemas differ!")
            if missing_cols_v2: print(f"  -> Columns missing in V2: {missing_cols_v2}")
            if missing_cols_v1: print(f"  -> Columns unexpected in V2: {missing_cols_v1}")
            status = "WARN"
            notes = "Schema Mismatch"
        
        if len(common_cols) > 0 and len(df_v1) > 0:
            df_v1_check = df_v1[common_cols].sort_values(by=common_cols).reset_index(drop=True)
            df_v2_check = df_v2[common_cols].sort_values(by=common_cols).reset_index(drop=True)
            
            # Compare
            diff_cols = []
            for col in common_cols:
                # Fill NAs to compare consistently
                s1 = df_v1_check[col].fillna('__N/A__')
                s2 = df_v2_check[col].fillna('__N/A__')
                if not s1.equals(s2):
                    diff_cols.append(col)
            
            if len(diff_cols) == 0:
                print(" [PASS] Data execution output is 100% physically identical!")
            else:
                print(f" [FAIL] The following {len(diff_cols)} columns have data discrepancies:")
                for c in diff_cols:
                    print(f"   -> {c}")
                status = "FAIL"
                notes = f"Data mismatch in {len(diff_cols)} cols"
        elif len(df_v1) == 0:
            print(" [PASS] Both queries returned 0 rows.")

    # Record to summary log
    results_log.append({
        "Procedure": sp_name,
        "V1_Time_sec": round(v1_time, 3),
        "V2_Time_sec": round(v2_time, 3),
        "Speedup_Multiplier": round(v1_time / max(v2_time, 0.001), 1),
        "V1_Rows": len(df_v1),
        "V2_Rows": len(df_v2),
        "Status": status,
        "Notes": notes
    })

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EtHiOhRi Mamba ETL Query Automator and Comparator")
    parser.add_argument("--sp", type=str, default="sp_fact_line_list_tx_curr_query", help="Base name of the SP or 'all' to run sequence.")
    parser.add_argument("--date", type=str, default="2024-12-31", help="Date argument to pass to the queries")
    parser.add_argument("--db", type=str, default="mysql+pymysql://root:password@localhost:3306/openmrs", help="SQLAlchemy Database URL")
    
    args = parser.parse_args()
    results_log = []
    
    if args.sp.lower() == 'all':
        print(f"Scanning {QUERIES_DIR} for SPs...")
        sps = get_all_sp_names()
        print(f"Found {len(sps)} queries to test!\n")
        
        for sp in sps:
            compare_queries(args.db, sp, args.date, results_log)
            
        print("\n" + "="*60)
        print(" FULL SUITE TESTING COMPLETED")
        print("="*60)
    else:
        compare_queries(args.db, args.sp, args.date, results_log)

    if results_log:
        df_results = pd.DataFrame(results_log)
        csv_filename = f"mamba_benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_results.to_csv(csv_filename, index=False)
        print(f"\nDetailed report saved to: {csv_filename}")
        
        # Print a nice markdown-style summary table to console
        print("\n" + df_results[['Procedure', 'Status', 'Speedup_Multiplier', 'Notes']].to_string(index=False))
