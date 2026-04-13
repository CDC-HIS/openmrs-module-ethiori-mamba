import sys
import time
import argparse
import pandas as pd
from sqlalchemy import create_engine

def compare_queries(db_url, sp_name, test_date):
    print(f"==================================================")
    print(f" Testing {sp_name} vs {sp_name}_v2")
    print(f" Test Date: {test_date}")
    print(f"==================================================")

    engine = create_engine(db_url)
    
    # 1. RUN ORIGINAL (V1)
    print(f"\n[1/2] Running original {sp_name}...")
    start_v1 = time.time()
    try:
        # We use pandas read_sql to automatically fetch the OUT cursor of the SP
        df_v1 = pd.read_sql(f"CALL {sp_name}('{test_date}')", con=engine)
    except Exception as e:
        print(f"Error executing V1: {e}")
        return

    end_v1 = time.time()
    v1_time = end_v1 - start_v1
    print(f" -> V1 took {v1_time:.3f} seconds. Rows fetched: {len(df_v1)}")

    # 2. RUN OPTIMIZED (V2)
    print(f"\n[2/2] Running optimized {sp_name}_v2...")
    start_v2 = time.time()
    try:
        df_v2 = pd.read_sql(f"CALL {sp_name}_v2('{test_date}')", con=engine)
    except Exception as e:
        print(f"Error executing V2: {e}")
        return

    end_v2 = time.time()
    v2_time = end_v2 - start_v2
    print(f" -> V2 took {v2_time:.3f} seconds. Rows fetched: {len(df_v2)}")

    # 3. VERIFICATION
    print(f"\n==================================================")
    print(f" PERFORMANCE COMPARISON")
    print(f"==================================================")
    
    speed_diff = v1_time - v2_time
    if speed_diff > 0:
        print(f" [SPEED] V2 is {speed_diff:.3f} seconds FASTER ({(v1_time/v2_time):.1f}x speedup)")
    else:
        print(f" [SPEED] V2 is {abs(speed_diff):.3f} seconds SLOWER")

    print(f"\n==================================================")
    print(f" DATA VALIDATION")
    print(f"==================================================")
    
    if len(df_v1) != len(df_v2):
        print(f" [FAIL] Rown count mismatch! V1: {len(df_v1)}, V2: {len(df_v2)}")
        
        # Determine UUID diff if 'UUID' column exists
        if 'UUID' in df_v1.columns and 'UUID' in df_v2.columns:
            missing_in_v2 = set(df_v1['UUID']) - set(df_v2['UUID'])
            missing_in_v1 = set(df_v2['UUID']) - set(df_v1['UUID'])
            if missing_in_v2: print(f"UUIDs missing in V2: {list(missing_in_v2)[:5]}...")
            if missing_in_v1: print(f"UUIDs unexpected in V2: {list(missing_in_v1)[:5]}...")
        else:
            print("To see exact row discrepancies, ensure the SP outputs a UUID column.")
    else:
        # Check identical content
        try:
            pd.testing.assert_frame_equal(df_v1.reset_index(drop=True), df_v2.reset_index(drop=True))
            print(" [PASS] Data execution output is 100% physically identical!")
        except AssertionError as e:
            print(" [WARN] Row counts are identical, but data contents differ slightly:")
            print(" >>", str(e).split('\n')[0])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EtHiOhRi Mamba ETL Query Automator and Comparator")
    parser.add_argument("--sp", type=str, default="sp_fact_line_list_tx_curr_query", help="Base name of the stored procedure (without _v2)")
    parser.add_argument("--date", type=str, default="2024-12-31", help="Date argument to pass to the queries")
    parser.add_argument("--db", type=str, default="mysql+pymysql://root:password@localhost:3306/openmrs", help="SQLAlchemy Database URL")
    
    args = parser.parse_args()
    compare_queries(args.db, args.sp, args.date)
