import pandas as pd

# ------------------------------
# 1. Test data (All four types of results have data, and the number of rows is different)
# ------------------------------
data1 = {
    # Key columns: 1 (unique to df1), 2 (intersection with no difference), 3 (intersection with difference), 4 (unique to df1)
    'id': [1, 2, 3, 4],
    'name': ['A', 'B', 'C', 'D'],
    # Value columns: The values of row 3 are different from those in df2
    'score': [85.0, 90.0, 70.5, 65.0],
    'count': [2, 5, 3, 1]
}

data2 = {
    # Key columns: 2 (intersection with no difference), 3 (intersection with difference), 5 (unique to df2)
    'id': [2, 3, 5],
    'name': ['B', 'C', 'E'],
    # Value columns: The score of row 3 is different from that in df1
    'score': [90.0, 72.5, 80.0],
    'count': [5, 3, 4]
}

# Convert to DataFrames (df1 has 4 rows, df2 has 3 rows, different number of rows)
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)


# ------------------------------
# 2. Define comparison parameters
# ------------------------------
key_columns = ['id', 'name']
value_columns = ['score', 'count']
remove_duplicates = False  # Do not enable deduplication (no duplicate rows in this example)
drop_columns = []


# ------------------------------
# 3. Perform comparison and print results
# ------------------------------
try:
    comparator = EnhancedDataFrameComparator(
        df1=df1, df2=df2,
        key_cols=key_columns,
        value_cols=value_columns,
        drop_duplicates=remove_duplicates,
        drop_cols=drop_columns
    )

    # 1. Row count check (Expected to be inconsistent)
    print("=== 1. Row count check ===")
    consistent, r1, r2 = comparator.check_row_count_consistency()
    print(f"df1 rows: {r1}, df2 rows: {r2} (consistent: {'Yes' if consistent else 'No'})\n")

    # 2. Value column sum comparison (Expected to have differences)
    print("=== 2. Value column sum comparison ===")
    comparator.print_sum_comparison()
    print()

    # 3. Row-level results (All four DataFrames have data)
    print("=== 3. Row-level results (all 4 DataFrames have data) ===")
    df1_unique, df2_unique, inter_with_diff, inter_no_diff = comparator.run_comparison()

    print("Rows unique to df1:\n", df1_unique)
    print("\nRows unique to df2:\n", df2_unique)
    print("\nIntersection rows with differences:\n", inter_with_diff)
    print("\nIntersection rows without differences:\n", inter_no_diff)

except ValueError as e:
    print(f"Comparison failed: {e}")
