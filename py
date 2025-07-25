import pandas as pd

# ------------------------------
# 1. Simplified test data
# ------------------------------
data1 = {
    # Key columns
    'id': [1, 2, 2, 3],
    'name': ['A', 'B', 'B', 'C'],
    # Value columns
    'score': [85.123, 90.456, 90.456, 75.789],
    'count': [3, 5, 5, 2]
}

data2 = {
    # Key columns (different order)
    'name': ['B', 'B', 'D', 'C'],
    'id': [2, 2, 4, 3],
    # Value columns (with precision differences)
    'count': [5, 5, 4, 2],
    'score': [90.457, 90.457, 60.123, 75.788]
}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)


# ------------------------------
# 2. Define parameters (simplified)
# ------------------------------
key_columns = ['id', 'name']  # Simplified to 2 key columns
value_columns = ['score', 'count']
remove_duplicates = True  # Enable deduplication
drop_columns = []  # No columns to remove


# ------------------------------
# 3. Perform comparison
# ------------------------------
try:
    comparator = EnhancedDataFrameComparator(
        df1=df1, df2=df2,
        key_cols=key_columns,
        value_cols=value_columns,
        drop_duplicates=remove_duplicates,
        drop_cols=drop_columns
    )

    # Print results
    print("=== 1. Row count check ===")
    consistent, r1, r2 = comparator.check_row_count_consistency()
    print(f"df1 rows: {r1}, df2 rows: {r2} (consistent: {'Yes' if consistent else 'No'})\n")

    print("=== 2. Value column sum comparison ===")
    comparator.print_sum_comparison()
    print()

    print("=== 3. Row-level results ===")
    u1, u2, diff, no_diff = comparator.run_comparison()
    print("Rows unique to df1:\n", u1)
    print("\nRows unique to df2:\n", u2)
    print("\nIntersection rows with differences:\n", diff)
    print("\nIntersection rows without differences:\n", no_diff)

except ValueError as e:
    print(f"Comparison failed: {e}")

