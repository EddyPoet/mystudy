import pandas as pd

# ------------------------------
# 1. 测试数据（四类结果均有数据，且行数不同）
# ------------------------------
data1 = {
    # 键列：1（df1独有）、2（交集无差异）、3（交集有差异）、4（df1独有）
    'id': [1, 2, 3, 4],
    'name': ['A', 'B', 'C', 'D'],
    # 值列：3号行与df2有差异
    'score': [85.0, 90.0, 70.5, 65.0],
    'count': [2, 5, 3, 1]
}

data2 = {
    # 键列：2（交集无差异）、3（交集有差异）、5（df2独有）
    'id': [2, 3, 5],
    'name': ['B', 'C', 'E'],
    # 值列：3号行score与df1不同
    'score': [90.0, 72.5, 80.0],
    'count': [5, 3, 4]
}

# 转换为DataFrame（df1有4行，df2有3行，行数不同）
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)


# ------------------------------
# 2. 定义对比参数
# ------------------------------
key_columns = ['id', 'name']
value_columns = ['score', 'count']
remove_duplicates = False  # 不启用去重（本示例无重复行）
drop_columns = []


# ------------------------------
# 3. 执行对比并打印结果
# ------------------------------
try:
    comparator = EnhancedDataFrameComparator(
        df1=df1, df2=df2,
        key_cols=key_columns,
        value_cols=value_columns,
        drop_duplicates=remove_duplicates,
        drop_cols=drop_columns
    )

    # 1. 行数检查（预期不一致）
    print("=== 1. Row count check ===")
    consistent, r1, r2 = comparator.check_row_count_consistency()
    print(f"df1 rows: {r1}, df2 rows: {r2} (consistent: {'Yes' if consistent else 'No'})\n")

    # 2. 值列总和对比（预期有差异）
    print("=== 2. Value column sum comparison ===")
    comparator.print_sum_comparison()
    print()

    # 3. 行级结果（四类DataFrame均有数据）
    print("=== 3. Row-level results (all 4 DataFrames have data) ===")
    df1_unique, df2_unique, inter_with_diff, inter_no_diff = comparator.run_comparison()

    print("Rows unique to df1:\n", df1_unique)
    print("\nRows unique to df2:\n", df2_unique)
    print("\nIntersection rows with differences:\n", inter_with_diff)
    print("\nIntersection rows without differences:\n", inter_no_diff)

except ValueError as e:
    print(f"Comparison failed: {e}")

