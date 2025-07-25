import pandas as pd

# ------------------------------
# 1. 调整测试数据（确保四类结果都有数据）
# ------------------------------
data1 = {
    # 键列
    'id': [1, 2, 3, 4],  # 1:df1独有, 2:交集无差异, 3:交集有差异, 4:重复行
    'name': ['A', 'B', 'C', 'D']
    # 值列
    'score': [80.0, 90.5, 70.3, 60.0],  # 3号行值与df2不同
    'count': [2, 5, 3, 1]
}

data2 = {
    # 键列
    'id': [2, 3, 5, 4],  # 2:交集无差异, 3:交集有差异, 5:df2独有, 4:重复行
    'name': ['B', 'C', 'E', 'D']
    # 值列（3号行与df1有差异，4号行与df1无差异）
    'score': [90.5, 75.3, 85.0, 60.0],
    'count': [5, 3, 4, 1]
}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)


# ------------------------------
# 2. 定义参数
# ------------------------------
key_columns = ['id', 'name']
value_columns = ['score', 'count']
remove_duplicates = True  # 保留重复行中的第一行
drop_columns = []


# ------------------------------
# 3. 执行对比
# ------------------------------
try:
    comparator = EnhancedDataFrameComparator(
        df1=df1, df2=df2,
        key_cols=key_columns,
        value_cols=value_columns,
        drop_duplicates=remove_duplicates,
        drop_cols=drop_columns
    )

    # 打印结果
    print("=== 1. 行数检查 ===")
    consistent, r1, r2 = comparator.check_row_count_consistency()
    print(f"df1 rows: {r1}, df2 rows: {r2} (consistent: {'Yes' if consistent else 'No'})\n")

    print("=== 2. 值列总和对比 ===")
    comparator.print_sum_comparison()
    print()

    print("=== 3. 行级结果（四类都有数据） ===")
    df1_unique, df2_unique, inter_with_diff, inter_no_diff = comparator.run_comparison()
    
    print("df1独有行:\n", df1_unique)
    print("\ndf2独有行:\n", df2_unique)
    print("\n有差异的交集行:\n", inter_with_diff)
    print("\n无差异的交集行:\n", inter_no_diff)

except ValueError as e:
    print(f"Comparison failed: {e}")
