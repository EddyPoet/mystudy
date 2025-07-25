# 示例数据（含多余列A、B，列顺序不同，有重复行）
data1 = {
    'A': [10, 20],  # 需要移除的列
    'B': ['x', 'y'],  # 需要移除的列
    'C': [1, 2],    # 键列1
    'D': ['a', 'b'],  # 键列2
    'E': [True, False],  # 键列3
    'F': [100, 200],  # 值列1
    'G': [1000, 2000]   # 值列2
}
data2 = {
    'B': ['y', 'y', 'z'],
    'A': [20, 20, 30],
    'D': ['b', 'b', 'c'],
    'C': [2, 2, 3],
    'E': [False, False, True],
    'F': [200, 250, 300],
    'G': [2000, 2000, 3000]
}
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

# 初始化工具（指定要移除的列A、B，开启去重）
comparator = EnhancedDataFrameComparator(
    df1, df2,
    key_cols=['C', 'D', 'E'],  # 明确指定键列（移除A、B后）
    value_cols=['F', 'G'],     # 明确指定值列
    drop_cols=['A', 'B'],      # 移除多余列
    drop_duplicates=True
)

# 执行对比
df1_unique, df2_unique, inter_diff, inter_no_diff = comparator.run_comparison()

# 输出结果（所有结果中均已无列A、B）
print("df1独有行：\n", df1_unique)
print("\ndf2独有行：\n", df2_unique)
print("\n交集有差异行：\n", inter_diff)
print("\n交集无差异行：\n", inter_no_diff)
