# 示例数据（含浮点值，需保留两位小数）
data1 = {'C': [1, 2], 'D': ['a', 'b'], 'E': [3, 4], 'F': [10.123, 20.456], 'G': [100.789, 200.123]}
data2 = {'C': [2, 3], 'D': ['b', 'c'], 'E': [4, 5], 'F': [20.454, 30.678], 'G': [200.125, 300.456]}
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

# 初始化工具（自动处理数值列保留两位小数）
comparator = EnhancedDataFrameComparator(df1, df2, key_cols=['C', 'D', 'E'], value_cols=['F', 'G'])
df1_unique, df2_unique, inter_diff, inter_no_diff = comparator.run_comparison()

# 输出结果中，F和G列会显示两位小数，差异判断更准确
print("交集中无差异的行：\n", inter_no_diff)
