import pandas as pd

class EnhancedDataFrameComparator:
    def __init__(self, df1, df2, key_cols=None, value_cols=None, drop_duplicates=False, drop_cols=None):
        """
        初始化DataFrame对比工具（支持数值列保留两位小数等功能）
        :param df1: 第一个DataFrame
        :param df2: 第二个DataFrame
        :param key_cols: 组合键列名列表（默认前3列）
        :param value_cols: 对比的数值列名列表（默认后2列）
        :param drop_duplicates: 是否去除重复行（默认False）
        :param drop_cols: 对比前需要移除的列（默认空）
        """
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        
        # 1. 移除指定列
        self.drop_cols = drop_cols if drop_cols is not None else []
        self._remove_unwanted_cols()
        
        # 2. 确定键列和值列
        self.key_cols = key_cols if key_cols else self.df1.columns[:3].tolist()
        self.value_cols = value_cols if value_cols else self.df1.columns[3:5].tolist()
        
        # 3. 数值列保留两位小数
        self._round_value_cols()
        
        # 4. 统一列顺序
        self._align_columns()
        
        # 5. 去除重复行（可选）
        if drop_duplicates:
            self._remove_duplicates()
        
        # 6. 验证列存在性
        self._validate_columns()

    def _remove_unwanted_cols(self):
        """移除指定的多余列"""
        cols_to_drop1 = [col for col in self.drop_cols if col in self.df1.columns]
        cols_to_drop2 = [col for col in self.drop_cols if col in self.df2.columns]
        self.df1 = self.df1.drop(columns=cols_to_drop1, errors='ignore')
        self.df2 = self.df2.drop(columns=cols_to_drop2, errors='ignore')

    def _round_value_cols(self):
        """将数值列保留两位小数（处理浮点精度问题）"""
        for col in self.value_cols:
            # 确保列是数值类型，非数值类型不处理
            if pd.api.types.is_numeric_dtype(self.df1[col]):
                self.df1[col] = self.df1[col].round(2)
            if pd.api.types.is_numeric_dtype(self.df2[col]):
                self.df2[col] = self.df2[col].round(2)

    def _align_columns(self):
        """统一列顺序"""
        target_columns = self.key_cols + self.value_cols
        self.df1 = self.df1.reindex(columns=target_columns)
        self.df2 = self.df2.reindex(columns=target_columns)

    def _remove_duplicates(self):
        """去除重复行"""
        dup_cols = self.key_cols + self.value_cols
        self.df1 = self.df1.drop_duplicates(subset=dup_cols, keep='first')
        self.df2 = self.df2.drop_duplicates(subset=dup_cols, keep='first')

    def _validate_columns(self):
        """验证列是否存在"""
        for col in self.key_cols + self.value_cols:
            if col not in self.df1.columns or col not in self.df2.columns:
                raise ValueError(f"列 '{col}' 在某个DataFrame中不存在")

    def get_unique_rows(self):
        """获取各自独有的行"""
        self.df1['_key_tuple'] = self.df1[self.key_cols].apply(tuple, axis=1)
        self.df2['_key_tuple'] = self.df2[self.key_cols].apply(tuple, axis=1)
        
        df1_unique = self.df1[~self.df1['_key_tuple'].isin(self.df2['_key_tuple'])].drop('_key_tuple', axis=1)
        df2_unique = self.df2[~self.df2['_key_tuple'].isin(self.df1['_key_tuple'])].drop('_key_tuple', axis=1)
        return df1_unique, df2_unique

    def get_intersection_rows(self):
        """获取交集中有差异和无差异的行"""
        merged = pd.merge(
            self.df1, self.df2,
            on=self.key_cols,
            suffixes=('_df1', '_df2'),
            how='inner'
        )
        
        for col in self.value_cols:
            merged[f'{col}_diff'] = merged[f'{col}_df1'] != merged[f'{col}_df2']
        
        merged['_has_any_diff'] = merged[[f'{col}_diff' for col in self.value_cols]].any(axis=1)
        inter_with_diff = merged[merged['_has_any_diff']].drop('_has_any_diff', axis=1)
        inter_no_diff = merged[~merged['_has_any_diff']].drop('_has_any_diff', axis=1)
        return inter_with_diff, inter_no_diff

    def run_comparison(self):
        """执行完整对比，返回四个结果"""
        df1_unique, df2_unique = self.get_unique_rows()
        inter_with_diff, inter_no_diff = self.get_intersection_rows()
        return df1_unique, df2_unique, inter_with_diff, inter_no_diff
