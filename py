import pandas as pd

class EnhancedDataFrameComparator:
    def __init__(self, df1, df2, key_cols=None, value_cols=None, drop_duplicates=False, drop_cols=None):
        """
        Initialize DataFrame comparison tool with multi-dimensional comparison features
        :param df1: First DataFrame to compare
        :param df2: Second DataFrame to compare
        :param key_cols: List of columns as composite key (first 3 columns by default)
        :param value_cols: List of numeric columns to compare (last 2 columns by default)
        :param drop_duplicates: Whether to remove duplicate rows (False by default)
        :param drop_cols: Columns to remove before comparison (empty by default)
        """
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        
        # 1. Remove specified columns
        self.drop_cols = drop_cols if drop_cols is not None else []
        self._remove_unwanted_cols()
        
        # 2. Determine key columns and value columns
        self.key_cols = key_cols if key_cols else self.df1.columns[:3].tolist()
        self.value_cols = value_cols if value_cols else self.df1.columns[3:5].tolist()
        
        # 3. Round value columns to 2 decimal places
        self._round_value_cols()
        
        # 4. Align column order
        self._align_columns()
        
        # 5. Remove duplicates (optional)
        if drop_duplicates:
            self._remove_duplicates()
        
        # 6. Validate column existence
        self._validate_columns()

    def _remove_unwanted_cols(self):
        """Remove specified unwanted columns from both DataFrames"""
        cols_to_drop1 = [col for col in self.drop_cols if col in self.df1.columns]
        cols_to_drop2 = [col for col in self.drop_cols if col in self.df2.columns]
        self.df1 = self.df1.drop(columns=cols_to_drop1, errors='ignore')
        self.df2 = self.df2.drop(columns=cols_to_drop2, errors='ignore')

    def _round_value_cols(self):
        """Round numeric value columns to 2 decimal places (handle floating-point precision issues)"""
        for col in self.value_cols:
            # Ensure the column is numeric type; skip non-numeric types
            if pd.api.types.is_numeric_dtype(self.df1[col]):
                self.df1[col] = self.df1[col].round(2)
            if pd.api.types.is_numeric_dtype(self.df2[col]):
                self.df2[col] = self.df2[col].round(2)

    def _align_columns(self):
        """Unify column order of both DataFrames"""
        target_columns = self.key_cols + self.value_cols
        self.df1 = self.df1.reindex(columns=target_columns)
        self.df2 = self.df2.reindex(columns=target_columns)

    def _remove_duplicates(self):
        """Remove duplicate rows based on key columns and value columns"""
        dup_cols = self.key_cols + self.value_cols
        self.df1 = self.df1.drop_duplicates(subset=dup_cols, keep='first')
        self.df2 = self.df2.drop_duplicates(subset=dup_cols, keep='first')

    def _validate_columns(self):
        """Validate that all required columns exist in both DataFrames"""
        for col in self.key_cols + self.value_cols:
            if col not in self.df1.columns or col not in self.df2.columns:
                raise ValueError(f"Column '{col}' does not exist in one of the DataFrames")

    def get_unique_rows(self):
        """Get rows unique to each DataFrame based on key columns"""
        self.df1['_key_tuple'] = self.df1[self.key_cols].apply(tuple, axis=1)
        self.df2['_key_tuple'] = self.df2[self.key_cols].apply(tuple, axis=1)
        
        df1_unique = self.df1[~self.df1['_key_tuple'].isin(self.df2['_key_tuple'])].drop('_key_tuple', axis=1)
        df2_unique = self.df2[~self.df2['_key_tuple'].isin(self.df1['_key_tuple'])].drop('_key_tuple', axis=1)
        return df1_unique, df2_unique

    def get_intersection_rows(self):
        """Get rows in intersection with differences and without differences"""
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
        """Execute full comparison and return four result DataFrames"""
        df1_unique, df2_unique = self.get_unique_rows()
        inter_with_diff, inter_no_diff = self.get_intersection_rows()
        return df1_unique, df2_unique, inter_with_diff, inter_no_diff

    def check_row_count_consistency(self):
        """
        Check if the number of rows in both DataFrames is consistent
        :return: Tuple (is_consistent, df1_row_count, df2_row_count)
        """
        df1_rows = len(self.df1)
        df2_rows = len(self.df2)
        is_consistent = df1_rows == df2_rows
        return is_consistent, df1_rows, df2_rows

    def get_value_columns_sum(self):
        """Calculate sum of specified value columns in both DataFrames, return as dictionary {column_name: (df1_sum, df2_sum)}"""
        sum_dict = {}
        for col in self.value_cols:
            df1_sum = self.df1[col].sum() if pd.api.types.is_numeric_dtype(self.df1[col]) else 0
            df2_sum = self.df2[col].sum() if pd.api.types.is_numeric_dtype(self.df2[col]) else 0
            sum_dict[col] = (round(df1_sum, 2), round(df2_sum, 2))
        return sum_dict

    def compare_value_columns_sum(self):
        """
        Compare differences in sums of value columns between two DataFrames
        :return: Dictionary with structure: {
            'has_diff': boolean (whether there are differences),
            'details': {column_name: (df1_sum, df2_sum, difference)}  # Only includes columns with differences
        }
        """
        sum_dict = self.get_value_columns_sum()  # Reuse sum calculation results
        diff_details = {}
        
        # Iterate through all value columns to calculate differences
        for col, (sum1, sum2) in sum_dict.items():
            if not pd.api.types.is_numeric_dtype(sum1) or not pd.api.types.is_numeric_dtype(sum2):
                continue  # Skip non-numeric types
            diff = round(sum1 - sum2, 2)  # Calculate difference (keep 2 decimal places)
            if diff != 0:
                diff_details[col] = (sum1, sum2, diff)
        
        # Check if there are any differences overall
        has_diff = len(diff_details) > 0
        
        return {
            'has_diff': has_diff,
            'details': diff_details
        }

    def print_sum_comparison(self):
        """Print comparison results of value columns' sums (enhanced version: show all matched columns when consistent)"""
        sum_dict = self.get_value_columns_sum()  # Get sums of all value columns
        diff_result = self.compare_value_columns_sum()  # Get difference judgment results
        
        if diff_result['has_diff']:
            # When there are differences: show columns with differences
            print("There are differences in the sum of value columns:")
            for col, (sum1, sum2, diff) in diff_result['details'].items():
                print(f"Column {col}: df1 sum = {sum1}, df2 sum = {sum2}, difference = {diff}")
        else:
            # When completely consistent: show sums of all value columns (distinguish df1 and df2)
            print("The sums of all value columns are completely consistent. Detailed sums are as follows:")
            print("-" * 60)
            print(f"{'Column Name':<15} | {'df1 Sum':<15} | {'df2 Sum':<15}")
            print("-" * 60)
            for col, (sum1, sum2) in sum_dict.items():
                print(f"{col:<15} | {sum1:<15} | {sum2:<15}")
            print("-" * 60)
