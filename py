import pandas as pd
import base64
import hashlib

class ComparisonResultSplitter:
    """Split comparison results and generate Base64 unique identifiers (compare_key)"""
    
    def __init__(self, df1_unique, df2_unique, inter_with_diff, inter_no_diff, key_cols, value_cols):
        # Create deep copies of input data during initialization to avoid affecting original data
        self.df1_unique = df1_unique.copy(deep=True)
        self.df2_unique = df2_unique.copy(deep=True)
        self.inter_with_diff = inter_with_diff.copy(deep=True)
        self.inter_no_diff = inter_no_diff.copy(deep=True)
        self.key_cols = key_cols
        self.value_cols = value_cols  # Columns used for value comparison/calculation
        self.all_original_cols = key_cols + value_cols
        self.split_df = None
        # Define universal column order (applied to final combined DataFrame)
        self._column_order = [
            'compare_key', 
            'compare_status'
        ] + self.value_cols + self.key_cols + ['resource']

    def _generate_compare_key(self, row):
        """Generate Base64-encoded unique key based on key_cols"""
        key_str = '|'.join([str(row[col]) if pd.notna(row[col]) else '' for col in self.key_cols])
        hash_bytes = hashlib.md5(key_str.encode('utf-8')).digest()
        return base64.b64encode(hash_bytes).decode('utf-8')

    def _split_intersection_rows(self):
        split_match, split_change = [], []
        
        # Process match rows
        if not self.inter_no_diff.empty:
            # Extract match rows from DF1 (copy first, then rename columns)
            df1_match = self.inter_no_diff[
                self.key_cols + [f'{col}_df1' for col in self.value_cols]
            ].copy(deep=True)
            df1_match.columns = self.all_original_cols
            
            # Batch add columns with loc to avoid multiple chained assignments
            new_cols = {
                'resource': 'DF1',
                'compare_status': 'match',
                'compare_key': df1_match.apply(self._generate_compare_key, axis=1)
            }
            for col, val in new_cols.items():
                df1_match.loc[:, col] = val  # Explicitly target all rows and the current column
            
            # Extract match rows from DF2 (same processing)
            df2_match = self.inter_no_diff[
                self.key_cols + [f'{col}_df2' for col in self.value_cols]
            ].copy(deep=True)
            df2_match.columns = self.all_original_cols
            new_cols['resource'] = 'DF2'
            for col, val in new_cols.items():
                df2_match.loc[:, col] = val
            
            split_match = [df1_match, df2_match]

        # Process change rows
        if not self.inter_with_diff.empty:
            # Extract change rows from DF1
            df1_change = self.inter_with_diff[
                self.key_cols + [f'{col}_df1' for col in self.value_cols]
            ].copy(deep=True)
            df1_change.columns = self.all_original_cols
            new_cols = {
                'resource': 'DF1',
                'compare_status': 'change',
                'compare_key': df1_change.apply(self._generate_compare_key, axis=1)
            }
            for col, val in new_cols.items():
                df1_change.loc[:, col] = val
            
            # Extract change rows from DF2
            df2_change = self.inter_with_diff[
                self.key_cols + [f'{col}_df2' for col in self.value_cols]
            ].copy(deep=True)
            df2_change.columns = self.all_original_cols
            new_cols['resource'] = 'DF2'
            for col, val in new_cols.items():
                df2_change.loc[:, col] = val
            
            split_change = [df1_change, df2_change]

        return split_match, split_change

    def _process_unique_rows(self):
        unique_rows = []
        
        # Process unique rows from DF1 (remove status)
        if not self.df1_unique.empty:
            df1_unique = self.df1_unique.copy(deep=True)
            new_cols = {
                'resource': 'DF1',
                'compare_status': 'remove',
                'compare_key': df1_unique.apply(self._generate_compare_key, axis=1)
            }
            for col, val in new_cols.items():
                df1_unique.loc[:, col] = val  # Explicit assignment
            unique_rows.append(df1_unique)
        
        # Process unique rows from DF2 (add status)
        if not self.df2_unique.empty:
            df2_unique = self.df2_unique.copy(deep=True)
            new_cols = {
                'resource': 'DF2',
                'compare_status': 'add',
                'compare_key': df2_unique.apply(self._generate_compare_key, axis=1)
            }
            for col, val in new_cols.items():
                df2_unique.loc[:, col] = val  # Explicit assignment
            unique_rows.append(df2_unique)

        return unique_rows

    def split_and_combine(self):
        split_match, split_change = self._split_intersection_rows()
        unique_rows = self._process_unique_rows()
        
        all_parts = split_match + split_change + unique_rows
        
        if all_parts:
            self.split_df = pd.concat(all_parts, ignore_index=True)
            # Reorder columns using the universal column order
            self.split_df = self.split_df.reindex(columns=self._column_order)
        else:
            # Define empty DataFrame with the universal column order
            self.split_df = pd.DataFrame(columns=self._column_order)
        
        return self.split_df


# Usage example
if __name__ == "__main__":
    df1_unique = pd.DataFrame({'id': [1], 'name': ['A'], 'score': [85.0], 'count': [2]})
    df2_unique = pd.DataFrame({'id': [5], 'name': ['E'], 'score': [80.0], 'count': [4]})
    inter_no_diff = pd.DataFrame({
        'id': [2], 'name': ['B'],
        'score_df1': [90.0], 'count_df1': [5],
        'score_df2': [90.0], 'count_df2': [5]
    })
    inter_with_diff = pd.DataFrame({
        'id': [3], 'name': ['C'],
        'score_df1': [70.5], 'count_df1': [3],
        'score_df2': [72.5], 'count_df2': [3]
    })

    splitter = ComparisonResultSplitter(
        df1_unique=df1_unique, df2_unique=df2_unique,
        inter_with_diff=inter_with_diff, inter_no_diff=inter_no_diff,
        key_cols=['id', 'name'], value_cols=['score', 'count']
    )
    result = splitter.split_and_combine()
    print("=== Split Result with Ordered Columns ===")
    print(result)
