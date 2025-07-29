import pandas as pd
import hashlib
import base64

class ComparisonMerger:
    """Merge and compare two DataFrames using df.merge, with value columns in the same row"""
    
    def __init__(self, df1, df2, key_cols, value_cols):
        self.df1 = df1.copy(deep=True)
        self.df2 = df2.copy(deep=True)
        self.key_cols = key_cols
        self.value_cols = value_cols
        self.merged_df = None
        self._column_order = [
            'compare_key', 'compare_status'
        ] + key_cols + [f'{col}_df1' for col in value_cols] + [f'{col}_df2' for col in value_cols]

    def _generate_compare_key(self, row):
        """Generate unique compare key based on key columns"""
        key_str = '|'.join([str(row[col]) if pd.notna(row[col]) else '' for col in self.key_cols])
        hash_bytes = hashlib.md5(key_str.encode('utf-8')).digest()
        return base64.b64encode(hash_bytes).decode('utf-8')

    def _get_value_diff_status(self, row):
        """Determine if value columns have differences"""
        for col in self.value_cols:
            val1 = row[f'{col}_df1']
            val2 = row[f'{col}_df2']
            # Handle NaN comparisons (pd.isna returns True for both NaN and None)
            if pd.isna(val1) and pd.isna(val2):
                continue
            if not pd.isna(val1) and not pd.isna(val2) and val1 == val2:
                continue
            return 'change'
        return 'match'

    def merge_and_compare(self):
        # Merge DataFrames on key columns with suffixes
        self.merged_df = pd.merge(
            self.df1, self.df2,
            on=self.key_cols,
            how='outer',
            suffixes=('_df1', '_df2'),
            indicator=True
        )

        # Generate compare key
        self.merged_df['compare_key'] = self.merged_df.apply(
            self._generate_compare_key, axis=1
        )

        # Determine compare status
        status_conditions = [
            # Rows only in df1
            (self.merged_df['_merge'] == 'left_only', 'remove'),
            # Rows only in df2
            (self.merged_df['_merge'] == 'right_only', 'add'),
            # Rows in both - check values
            (self.merged_df['_merge'] == 'both', 
             self.merged_df.apply(self._get_value_diff_status, axis=1))
        ]

        # Apply conditions
        conditions, choices = zip(*status_conditions)
        self.merged_df['compare_status'] = np.select(conditions, choices, default='unknown')

        # Drop temporary merge column and reorder
        self.merged_df = self.merged_df.drop(columns=['_merge'])
        self.merged_df = self.merged_df.reindex(columns=self._column_order)

        return self.merged_df


# Usage example
if __name__ == "__main__":
    df1 = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'score': [85.0, 90.0, 70.5],
        'count': [2, 5, 3]
    })
    df2 = pd.DataFrame({
        'id': [2, 3, 5],
        'name': ['B', 'C', 'E'],
        'score': [90.0, 72.5, 80.0],
        'count': [5, 3, 4]
    })

    merger = ComparisonMerger(
        df1=df1, df2=df2,
        key_cols=['id', 'name'],
        value_cols=['score', 'count']
    )
    result = merger.merge_and_compare()
    print("=== Merged Comparison Result ===")
    print(result)
