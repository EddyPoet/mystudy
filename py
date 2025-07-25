import pandas as pd
import base64
import hashlib

class ComparisonResultSplitter:
    """Split comparison results and generate Base64 unique identifiers (compare_key)"""
    
    def __init__(self, df1_unique, df2_unique, inter_with_diff, inter_no_diff, key_cols, value_cols):
        self.df1_unique = df1_unique.copy()
        self.df2_unique = df2_unique.copy()
        self.inter_with_diff = inter_with_diff.copy()
        self.inter_no_diff = inter_no_diff.copy()
        self.key_cols = key_cols  # Columns for generating compare_key
        self.value_cols = value_cols
        self.all_original_cols = key_cols + value_cols
        self.split_df = None

    def _generate_compare_key(self, row):
        """Generate Base64-encoded unique key based on key_cols"""
        # Concatenate key column values (handle NaN)
        key_str = '|'.join([str(row[col]) if pd.notna(row[col]) else '' for col in self.key_cols])
        # MD5 hash + Base64 encoding
        hash_bytes = hashlib.md5(key_str.encode('utf-8')).digest()
        return base64.b64encode(hash_bytes).decode('utf-8')

    def _split_intersection_rows(self):
        """Split intersection rows (match/change) into two rows with compare_key"""
        split_match, split_change = [], []
        
        # Process match rows
        if not self.inter_no_diff.empty:
            # DF1 match rows
            df1_match = self.inter_no_diff[self.key_cols + [f'{col}_df1' for col in self.value_cols]]
            df1_match.columns = self.all_original_cols
            df1_match['resource'] = 'DF1'
            df1_match['compare_status'] = 'match'
            df1_match['compare_key'] = df1_match.apply(self._generate_compare_key, axis=1)
            
            # DF2 match rows
            df2_match = self.inter_no_diff[self.key_cols + [f'{col}_df2' for col in self.value_cols]]
            df2_match.columns = self.all_original_cols
            df2_match['resource'] = 'DF2'
            df2_match['compare_status'] = 'match'
            df2_match['compare_key'] = df2_match.apply(self._generate_compare_key, axis=1)
            
            split_match = [df1_match, df2_match]

        # Process change rows
        if not self.inter_with_diff.empty:
            # DF1 change rows
            df1_change = self.inter_with_diff[self.key_cols + [f'{col}_df1' for col in self.value_cols]]
            df1_change.columns = self.all_original_cols
            df1_change['resource'] = 'DF1'
            df1_change['compare_status'] = 'change'
            df1_change['compare_key'] = df1_change.apply(self._generate_compare_key, axis=1)
            
            # DF2 change rows
            df2_change = self.inter_with_diff[self.key_cols + [f'{col}_df2' for col in self.value_cols]]
            df2_change.columns = self.all_original_cols
            df2_change['resource'] = 'DF2'
            df2_change['compare_status'] = 'change'
            df2_change['compare_key'] = df2_change.apply(self._generate_compare_key, axis=1)
            
            split_change = [df1_change, df2_change]

        return split_match, split_change

    def _process_unique_rows(self):
        """Process unique rows (add/remove) with compare_key"""
        unique_rows = []
        
        # DF1 unique rows (remove status)
        if not self.df1_unique.empty:
            self.df1_unique['resource'] = 'DF1'
            self.df1_unique['compare_status'] = 'remove'
            self.df1_unique['compare_key'] = self.df1_unique.apply(self._generate_compare_key, axis=1)
            unique_rows.append(self.df1_unique[self.all_original_cols + ['resource', 'compare_status', 'compare_key']])
        
        # DF2 unique rows (add status)
        if not self.df2_unique.empty:
            self.df2_unique['resource'] = 'DF2'
            self.df2_unique['compare_status'] = 'add'
            self.df2_unique['compare_key'] = self.df2_unique.apply(self._generate_compare_key, axis=1)
            unique_rows.append(self.df2_unique[self.all_original_cols + ['resource', 'compare_status', 'compare_key']])

        return unique_rows

    def split_and_combine(self):
        """Combine all split results with compare_key"""
        split_match, split_change = self._split_intersection_rows()
        unique_rows = self._process_unique_rows()
        
        all_parts = split_match + split_change + unique_rows
        
        if all_parts:
            self.split_df = pd.concat(all_parts, ignore_index=True)
            self.split_df = self.split_df[['compare_key'] + self.all_original_cols + ['resource', 'compare_status']]
        else:
            self.split_df = pd.DataFrame(columns=['compare_key'] + self.all_original_cols + ['resource', 'compare_status'])
        
        return self.split_df


# ------------------------------
# Usage Example
# ------------------------------
if __name__ == "__main__":
    # Simulate comparison results
    df1_unique = pd.DataFrame({
        'id': [1], 'name': ['A'], 'score': [85.0], 'count': [2]
    })

    df2_unique = pd.DataFrame({
        'id': [5], 'name': ['E'], 'score': [80.0], 'count': [4]
    })

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

    # Generate result
    splitter = ComparisonResultSplitter(
        df1_unique=df1_unique, df2_unique=df2_unique,
        inter_with_diff=inter_with_diff, inter_no_diff=inter_no_diff,
        key_cols=['id', 'name'], value_cols=['score', 'count']
    )
    result = splitter.split_and_combine()
    print("=== Split Result with Unique compare_key ===")
    print(result)
