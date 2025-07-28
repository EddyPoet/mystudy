import os
import pandas as pd
from pathlib import Path
from typing import List, Tuple


class CSVFileComparator:
    def __init__(self, dir1: str, dir2: str):
        self.dir1 = Path(dir1).resolve()
        self.dir2 = Path(dir2).resolve()
        self.inconsistent_files = []  # 不匹配的文件对
        self.consistent_files = []    # 匹配的文件对
        self.mismatch_details = pd.DataFrame()  # 不匹配的详细对比结果

    def _read_csv_as_df(self, file_path: Path) -> pd.DataFrame:
        """读取CSV为DataFrame，统一列名排序"""
        try:
            df = pd.read_csv(file_path)
            # 按列名排序（忽略列顺序影响）
            return df.reindex(sorted(df.columns), axis=1)
        except Exception as e:
            print(f"读取文件 {file_path} 失败: {e}")
            return pd.DataFrame()

    def compare(self) -> None:
        """执行基础对比，区分匹配/不匹配文件"""
        self.inconsistent_files = []
        self.consistent_files = []
        
        for csv_path in self.dir1.rglob('*.csv'):
            rel_path = csv_path.relative_to(self.dir1)
            counterpart = self.dir2 / rel_path

            if not counterpart.exists() or not counterpart.is_file():
                print(f"文件 {counterpart} 不存在，跳过对比")
                continue

            # 读取并标准化列顺序
            df1 = self._read_csv_as_df(csv_path)
            df2 = self._read_csv_as_df(counterpart)

            # 基础对比（忽略列顺序和行顺序）
            if df1.equals(df2.sort_values(by=list(df1.columns)).reset_index(drop=True)):
                self.consistent_files.append((csv_path, counterpart))
            else:
                self.inconsistent_files.append((csv_path, counterpart))

    def analyze_mismatches(self) -> pd.DataFrame:
        """分析不匹配文件的详细差异，生成汇总DataFrame"""
        all_details = []
        
        for idx, (path1, path2) in enumerate(self.inconsistent_files, 1):
            # 读取两个文件的DataFrame
            df1 = self._read_csv_as_df(path1).assign(source=os.path.basename(path1))
            df2 = self._read_csv_as_df(path2).assign(source=os.path.basename(path2))
            
            # 合并数据（按所有列进行匹配）
            merged = pd.merge(
                df1, df2, 
                on=list(df1.columns[:-1]),  # 排除source列
                how='outer',
                indicator=True
            )
            
            # 标记匹配状态
            merged['match_status'] = merged['_merge'].map({
                'both': 'match',
                'left_only': 'only_in_file1',
                'right_only': 'only_in_file2'
            })
            
            # 整理列名和排序
            merged = merged.rename(columns={
                'source_x': 'source', 
                'source_y': None
            }).drop(columns=['_merge']).fillna('')
            
            # 添加文件对标识
            merged['file_pair'] = f"pair_{idx}: {os.path.basename(path1)} vs {os.path.basename(path2)}"
            
            all_details.append(merged)

        # 合并所有结果并排序
        if all_details:
            self.mismatch_details = pd.concat(all_details, ignore_index=True)
            # 按文件对和匹配状态排序
            self.mismatch_details = self.mismatch_details.sort_values(
                by=['file_pair', 'match_status']
            ).reset_index(drop=True)
        
        return self.mismatch_details

    def print_results(self) -> None:
        """打印基础对比结果"""
        print(f"共发现 {len(self.consistent_files)} 对匹配文件，{len(self.inconsistent_files)} 对不匹配文件")
        if self.inconsistent_files:
            print("\n不匹配文件对：")
            for i, (p1, p2) in enumerate(self.inconsistent_files, 1):
                print(f"{i}. {p1} vs {p2}")

    def save_mismatch_details(self, output_file: str = "mismatch_details.csv") -> None:
        """保存不匹配的详细对比结果到CSV"""
        if self.mismatch_details.empty:
            print("没有不匹配的文件需要保存详细结果")
            return
        
        self.mismatch_details.to_csv(output_file, index=False)
        print(f"\n不匹配文件的详细对比结果已保存到：{output_file}")


# 使用示例
if __name__ == "__main__":
    folder1 = "path/to/first/folder"
    folder2 = "path/to/second/folder"

    comparator = CSVFileComparator(folder1, folder2)
    comparator.compare()  # 基础对比
    comparator.print_results()  # 打印匹配/不匹配文件列表
    
    # 分析不匹配的详细差异
    mismatch_df = comparator.analyze_mismatches()
    if not mismatch_df.empty:
        print("\n不匹配文件的详细对比结果（前5行）：")
        print(mismatch_df.head().to_string())
    
    comparator.save_mismatch_details()  # 保存详细结果到CSV
