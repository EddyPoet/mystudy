import os
import csv
from pathlib import Path
from typing import List, Tuple


class CSVFileComparator:
    def __init__(self, dir1: str, dir2: str):
        self.dir1 = Path(dir1).resolve()
        self.dir2 = Path(dir2).resolve()
        self.inconsistent_files = []  # 内容不一致的文件对
        self.consistent_files = []    # 内容一致的文件对

    def _read_csv_as_sorted(self, file_path: Path) -> tuple:
        """读取CSV并处理为可对比的格式（忽略列名顺序和行顺序）"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                if not rows:
                    return ()
                
                headers = rows[0]
                sorted_headers = sorted(headers)
                header_index = {h: i for i, h in enumerate(headers)}
                
                data_rows = []
                for row in rows[1:]:
                    sorted_row = tuple(row[header_index[h]] for h in sorted_headers)
                    data_rows.append(sorted_row)
                data_rows.sort()
                
                return (tuple(sorted_headers), tuple(data_rows))
        
        except Exception as e:
            print(f"读取文件 {file_path} 失败: {e}")
            return ()

    def compare(self) -> None:
        """执行对比，同时记录一致和不一致的文件"""
        self.inconsistent_files = []
        self.consistent_files = []
        
        for csv_path in self.dir1.rglob('*.csv'):
            rel_path = csv_path.relative_to(self.dir1)
            counterpart = self.dir2 / rel_path

            if not counterpart.exists() or not counterpart.is_file():
                print(f"文件 {counterpart} 不存在，跳过对比")
                continue

            content1 = self._read_csv_as_sorted(csv_path)
            content2 = self._read_csv_as_sorted(counterpart)

            if content1 != content2:
                self.inconsistent_files.append((csv_path, counterpart))
            else:
                self.consistent_files.append((csv_path, counterpart))

    def print_results(self) -> None:
        """打印所有结果（包括一致和不一致的文件）"""
        print(f"\n共发现 {len(self.consistent_files)} 对内容一致的文件：")
        for i, (path1, path2) in enumerate(self.consistent_files, 1):
            print(f"{i}. {path1} 与 {path2} 内容一致")

        print(f"\n共发现 {len(self.inconsistent_files)} 对内容不一致的文件：")
        for i, (path1, path2) in enumerate(self.inconsistent_files, 1):
            print(f"{i}. {path1} 与 {path2} 内容不一致")

        if not self.consistent_files and not self.inconsistent_files:
            print("未找到可对比的CSV文件")

    def save_results_to_files(self, 
                             match_file: str = "matched_files.txt", 
                             mismatch_file: str = "mismatched_files.txt") -> None:
        """
        将结果保存到两个文件：
        - 匹配成功的文件列表
        - 匹配失败的文件列表
        """
        # 保存匹配成功的文件
        with open(match_file, 'w', encoding='utf-8') as f:
            if not self.consistent_files:
                f.write("未发现内容一致的CSV文件")
            else:
                f.write(f"共发现 {len(self.consistent_files)} 对内容一致的文件：\n\n")
                for i, (path1, path2) in enumerate(self.consistent_files, 1):
                    f.write(f"{i}.\n")
                    f.write(f"文件1：{path1}\n")
                    f.write(f"文件2：{path2}\n\n")

        # 保存匹配失败的文件
        with open(mismatch_file, 'w', encoding='utf-8') as f:
            if not self.inconsistent_files:
                f.write("未发现内容不一致的CSV文件")
            else:
                f.write(f"共发现 {len(self.inconsistent_files)} 对内容不一致的文件：\n\n")
                for i, (path1, path2) in enumerate(self.inconsistent_files, 1):
                    f.write(f"{i}.\n")
                    f.write(f"文件1：{path1}\n")
                    f.write(f"文件2：{path2}\n\n")

        print(f"\n结果已保存：")
        print(f" - 内容一致的文件列表：{match_file}")
        print(f" - 内容不一致的文件列表：{mismatch_file}")


# 使用示例
if __name__ == "__main__":
    folder1 = "path/to/first/folder"
    folder2 = "path/to/second/folder"

    comparator = CSVFileComparator(folder1, folder2)
    comparator.compare()
    comparator.print_results()
    comparator.save_results_to_files()  # 分别保存到两个文件
