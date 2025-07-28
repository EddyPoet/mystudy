import os
import csv
from pathlib import Path
from typing import List, Tuple


class CSVFileComparator:
    def __init__(self, dir1: str, dir2: str):
        self.dir1 = Path(dir1).resolve()
        self.dir2 = Path(dir2).resolve()
        self.inconsistent_files = []

    def _read_csv_as_sorted(self, file_path: Path) -> tuple:
        """
        读取CSV并处理为可对比的格式：
        - 列名排序后作为元组（确保列顺序不影响）
        - 每行数据按列名排序后转为元组，再整体排序（确保行顺序不影响）
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)  # 读取所有行（首行为列名）
                if not rows:
                    return ()  # 空文件
                
                # 提取列名并排序（确保列顺序不影响）
                headers = rows[0]
                sorted_headers = sorted(headers)
                header_index = {h: i for i, h in enumerate(headers)}  # 列名→索引映射
                
                # 处理数据行：按排序后的列名提取值，再转为元组
                data_rows = []
                for row in rows[1:]:  # 跳过首行
                    # 按排序后的列名取对应值（确保列顺序一致）
                    sorted_row = tuple(row[header_index[h]] for h in sorted_headers)
                    data_rows.append(sorted_row)
                
                # 对数据行排序（确保行顺序不影响）
                data_rows.sort()
                
                # 最终结果：(排序后的列名元组, 排序后的数据行元组)
                return (tuple(sorted_headers), tuple(data_rows))
        
        except Exception as e:
            print(f"读取文件 {file_path} 失败: {e}")
            return ()

    def compare(self) -> List[Tuple[Path, Path]]:
        self.inconsistent_files = []
        
        for csv_path in self.dir1.rglob('*.csv'):
            rel_path = csv_path.relative_to(self.dir1)
            counterpart = self.dir2 / rel_path

            if not counterpart.exists() or not counterpart.is_file():
                print(f"文件 {counterpart} 不存在，跳过对比")
                continue

            # 读取处理后的内容（忽略列名顺序和行顺序）
            content1 = self._read_csv_as_sorted(csv_path)
            content2 = self._read_csv_as_sorted(counterpart)

            # 对比处理后的内容
            if content1 != content2:
                self.inconsistent_files.append((csv_path, counterpart))

        return self.inconsistent_files

    def print_results(self) -> None:
        if not self.inconsistent_files:
            print("所有对应CSV文件内容一致（忽略列名顺序和行顺序）")
            return

        print(f"共发现 {len(self.inconsistent_files)} 对内容不一致的文件：")
        for i, (path1, path2) in enumerate(self.inconsistent_files, 1):
            print(f"\n{i}.")
            print(f"文件1：{path1}")
            print(f"文件2：{path2}")

    def save_results_to_file(self, output_file: str = "compare_result.txt") -> None:
        with open(output_file, 'w', encoding='utf-8') as f:
            if not self.inconsistent_files:
                f.write("所有对应CSV文件内容一致（忽略列名顺序和行顺序）")
                return

            f.write(f"共发现 {len(self.inconsistent_files)} 对内容不一致的文件：\n\n")
            for i, (path1, path2) in enumerate(self.inconsistent_files, 1):
                f.write(f"{i}.\n")
                f.write(f"文件1：{path1}\n")
                f.write(f"文件2：{path2}\n\n")
        print(f"对比结果已保存到 {output_file}")


# 使用示例
if __name__ == "__main__":
    folder1 = "path/to/first/folder"
    folder2 = "path/to/second/folder"

    comparator = CSVFileComparator(folder1, folder2)
    comparator.compare()
    comparator.print_results()
    comparator.save_results_to_file()
