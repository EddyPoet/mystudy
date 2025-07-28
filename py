import pandas as pd
from pathlib import Path
from typing import Tuple


class CSVDataFrameComparator:
    def __init__(self, file1: str, file2: str):
        """
        初始化CSV对比工具
        :param file1: 第一个CSV文件路径
        :param file2: 第二个CSV文件路径
        """
        self.file1 = Path(file1).resolve()
        self.file2 = Path(file2).resolve()
        self.df1 = pd.DataFrame()  # 第一个文件的DataFrame
        self.df2 = pd.DataFrame()  # 第二个文件的DataFrame
        self.compare_result = pd.DataFrame()  # 最终对比结果

    def load_csv(self, encoding: str = 'utf-8') -> None:
        """加载两个CSV文件为DataFrame（自动按列名排序，确保列顺序一致）"""
        try:
            # 读取CSV并按列名排序（忽略列顺序影响）
            self.df1 = pd.read_csv(self.file1, encoding=encoding).reindex(sorted(pd.read_csv(self.file1).columns), axis=1)
            self.df2 = pd.read_csv(self.file2, encoding=encoding).reindex(sorted(pd.read_csv(self.file2).columns), axis=1)
            print(f"成功加载文件：\n- {self.file1}\n- {self.file2}")
        except Exception as e:
            print(f"加载CSV文件失败：{e}")

    def compare(self) -> None:
        """对比两个DataFrame，计算交集和差集并合并结果"""
        if self.df1.empty or self.df2.empty:
            print("请先加载有效的CSV文件（DataFrame为空）")
            return

        # 确保两个DataFrame列名一致（否则无法直接对比）
        if set(self.df1.columns) != set(self.df2.columns):
            print("警告：两个CSV文件的列名集合不一致，可能影响对比结果")

        # 合并两个DataFrame，通过_merge标记来源
        merged = pd.merge(
            self.df1.assign(source=str(self.file1)),  # 添加来源列（第一个文件）
            self.df2.assign(source=str(self.file2)),  # 添加来源列（第二个文件）
            on=list(self.df1.columns),  # 基于所有列对比
            how='outer',
            indicator=True
        )

        # 标记每行的状态（交集/仅在文件1/仅在文件2）
        merged['status'] = merged['_merge'].map({
            'both': '交集（两个文件都有）',
            'left_only': f'仅在文件1：{self.file1}',
            'right_only': f'仅在文件2：{self.file2}'
        })

        # 整理结果（删除_merge列，保留有用信息）
        self.compare_result = merged.drop(columns=['_merge']).sort_values(by='status')

    def save_result(self, output_file: str = 'csv_compare_result.csv') -> None:
        """将对比结果保存为CSV文件"""
        if self.compare_result.empty:
            print("没有可保存的对比结果（结果为空）")
            return

        try:
            self.compare_result.to_csv(output_file, index=False, encoding='utf-8')
            print(f"对比结果已保存至：{output_file}")
        except Exception as e:
            print(f"保存结果失败：{e}")

    def print_summary(self) -> None:
        """打印对比结果的摘要信息"""
        if self.compare_result.empty:
            print("暂无对比结果")
            return

        total = len(self.compare_result)
        intersection = sum(self.compare_result['status'] == '交集（两个文件都有）')
        only_file1 = sum(self.compare_result['status'].str.contains('仅在文件1'))
        only_file2 = sum(self.compare_result['status'].str.contains('仅在文件2'))

        print("\n===== 对比结果摘要 =====")
        print(f"总记录数：{total}")
        print(f"交集（两个文件都有的行）：{intersection} 行")
        print(f"仅在文件1的行：{only_file1} 行")
        print(f"仅在文件2的行：{only_file2} 行")


# 使用示例
if __name__ == "__main__":
    # 替换为实际的CSV文件路径
    csv1 = "file1.csv"
    csv2 = "file2.csv"

    comparator = CSVDataFrameComparator(csv1, csv2)
    comparator.load_csv()  # 加载文件
    comparator.compare()   # 执行对比
    comparator.print_summary()  # 打印摘要
    comparator.save_result()    # 保存结果
