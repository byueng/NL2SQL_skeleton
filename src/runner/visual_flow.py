# -*- coding: utf-8 -*-
# @Time    : 2025-07-28 15:36
# @Author  : jwm
# @File    : visible_flow.py
# @description: Visualize progress

import matplotlib.pyplot as plt
from tqdm import tqdm

class Visual:
    def __init__(self, total_steps, update_interval=100):
        """
        初始化训练进度可视化工具。

        :param total_steps: 总训练步数（数据量）
        :param update_interval: 更新间隔（每处理多少数据后更新一次）
        :param title: 图表标题
        :param figsize: 图表尺寸 (宽, 高)
        """
        self.total_steps = total_steps
        self.update_interval = update_interval

        # 初始化进度条
        self.progress_bar = tqdm(total=total_steps, desc="Training Progress")
       

    def update(self, step):
        """
        更新训练进度和图表。

        """
        # 更新进度条
        self.progress_bar.update(step - self.progress_bar.n)

    def close(self):
        """
        关闭进度条和图表。
        """
        self.progress_bar.close()
        plt.ioff()  # 关闭交互模式
        plt.show()