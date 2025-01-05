from abc import ABC, abstractmethod
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Dict, Optional
import matplotlib.font_manager as fm
import platform
import os
import subprocess
from pathlib import Path

from database.models import ConceptName


class ChartConfig:
    """图表配置类"""

    @classmethod
    def install_font(cls):
        """安装所需的字体"""
        try:
            if platform.system() == 'Linux':
                result = subprocess.run(['fc-list', ':', 'family'],
                                        capture_output=True,
                                        text=True)
                if 'WenQuanYi' not in result.stdout:
                    print("正在安装中文字体...")
                    subprocess.run(['sudo', 'apt-get', 'update'], check=True)
                    subprocess.run(['sudo', 'apt-get', 'install', '-y',
                                    'fonts-wqy-microhei', 'fonts-wqy-zenhei'],
                                   check=True)
                    subprocess.run(['fc-cache', '-fv'], check=True)
                    print("字体安装完成")
                return True
        except Exception as e:
            print(f"字体安装失败: {e}")
            return False

    @classmethod
    def setup_font(cls):
        """设置字体配置"""
        # 首先尝试安装字体
        cls.install_font()

        # 刷新字体缓存
        fm.fontManager.addfont('/usr/share/fonts/truetype/wqy/wqy-microhei.ttc')
        try:
            # 尝试使用新版本的方法
            fm.fontManager.rebuild()
        except:
            try:
                # 尝试使用旧版本的方法
                fm._rebuild()
            except:
                print("警告：无法重建字体缓存")

        # 设置默认字体
        plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'SimHei',
                                           'Microsoft YaHei', 'Arial Unicode MS',
                                           'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False

        # 验证字体是否可用
        font_set = False
        for font in plt.rcParams['font.sans-serif']:
            try:
                if fm.findfont(font) is not None:
                    font_set = True
                    break
            except:
                continue

        if not font_set:
            print("警告：未找到可用的中文字体，将使用英文标签")
            return False
        return True


class ChartGenerator(ABC):
    """图表生成器的抽象基类"""

    def __init__(self):
        self.use_chinese = ChartConfig.setup_font()

    def get_label(self, chinese: str, english: str) -> str:
        """根据字体支持情况返回适当的标签"""
        return chinese if self.use_chinese else english


class PlotlyChartGenerator(ChartGenerator):
    """Plotly交互式图表生成器"""

    def generate(self, concept_data: Dict[str, pd.DataFrame]) -> go.Figure:
        fig = go.Figure()
        colors = self._get_color_palette(len(concept_data))

        for i, (name, df) in enumerate(concept_data.items()):
            # 使用数据点的索引作为x轴
            x_indices = list(range(len(df)))
            # 选择部分时间戳作为刻度标签
            tick_values = x_indices
            tick_texts = [ts.strftime('%m-%d %H:%M') for ts in df['timestamp']]

            fig.add_trace(go.Scatter(
                x=x_indices,  # 使用索引作为x轴值
                y=df['rank'],
                name=name,
                line=dict(color=colors[i]),
                hovertemplate=
                '<b>%{text}</b><br>' +
                'Time: ' + df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').iloc[0] + '<br>' +
                'Rank: %{y}<br>',
                text=[name] * len(df)
            ))

        self._update_layout(fig, tick_values, tick_texts)
        return fig

    def save(self, chart: go.Figure, filename: str):
        chart.write_html(filename)

    def _get_color_palette(self, n_colors: int) -> List[str]:
        import plotly.express as px
        return px.colors.qualitative.Set3[:n_colors]

    def _update_layout(self, fig: go.Figure, tick_values, tick_texts):
        title = self.get_label('概念板块排名变化趋势', 'Concept Sector Rank Changes')
        x_label = self.get_label('日期', 'Date')
        y_label = self.get_label('排名', 'Rank')

        fig.update_layout(
            title={'text': title, 'font': {'size': 20}},
            xaxis_title=x_label,
            yaxis_title=y_label,
            yaxis_autorange="reversed",
            hovermode='x unified',
            showlegend=True,
            template='plotly_white',
            height=600,
            font=dict(size=14)
        )

        # 设置等距的x轴刻度
        fig.update_xaxes(
            tickmode='array',
            tickvals=tick_values,
            ticktext=tick_texts,
            tickangle=45
        )


class MatplotlibChartGenerator(ChartGenerator):
    """Matplotlib静态图表生成器"""

    def generate(self, concept_data: Dict[str, pd.DataFrame]) -> plt.Figure:
        # 计算最大数据点数，动态调整图表宽度
        max_points = max(len(df) for df in concept_data.values())
        fig_width = max(12, int(max_points * 0.2 + 1))  # 每个点占0.2宽度，最小宽度为12
        fig = plt.figure(figsize=(fig_width, 8))

        colors = self._get_color_palette(len(concept_data))

        for (name, df), color in zip(concept_data.items(), colors):
            # 使用数据点的索引作为x轴
            x_indices = range(len(df))
            plt.plot(x_indices, df['rank'],
                     label=name, color=color,
                     marker='o', markersize=4)

            # 设置x轴刻度和标签，优化间隔
            xticks_interval = max(1, len(df) // 10)  # 每10个点显示一个日期
            plt.xticks(
                x_indices[::xticks_interval],
                [ts.strftime('%m-%d %H:%M') for ts in df['timestamp'][::xticks_interval]],
                rotation=45, ha='right'
            )

        self._update_layout()
        return fig

    def save(self, chart: plt.Figure, filename: str):
        chart.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

    def _get_color_palette(self, n_colors: int) -> List[str]:
        return sns.color_palette("husl", n_colors)

    def _update_layout(self):
        title = self.get_label('概念板块排名变化趋势', 'Concept Sector Rank Changes')
        x_label = self.get_label('日期', 'Date')
        y_label = self.get_label('排名', 'Rank')

        plt.title(title, fontsize=14, pad=20)
        plt.xlabel(x_label, fontsize=12)
        plt.ylabel(y_label, fontsize=12)
        plt.gca().invert_yaxis()
        plt.legend(bbox_to_anchor=(1.05, 1),
                   loc='upper left',
                   borderaxespad=0.)
        plt.tight_layout()


class ConceptRankChart:
    """概念股排名分析器"""

    def __init__(self):
        self.chart_generators = {
            'html': PlotlyChartGenerator(),
            'png': MatplotlibChartGenerator()
        }
        self.concept_data = {}

    def analyze(self, concept_codes: List[str], days: int = 30) -> Dict[str, pd.DataFrame]:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        for code in concept_codes:
            query = (ConceptName
                     .select(ConceptName.code,
                             ConceptName.name,
                             ConceptName.rank,
                             ConceptName.timestamp)
                     .where(
                (ConceptName.code == code) &
                (ConceptName.timestamp >= start_date) &
                (ConceptName.timestamp <= end_date))
                     .order_by(ConceptName.timestamp))

            df = pd.DataFrame(list(query.dicts()))
            if not df.empty:
                self.concept_data[df['name'].iloc[0]] = df

        return self.concept_data

    def generate_chart(self, output_type: str) -> any:
        if output_type not in self.chart_generators:
            raise ValueError(f"Unsupported output type: {output_type}")

        generator = self.chart_generators[output_type]
        return generator.generate(self.concept_data)

    def save_chart(self, chart: any, filename: str, output_type: str):
        generator = self.chart_generators[output_type]
        generator.save(chart, filename)


def main():
    analyzer = ConceptRankChart()
    concept_codes = ['BK0493', 'BK0891', 'BK0628']
    analyzer.analyze(concept_codes)

    html_chart = analyzer.generate_chart('html')
    analyzer.save_chart(html_chart, 'concept_ranks.html', 'html')

    png_chart = analyzer.generate_chart('png')
    analyzer.save_chart(png_chart, 'concept_ranks.png', 'png')


if __name__ == "__main__":
    main()
