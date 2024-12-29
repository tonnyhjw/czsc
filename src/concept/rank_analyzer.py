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


from database.models import ConceptName


class ChartConfig:
    """图表配置类"""

    @staticmethod
    def setup_chinese_font():
        """配置中文字体支持"""
        system = platform.system()
        if system == 'Linux':
            # Linux系统字体配置
            plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'SimHei', 'Arial Unicode MS']
        elif system == 'Windows':
            # Windows系统字体配置
            plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
        elif system == 'Darwin':  # MacOS
            # MacOS系统字体配置
            plt.rcParams['font.sans-serif'] = ['PingFang HK', 'Arial Unicode MS']

        plt.rcParams['axes.unicode_minus'] = False  # 正确显示负号

    @staticmethod
    def verify_font_support():
        """验证中文字体支持"""
        fonts = [f.name for f in fm.fontManager.ttflist]
        available_chinese_fonts = []

        # 检查常见中文字体
        chinese_fonts = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei',
                         'PingFang HK', 'Arial Unicode MS']

        for font in chinese_fonts:
            if font in fonts:
                available_chinese_fonts.append(font)

        if not available_chinese_fonts:
            print("警告：未找到中文字体，可能需要安装中文字体包")
            print("Linux可以运行：sudo apt-get install fonts-wqy-microhei")
            return False

        return True


class ChartGenerator(ABC):
    """图表生成器的抽象基类"""

    def __init__(self):
        ChartConfig.setup_chinese_font()
        self.font_supported = ChartConfig.verify_font_support()

    @abstractmethod
    def generate(self, concept_data: Dict[str, pd.DataFrame]) -> any:
        """生成图表"""
        pass

    @abstractmethod
    def save(self, chart: any, filename: str):
        """保存图表"""
        pass


class PlotlyChartGenerator(ChartGenerator):
    """Plotly交互式图表生成器"""

    def generate(self, concept_data: Dict[str, pd.DataFrame]) -> go.Figure:
        fig = go.Figure()
        colors = self._get_color_palette(len(concept_data))

        for i, (name, df) in enumerate(concept_data.items()):
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['rank'],
                name=name,
                line=dict(color=colors[i]),
                hovertemplate=
                '<b>%{text}</b><br>' +
                '时间: %{x}<br>' +
                '排名: %{y}<br>',
                text=[name] * len(df)
            ))

        self._update_layout(fig)
        return fig

    def save(self, chart: go.Figure, filename: str):
        chart.write_html(filename)

    def _get_color_palette(self, n_colors: int) -> List[str]:
        """获取颜色列表"""
        import plotly.express as px
        return px.colors.qualitative.Set3[:n_colors]

    def _update_layout(self, fig: go.Figure):
        """更新图表布局"""
        fig.update_layout(
            title={'text': '概念板块排名变化趋势',
                   'font': {'size': 20}},
            xaxis_title='日期',
            yaxis_title='排名',
            yaxis_autorange="reversed",
            hovermode='x unified',
            showlegend=True,
            template='plotly_white',
            height=600,
            font=dict(size=14)
        )


class MatplotlibChartGenerator(ChartGenerator):
    """Matplotlib静态图表生成器"""

    def generate(self, concept_data: Dict[str, pd.DataFrame]) -> plt.Figure:
        if not self.font_supported:
            print("警告：未找到合适的中文字体，图表中的中文可能无法正确显示")

        fig = plt.figure(figsize=(12, 8))
        colors = self._get_color_palette(len(concept_data))

        for (name, df), color in zip(concept_data.items(), colors):
            plt.plot(df['timestamp'], df['rank'],
                     label=name, color=color,
                     marker='o', markersize=4)

        self._update_layout()
        return fig

    def save(self, chart: plt.Figure, filename: str):
        chart.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()

    def _get_color_palette(self, n_colors: int) -> List[str]:
        """获取颜色列表"""
        return sns.color_palette("husl", n_colors)

    def _update_layout(self):
        """更新图表布局"""
        plt.title('概念板块排名变化趋势', fontsize=14, pad=20)
        plt.xlabel('日期', fontsize=12)
        plt.ylabel('排名', fontsize=12)
        plt.gca().invert_yaxis()
        plt.legend(bbox_to_anchor=(1.05, 1),
                   loc='upper left',
                   borderaxespad=0.)
        plt.tight_layout()


class ConceptRankAnalyzer:
    """概念股排名分析器"""

    def __init__(self):
        self.chart_generators = {
            'html': PlotlyChartGenerator(),
            'png': MatplotlibChartGenerator()
        }
        self.concept_data = {}

    def analyze(self, concept_codes: List[str], days: int = 30) -> Dict[str, pd.DataFrame]:
        """分析指定概念板块的排名数据"""
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
        """生成指定类型的图表"""
        if output_type not in self.chart_generators:
            raise ValueError(f"不支持的输出类型: {output_type}")

        generator = self.chart_generators[output_type]
        return generator.generate(self.concept_data)

    def save_chart(self, chart: any, filename: str, output_type: str):
        """保存图表"""
        generator = self.chart_generators[output_type]
        generator.save(chart, filename)


def main():
    # 使用示例
    analyzer = ConceptRankAnalyzer()

    # 分析数据
    concept_codes = ['BK0493', 'BK0891', 'BK0628']
    analyzer.analyze(concept_codes)

    # 生成并保存HTML图表
    html_chart = analyzer.generate_chart('html')
    analyzer.save_chart(html_chart, 'concept_ranks.html', 'html')

    # 生成并保存PNG图表
    png_chart = analyzer.generate_chart('png')
    analyzer.save_chart(png_chart, 'concept_ranks.png', 'png')


if __name__ == "__main__":
    main()