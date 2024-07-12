from src.objects import *


def process_inclusion(feature_sequence: List[FeatureElement], new_elem: FeatureElement) -> List[FeatureElement]:
    """处理包含关系"""
    last_elem = feature_sequence[-1]
    if not is_included(last_elem, new_elem):
        feature_sequence.append(new_elem)
    else:
        # 如果存在包含关系，合并元素
        feature_sequence[-1] = merge_elements(last_elem, new_elem)
    return feature_sequence


def is_included(elem1: FeatureElement, elem2: FeatureElement) -> bool:
    """判断两个元素之间是否存在包含关系"""
    return (elem1.low <= elem2.low and elem1.high >= elem2.high) or \
        (elem2.low <= elem1.low and elem2.high >= elem1.high)


def merge_elements(elem1: FeatureElement, elem2: FeatureElement) -> FeatureElement:
    """合并两个存在包含关系的元素，移除被包含的一根"""

    if elem1.direction == Direction.Up:
        remain_elem = elem1 if elem1.low < elem2.low else elem2
        remain_elem.high = min(elem1.high, elem2.high)
    else:
        remain_elem = elem1 if elem1.high > elem2.high else elem2
        remain_elem.low = max(elem1.low, elem2.low)
    return remain_elem


def identify_tzfxs(feature_sequence: List[FeatureElement], xd_direction: Direction):
    """
    识别特征序列中的分型
    只考虑与起始方向相符的分型
    """
    tzfx = None
    if xd_direction == Direction.Up and is_top_tzfx(feature_sequence[-3:]):
        tzfx = TZFX(feature_sequence[-2].symbol, feature_sequence[-2].bi.sdt, Mark.G,
                    float(feature_sequence[-2].high), min([f.low for f in feature_sequence[-3:]]),
                    float(feature_sequence[-2].high), feature_sequence[-2].bi_index, feature_sequence[-3:])
    elif xd_direction == Direction.Down and is_bottom_tzfx(feature_sequence[-3:]):
        tzfx = TZFX(feature_sequence[-2].symbol, feature_sequence[-2].bi.sdt, Mark.D,
                    max([f.high for f in feature_sequence[-3:]]), float(feature_sequence[-2].low),
                    float(feature_sequence[-2].low), feature_sequence[-2].bi_index, feature_sequence[-3:])
    return tzfx


def is_top_tzfx(elements: List[FeatureElement]) -> bool:
    """判断是否为顶分型"""
    return (elements[1].high > elements[0].high and elements[1].high > elements[2].high and
            elements[1].low > elements[0].low and elements[1].low > elements[2].low)


def is_bottom_tzfx(elements: List[FeatureElement]) -> bool:
    """判断是否为底分型"""
    return (elements[1].low < elements[0].low and elements[1].low < elements[2].low and
            elements[1].high < elements[0].high and elements[1].high < elements[2].high)


def analyze_xd(bis: List[BI]) -> List[XD]:
    """分析笔序列，划分线段"""
    xds = []

    # 创建两个特征序列
    xd_up_sequence = FeatureSequence(xd_direction=Direction.Up, elem_direction=Direction.Down)
    xd_down_sequence = FeatureSequence(xd_direction=Direction.Down, elem_direction=Direction.Up)

    bi_index = 0
    while bi_index < len(bis):
        if not xds:
            if has_gap(bis[bi_index + 1], bis[bi_index + 3]):
                xds.append(XD(bis[bi_index].symbol, [bis[bi_index]], bis[bi_index], bi_index))
            else:
                bi_index += 1
                continue

        # 向上特征序列添加向下笔，向下特征序列添加向上笔
        if bis[bi_index].direction == Direction.Down:
            # 处理向上特征序列
            xd_up_sequence, xd_down_sequence, xds = process_sequence(xd_up_sequence, xd_down_sequence, bis, bi_index, xds)
        else:
            # 处理向下特征序列
            xd_down_sequence, xd_up_sequence, xds = process_sequence(xd_down_sequence, xd_up_sequence, bis, bi_index, xds)

        # 如果最后的线段完结了，就要开始一个新线段
        if xds[-1].is_valid:
            bi_index = xds[-1].end_bi_index + 1
            new_xd = XD(bis[bi_index].symbol, [bis[bi_index]], bis[bi_index], bi_index, start_fx=xds[-1].end_fx)
            xds.append(new_xd)
            xd_up_sequence.sequence, xd_down_sequence.sequence = [], []
            xd_up_sequence.last_tzfx, xd_down_sequence.last_tzfx = None, None
        else:
            bi_index += 1

    # 处理最后一个未完成的线段
    if not xds[-1].is_valid:
        xds[-1].end_bi, xds[-1].end_bi_index = find_extreme_bi(bis, xds[-1].start_bi_index, bi_index, xds[-1].direction)
        xds[-1].bis = bis[xds[-1].start_bi_index: xds[-1].end_bi_index + 1]

    for xd in xds:
        print(xd.start_bi.sdt, xd.end_bi.edt, xd.direction, xd.is_valid)
    return xds


def process_sequence(current_seq: FeatureSequence, opposite_seq: FeatureSequence, bis: List[BI], bi_index: int, xds: List[XD]):

    if len(current_seq.sequence) == 0:
        current_seq.sequence.append(FeatureElement(bis[bi_index], bi_index))
    if len(current_seq.sequence) > 0:
        current_seq.sequence = process_inclusion(current_seq.sequence, FeatureElement(bis[bi_index], bi_index))
    if len(current_seq.sequence) >= 3:
        tzfx = identify_tzfxs(current_seq.sequence, current_seq.xd_direction)
        if tzfx:
            last_tzfx = opposite_seq.last_tzfx

            # 优先判断前一个反向特征序列的last_tzfx是否存在，若存在则直接确定两个线段
            if last_tzfx:
                # 将前一个未完结线段完结
                xds[-1].end_bi = bis[last_tzfx.fx_bi_index - 1]
                xds[-1].end_bi_index = last_tzfx.fx_bi_index - 1
                xds[-1].bis = bis[xds[-1].start_bi_index: xds[-1].end_bi_index + 1]
                xds[-1].end_fx = last_tzfx
                # 直接接入新的线段并完结
                xds.append(XD(tzfx.symbol, bis[last_tzfx.fx_bi_index: tzfx.fx_bi_index],
                              bis[last_tzfx.fx_bi_index], last_tzfx.fx_bi_index,
                              bis[tzfx.fx_bi_index - 1], tzfx.fx_bi_index - 1, last_tzfx, tzfx))
                # 还原两个特征序列的last_tzfx
                current_seq.last_tzfx, opposite_seq.last_tzfx = None, None

            elif current_seq.last_tzfx:
                # 同类分型，用新分型替换旧分型
                current_seq.last_tzfx = tzfx
            else:
                if len(xds) == 1 or tzfx.mark != xds[-1].start_fx.mark:
                    if not tzfx.has_gap():
                        # 无缺口分型，直接形成线段
                        xds[-1].end_bi = bis[tzfx.fx_bi_index-1]
                        xds[-1].end_bi_index = tzfx.fx_bi_index-1
                        xds[-1].bis = bis[xds[-1].start_bi_index: xds[-1].end_bi_index + 1]
                        xds[-1].end_fx = tzfx
                        current_seq.last_tzfx = None
                        opposite_seq.last_tzfx = None

                    else:
                        # 有缺口分型，保存当前分型
                        current_seq.last_tzfx = tzfx
                else:
                    # 假如同向且没有缺口
                    if not tzfx.has_gap():
                        if xds[-1].start_bi_index - tzfx.fx_bi_index >= 6:
                            # 如果分型的笔到起始笔够6笔就插入一笔，以其中最高的一笔算
                            xds[-1].end_bi, xds[-1].end_bi_index = find_extreme_bi(bis, xds[-1].start_bi_index,
                                                                                   tzfx.fx_bi_index, xds[-1].direction)
                            xds[-1].bis = bis[xds[-1].start_bi_index: xds[-1].end_bi_index + 1]
                            # 再创建一个新的线段接上底分型
                            xds.append(XD(tzfx.symbol, bis[xds[-1].end_bi_index + 1: tzfx.fx_bi_index],
                                          bis[xds[-1].end_bi_index + 1], xds[-1].end_bi_index + 1,
                                          bis[tzfx.fx_bi_index - 1], tzfx.fx_bi_index - 1, end_fx=tzfx))
                        else:
                            # 否则用后一个分型替换前一个分型
                            xds.pop()
                            xds[-1].end_bi = bis[tzfx.fx_bi_index - 1]
                            xds[-1].end_bi_index = tzfx.fx_bi_index - 1
                            xds[-1].end_fx = tzfx
                            xds[-1].bis = bis[xds[-1].start_bi_index: xds[-1].end_bi_index + 1]
                        current_seq.last_tzfx = None
                        opposite_seq.last_tzfx = None

                    # 假如同向且有缺口不处理
                    else:
                        pass

    return current_seq, opposite_seq, xds


def find_extreme_bi(bis: List[BI], start_index: int, end_index: int, xd_direction: Direction) -> tuple[BI, int]:
    if end_index - start_index < 6:
        raise ValueError("end_index must be at least 6 greater than start_index")

    if len(bis) < end_index - 3 + 1:
        raise ValueError("bis list is not long enough for the given end_index")

    # 调整切片范围
    start = start_index + 2
    end = end_index - 3

    if start >= end:
        raise ValueError("Invalid index range: no elements to process")

    # 选择合适的极值函数和方向
    extreme_func = max if xd_direction == Direction.Up else min
    target_direction = Direction.Up if xd_direction == Direction.Up else Direction.Down

    # 选择合适的属性
    attr = 'high' if xd_direction == Direction.Up else 'low'

    # 使用 enumerate 和极值函数找到符合条件的元素及其索引
    target_bi, relative_index = extreme_func(
        ((bi, i) for i, bi in enumerate(bis[start:end]) if bi.direction == target_direction),
        key=lambda x: getattr(x[0], attr),
        default=(None, -1)
    )

    # 如果找到了符合条件的元素，计算其在原列表中的索引
    if target_bi is not None:
        target_bi_index = start + relative_index
        return target_bi, target_bi_index
    else:
        raise ValueError(f"{target_bi=} {len(bis[start:end])=} {bis[start]} {target_direction} {end=}")


# 主函数
def analyze_bi_sequence(bis: List[BI]) -> List[XD]:
    """分析完整的笔序列，返回划分的线段列表"""
    return analyze_xd(bis)


# 使用示例
if __name__ == "__main__":
    # 创建一些示例BI数据
    sample_bis = [
        BI(10, 20, Direction.Up),
        BI(20, 15, Direction.Down),
        BI(15, 25, Direction.Up),
        BI(25, 18, Direction.Down),
        BI(18, 30, Direction.Up),
        BI(30, 22, Direction.Down),
    ]

    segments = analyze_bi_sequence(sample_bis)

    # 输出结果
    for i, segment in enumerate(segments):
        print(f"线段 {i + 1}:")
        print(f"  起始: {segment.start_bi.start} -> {segment.start_bi.end}")
        print(f"  结束: {segment.end_bi.start} -> {segment.end_bi.end}")
        print(f"  特征序列: {[(fe.high, fe.low) for fe in segment.feature_sequence]}")
        print()
