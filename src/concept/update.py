import datetime
import akshare as ak
from loguru import logger


from database.models import ConceptName, ConceptCons, db_concept_em

logger.add("statics/logs/concept.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def fetch_and_store_concept_name():
    # 获取概念板块数据
    concept_df = ak.stock_board_concept_name_em()
    _timestamp = datetime.datetime.now()

    # 计算涨跌比
    concept_df['涨跌比'] = concept_df['上涨家数'] / (concept_df['上涨家数'] + concept_df['下跌家数'])

    try:
        # 遍历数据并存入数据库
        with db_concept_em.atomic():  # 使用事务确保数据一致性
            for _, row in concept_df.iterrows():
                ConceptName.create(
                    name=row['板块名称'],
                    code=row['板块代码'],
                    rank=row['排名'],
                    rise_ratio=row['涨跌比'],
                    up_count=row['上涨家数'],
                    down_count=row['下跌家数'],
                    timestamp=_timestamp
                )
            # 如果操作正常，事务会被提交
            db_concept_em.commit()
        logger.info(f"{len(concept_df)} 条数据已成功插入数据库。")

    except Exception as e:
        # 出现异常时，回滚事务
        db_concept_em.rollback()
        logger.error(f"数据库操作失败：{str(e)}")


def fetch_and_store_concept_cons():
    # 获取概念板块数据
    concept_name_df = ak.stock_board_concept_name_em()
    new_concept_name, new_concept_cons = [], []

    try:
        # 遍历数据并存入数据库
        with db_concept_em.atomic():  # 使用事务确保数据一致性
            for _, concept_name_row in concept_name_df.iterrows():
                _concept_name = concept_name_row['板块名称']
                _concept_code = concept_name_row['板块代码']

                # 查重：检查概念代码是否已存在
                if not ConceptCons.select().where(ConceptCons.code == _concept_code).exists():
                    new_concept_name.append({"name": _concept_name, "code": _concept_code})

                    # 获取该概念的成分股
                    concept_cons_df = ak.stock_board_concept_cons_em(symbol=_concept_name)

                    for _, concept_cons_row in concept_cons_df.iterrows():
                        _symbol = concept_cons_row['代码']
                        _stock_name = concept_cons_row['名称']

                        # 查重：检查 (概念代码, 股票代码) 是否已存在
                        if not ConceptCons.select().where(
                            (ConceptCons.code == _concept_code) &
                            (ConceptCons.symbol == _symbol)
                        ).exists():
                            new_concept_cons.append({
                                "name": _concept_name,
                                "code": _concept_code,
                                "symbol": _symbol,
                                "stock_name": _stock_name
                            })

            # 批量插入新的概念数据
            if new_concept_cons:
                ConceptCons.insert_many(new_concept_cons).execute()
            # 如果操作正常，事务会被提交
            db_concept_em.commit()

        # 打印结果
        logger.info(f"新增概念数量: {len(new_concept_name)}")
        logger.info(f"新增成分股记录数量: {len(new_concept_cons)}")

    except Exception as e:
        # 出现异常时，回滚事务
        db_concept_em.rollback()
        logger.error(f"数据库操作失败：{str(e)}")

    return new_concept_name, new_concept_cons


def fetch_and_store_concept_new_stock():
    # 获取概念板块数据
    concept_name_df = ak.stock_board_concept_name_em()
    new_concept_cons = []
    try:
        # 遍历数据并存入数据库
        with db_concept_em.atomic():  # 使用事务确保数据一致性
            for _, concept_name_row in concept_name_df.iterrows():
                _concept_name = concept_name_row['板块名称']
                _concept_code = concept_name_row['板块代码']

                # 获取该概念的成分股
                concept_cons_df = ak.stock_board_concept_cons_em(symbol=_concept_name)

                for _, concept_cons_row in concept_cons_df.iterrows():
                    _symbol = concept_cons_row['代码']
                    _stock_name = concept_cons_row['名称']

                    # 查重：检查 (概念代码, 股票代码) 是否已存在
                    if not ConceptCons.select().where(
                            (ConceptCons.code == _concept_code) &
                            (ConceptCons.symbol == _symbol)
                    ).exists():
                        new_concept_cons.append({
                            "name": _concept_name,
                            "code": _concept_code,
                            "symbol": _symbol,
                            "stock_name": _stock_name
                        })

            # 批量插入新的概念数据
            if new_concept_cons:
                ConceptCons.insert_many(new_concept_cons).execute()
            db_concept_em.commit()

        # 打印结果
        logger.info(f"新增成分股记录数量: {len(new_concept_cons)}")
    except Exception as e:
        # 出现异常时，回滚事务
        db_concept_em.rollback()
        logger.error(f"数据库操作失败：{str(e)}")
    return new_concept_cons


