import streamlit as st
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta

tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

def get_concept_data(trade_date):
    """
    获取题材数据，字段包括：交易日期、题材名称、题材代码、涨停板数量
    按涨停板数量降序排列。
    """
    try:
        df = pro.kpl_concept(
            trade_date=trade_date,
            fields=["trade_date", "ts_code", "name", "z_t_num"]
        )
        df = df.sort_values(by="z_t_num", ascending=False)
        df.rename(columns={
            'ts_code': '题材名称',
            'name': '题材代码',
            'z_t_num': '涨停板数量',
            'trade_date': '交易日期'
        }, inplace=True)
        df.index = range(1, len(df) + 1)
        return df
    except Exception as e:
        st.error(f"获取题材数据失败: {e}")
        return pd.DataFrame()

def get_latest_daily_data(trade_date, max_rollback=5):
    """
    获取每日行情数据（只包含股票代码和涨跌幅），
    如果当前日期没有数据则回撤一天，最多回撤 max_rollback 次，
    返回每日数据及实际使用的交易日期。
    """
    try:
        daily_data = pro.daily(trade_date=trade_date, fields=["ts_code", "pct_chg"])
        rollback_attempt = 0
        while daily_data.empty and rollback_attempt < max_rollback:
            trade_date_dt = datetime.strptime(trade_date, "%Y%m%d")
            trade_date_dt -= timedelta(days=1)
            trade_date = trade_date_dt.strftime("%Y%m%d")
            st.info(f"每日行情数据为空，回撤到 {trade_date}")
            daily_data = pro.daily(trade_date=trade_date, fields=["ts_code", "pct_chg"])
            rollback_attempt += 1
        return daily_data, trade_date
    except Exception as e:
        st.error(f"获取每日行情数据失败: {e}")
        return pd.DataFrame(), trade_date

def get_concept_cons_data(concept_code, cons_trade_date, daily_trade_date, max_rollback=5):
    """
    获取指定题材代码对应的成分股数据，并合并每个成分股的最新涨跌幅数据。
    成分股数据和每日行情数据均采用回撤逻辑：
      - 如果成分股数据为空，则回撤一天后重新查询（最多回撤 max_rollback 次）。
      - 每日行情数据查询时，如果当前日期无数据，则回撤一天后重新查询（最多回撤 max_rollback 次）。
    注意：最终输出的成分股表中不包含交易日期列。
    """
    try:
        # 查询成分股数据，若无数据则回撤一个交易日
        rollback_attempt = 0
        df_cons = pro.kpl_concept_cons(
            ts_code=concept_code,
            trade_date=cons_trade_date,
            fields=["ts_code", "name", "con_name", "con_code", "trade_date", "desc", "hot_num"]
        )
        while df_cons.empty and rollback_attempt < max_rollback:
            cons_trade_date_dt = datetime.strptime(cons_trade_date, "%Y%m%d")
            cons_trade_date_dt -= timedelta(days=1)
            cons_trade_date = cons_trade_date_dt.strftime("%Y%m%d")
            st.info(f"成分股数据为空，回撤到 {cons_trade_date}")
            df_cons = pro.kpl_concept_cons(
                ts_code=concept_code,
                trade_date=cons_trade_date,
                fields=["ts_code", "name", "con_name", "con_code", "trade_date", "desc", "hot_num"]
            )
            rollback_attempt += 1

        if df_cons.empty:
            st.info("已达到最大回撤次数，仍然未找到成分股数据。")
            return df_cons

        # 查询每日行情数据（涨跌幅），采用独立的日期回撤逻辑
        df_daily, used_daily_trade_date = get_latest_daily_data(daily_trade_date, max_rollback)

        # 合并成分股数据与每日行情数据（按股票代码合并，成分股字段为 con_code）
        df_merged = pd.merge(df_cons, df_daily, left_on="con_code", right_on="ts_code", how="left")

        # 删除合并后冗余的 ts_code 列，并调整字段名称
        df_merged.drop(columns=["ts_code_y"], inplace=True, errors='ignore')
        df_merged.rename(columns={"ts_code_x": "原始题材代码"}, inplace=True)
        df_merged.rename(columns={
            '原始题材代码': '题材代码',   # 原 ts_code 实际为题材代码
            'name': '题材名称',
            'con_code': '股票代码',     # 原 con_code 实际为股票代码
            'con_name': '股票名称',
            'desc': '描述',
            'hot_num': '热度',
            'pct_chg': '涨跌幅'
        }, inplace=True)

        # 格式化涨跌幅，保留一位小数
        df_merged['涨跌幅'] = df_merged['涨跌幅'].apply(lambda x: round(x, 1) if pd.notnull(x) else x)

        # 去除交易日期列
        if 'trade_date' in df_merged.columns:
            df_merged.drop(columns=["trade_date"], inplace=True)

        # 调整输出字段顺序
        df_merged = df_merged[['题材代码', '题材名称', '股票代码', '股票名称', '涨跌幅', '描述', '热度']]

        # 按涨跌幅降序排列，并设置索引从 1 开始
        df_merged = df_merged.sort_values(by="涨跌幅", ascending=False)
        df_merged.index = range(1, len(df_merged) + 1)

        return df_merged
    except Exception as e:
        st.error(f"获取成分股数据失败: {e}")
        return pd.DataFrame()

def main():
    st.title("题材数据及成分股查询")
    st.markdown("输入查询日期和题材代码，获取对应的题材数据及成分股数据。")

    # 用户输入查询日期和题材代码
    trade_date = st.date_input("选择日期", value=datetime.now().date())
    trade_date_str = trade_date.strftime("%Y%m%d") if trade_date else ""
    concept_code = st.text_input("输入题材代码")

    if st.button("开始查询"):
        # 显示题材数据（包含交易日期）
        st.subheader(f"题材数据（{trade_date_str}）")
        concept_data = get_concept_data(trade_date_str)
        if not concept_data.empty:
            st.dataframe(concept_data)
        else:
            st.info("没有找到题材数据。")

        # 查询并显示成分股数据（输出表中不含交易日期），同时涨跌幅使用独立的最新交易日逻辑
        if concept_code and trade_date_str:
            st.subheader(f"题材成分股数据（{concept_code}）")
            cons_data = get_concept_cons_data(concept_code, trade_date_str, trade_date_str)
            if not cons_data.empty:
                st.dataframe(cons_data)
            else:
                st.info("没有找到成分股数据。")
        else:
            st.info("请填写题材代码并选择日期。")

if __name__ == "__main__":
    main()
