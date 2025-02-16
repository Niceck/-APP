import streamlit as st
import tushare as ts
import pandas as pd

tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()



def get_concept_data(trade_date):
    """
    获取题材数据，字段包括：trade_date, ts_code, name, z_t_num
    字段转换：
      ts_code -> 题材名称
      name    -> 题材代码
      z_t_num -> 涨停板数量
      trade_date -> 交易日期
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
        # 设置索引从1开始
        df.index = range(1, len(df) + 1)
        return df
    except Exception as e:
        st.error(f"获取题材数据失败: {e}")
        return pd.DataFrame()


def get_concept_cons_data(concept_code, trade_date):
    """
    获取指定题材代码对应的成分股数据，并合并每个成分股的涨跌幅数据。
    注意：原始数据中，股票代码与题材代码的字段位置与预期相反，
    因此做如下映射：
      ts_code   -> 题材代码
      name      -> 题材名称
      con_code  -> 股票代码
      con_name  -> 股票名称
      trade_date-> 交易日期
      desc      -> 描述
      hot_num   -> 热度
    """
    try:
        # 获取成分股数据
        df_cons = pro.kpl_concept_cons(
            ts_code=concept_code,
            trade_date=trade_date,
            fields=["ts_code", "name", "con_name", "con_code", "trade_date", "desc", "hot_num"]
        )
        if df_cons.empty:
            st.info("没有找到对应的成分股数据。")
            return df_cons

        # 一次性拉取当日所有股票的日线数据（涨跌幅）
        df_daily = pro.daily(
            trade_date=trade_date,
            fields=["ts_code", "pct_chg"]
        )

        # 合并成分股数据与日线数据（按股票代码合并，这里原始 con_code 为股票代码）
        df_merged = pd.merge(df_cons, df_daily, left_on="con_code", right_on="ts_code", how="left")

        # 调整字段，删除合并后多余的字段（右侧的 ts_code 可以去掉）
        df_merged.drop(columns=["ts_code_y"], inplace=True)
        df_merged.rename(columns={"ts_code_x": "原始题材代码"}, inplace=True)

        # 重命名字段，注意对调股票代码与题材代码
        df_merged.rename(columns={
            '原始题材代码': '题材代码',   # 原 ts_code 实际为题材代码
            'name': '题材名称',
            'con_code': '股票代码',     # 原 con_code 实际为股票代码
            'con_name': '股票名称',
            'trade_date': '交易日期',
            'desc': '描述',
            'hot_num': '热度',
            'pct_chg': '涨跌幅'
        }, inplace=True)

        # 格式化涨跌幅为保留一位小数
        df_merged['涨跌幅'] = df_merged['涨跌幅'].apply(lambda x: round(x, 1) if pd.notnull(x) else x)

        # 调整字段顺序，确保涨跌幅放在交易日期和描述之间
        df_merged = df_merged[['题材代码', '题材名称', '股票代码', '股票名称', '交易日期', '涨跌幅', '描述', '热度']]

        # 按涨跌幅降序排列
        df_merged = df_merged.sort_values(by="涨跌幅", ascending=False)

        # 设置索引从1开始
        df_merged.index = range(1, len(df_merged) + 1)

        return df_merged
    except Exception as e:
        st.error(f"获取成分股数据失败: {e}")
        return pd.DataFrame()


def main():
    st.title("题材数据及成分股查询")
    st.markdown("输入查询日期和题材代码，获取对应的题材数据及成分股数据。")

    # 侧边栏参数输入
    st.sidebar.header("参数设置")
    trade_date = st.sidebar.date_input("选择日期")
    trade_date_str = trade_date.strftime("%Y%m%d")
    concept_code = st.sidebar.text_input("输入题材代码")

    if st.sidebar.button("开始查询"):
        # 获取并展示题材数据（供参考）
        st.subheader(f"题材数据（{trade_date_str}）")
        concept_data = get_concept_data(trade_date_str)
        if not concept_data.empty:
            st.dataframe(concept_data)
        else:
            st.info("没有找到题材数据。")

        # 获取成分股数据
        if concept_code and trade_date:
            st.subheader(f"题材成分股数据（{concept_code} - {trade_date_str}）")
            cons_data = get_concept_cons_data(concept_code, trade_date_str)
            if not cons_data.empty:
                st.dataframe(cons_data)
            else:
                st.info("没有找到成分股数据。")
        else:
            st.info("请填写题材代码并选择日期。")


if __name__ == "__main__":
    main()
