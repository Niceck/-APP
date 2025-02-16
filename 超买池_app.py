import streamlit as st
import tushare as ts
import pandas as pd
import os
import ast
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

pd.set_option('display.max_colwidth', None)

# 从 secrets.toml 中读取 Tushare API Token
tushare_token = st.secrets.get("api_keys", {}).get("tushare_token", "your_default_token_here")
ts.set_token(tushare_token)
pro = ts.pro_api()


def get_recent_trading_days(n, days_back=60):
    """
    从 Tushare 获取最近 days_back 天的交易日历，
    返回最新 n 个交易日（升序排列）。
    """
    today = datetime.today()
    end_date = today.strftime("%Y%m%d")
    start_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
    df_cal = pro.trade_cal(
        exchange='SSE',
        start_date=start_date,
        end_date=end_date,
        fields="cal_date,is_open"
    )
    df_open = df_cal[df_cal["is_open"] == 1].sort_values("cal_date")
    trading_days = df_open["cal_date"].tolist()
    if len(trading_days) >= n:
        return trading_days[-n:]
    else:
        return trading_days


def get_stock_concepts(stock_code):
    """获取指定股票的 concept 标签，并去重返回字符串"""
    try:
        df = pro.ths_hot(ts_code=stock_code, fields=["concept"])
        if df.empty:
            return "无"
        concepts_set = set()
        for concept_entry in df['concept'].dropna():
            try:
                parsed = ast.literal_eval(concept_entry)
                if isinstance(parsed, list):
                    concepts_set.update(parsed)
                else:
                    concepts_set.add(concept_entry)
            except (ValueError, SyntaxError):
                concepts_set.add(concept_entry)
        return "; ".join(sorted(concepts_set)) if concepts_set else "无"
    except Exception as e:
        logging.error(f"获取 {stock_code} 的 concept 失败: {e}")
        return "获取失败"


def main():
    st.title("RSI强势选股")
    st.write("选出短期持续超买股票，可沿5日均线顺势交易")

    # 自动获取最近 3 个交易日（升序排列）
    trading_days = get_recent_trading_days(3)
    if len(trading_days) < 3:
        st.error("无法获取足够的交易日数据。")
        return
    st.write("使用交易日：", trading_days)

    # 分别获取这 3 个交易日的 RSI6 数据（单日查询），传入参数 trade_date
    dfs = []
    for day in trading_days:
        df_day = pro.stk_factor(trade_date=day, fields="rsi_6,ts_code")
        if df_day.empty:
            st.warning(f"{day} 的 RSI 数据为空。")
        else:
            # 将 RSI6 列重命名为 rsi6_日期
            df_day = df_day.rename(columns={"rsi_6": f"rsi6_{day}"})
            dfs.append(df_day)

    if len(dfs) < 3:
        st.error("至少有一天的数据为空，无法进行筛选。")
        return

    # 内连接合并 3 个 DataFrame（按股票代码）
    df_merged = dfs[0]
    for df in dfs[1:]:
        df_merged = pd.merge(df_merged, df, on="ts_code", how="inner")

    # 筛选出这 3 个交易日内 RSI6 均大于等于 80 的股票
    condition = True
    for day in trading_days:
        condition &= (df_merged[f"rsi6_{day}"] >= 80)
    df_selected = df_merged[condition]

    if df_selected.empty:
        st.info("没有符合条件的股票。")
        return

    st.write(f"筛选出 {len(df_selected)} 支股票，获取标签中......")
    rsi_codes = set(df_selected["ts_code"].unique())

    # 加载本地股票池文件（默认路径：date/股东.txt，每行一个股票代码）
    local_stock_pool_path = "date/股东.txt"
    if os.path.exists(local_stock_pool_path):
        with open(local_stock_pool_path, "r", encoding="utf-8") as f:
            local_pool = {line.strip() for line in f if line.strip()}
    else:
        st.error(f"本地股票池文件不存在：{local_stock_pool_path}")
        return

    # 与本地股票池取交集
    selected_codes = rsi_codes.intersection(local_pool)
    if not selected_codes:
        st.info("经过本地股票池筛选后，没有符合条件的股票。")
        return

    # 获取股票基本信息映射：股票代码 -> 股票名称
    df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
    name_map = pd.Series(df_basic.name.values, index=df_basic.ts_code).to_dict()

    records = []
    for code in selected_codes:
        stock_name = name_map.get(code, "未知")
        concepts = get_stock_concepts(code)
        records.append({
            "股票代码": code,
            "股票名称": stock_name,
            "标签": concepts
        })
    result_df = pd.DataFrame(records)

    # 将索引从 1 开始
    result_df.index = range(1, len(result_df) + 1)

    st.write("### 选股结果")
    st.dataframe(result_df, use_container_width=True)


    # 保存股票代码到本地文件，保存路径为 "date/RSI选股.txt"（相对路径）
    save_dir = "date"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    save_path = os.path.join(save_dir, "RSI选股.txt")
    with open(save_path, "w", encoding="utf-8") as f:
        for code in result_df["股票代码"]:
            f.write(f"{code}\n")
    st.success(f"选股结果已保存到：{save_path}")

# 返回文件路径供主脚本调用 Git 更新
return save_path

if __name__ == "__main__":
    main()
