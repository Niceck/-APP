import time
import tushare as ts
import pandas as pd
import datetime as dt
import logging
import os
import streamlit as st

# ==================== 全局设置 ====================
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')

logging.basicConfig(
    filename='script.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 从 secrets.toml 中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]
ts.set_token(tushare_token)
pro = ts.pro_api()


# ==================== 工具函数 ====================

def get_last_n_trade_dates(n=10):
    """
    获取最近 n 个交易日列表，返回形如 ['20250109', '20250108', …]
    """
    try:
        today = dt.datetime.today()
        today_str = today.strftime('%Y%m%d')
        trade_cal = pro.trade_cal(exchange='', end_date=today_str, is_open=1)
        if trade_cal.empty:
            logging.error("获取交易日历失败，返回空数据")
            return []
        trade_cal = trade_cal.sort_values(by='cal_date', ascending=False)
        return trade_cal['cal_date'].head(n).tolist()
    except Exception as e:
        logging.error(f"获取交易日历时出错: {e}")
        return []


def get_themes_for_date(trade_date):
    """
    获取指定交易日的题材数据，字段包括：
      trade_date, ts_code, name, z_t_num, up_num
    返回 DataFrame
    """
    try:
        logging.info(f"Fetching themes for date: {trade_date}")
        df = pro.kpl_concept(
            trade_date=trade_date,
            ts_code="",
            name="",
            limit="",
            offset="",
            fields=["trade_date", "ts_code", "name", "z_t_num", "up_num"]
        )
        if df.empty:
            logging.warning(f"当天({trade_date}) kpl_concept 接口返回空")
            return pd.DataFrame()
        for col in ['z_t_num', 'up_num']:
            if col not in df.columns:
                logging.error(f"'{col}' 不在题材数据中")
                return pd.DataFrame()
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        logging.error(f"Error fetching themes for date {trade_date}: {e}")
        return pd.DataFrame()


def calculate_avg(df_all):
    """
    计算5日和10日的平均涨停数、升温值，
    返回 DataFrame 包含：ts_code, avg_z_t_num_5, avg_z_t_num_10, avg_up_num_5, avg_up_num_10
    """
    try:
        df_all = df_all.sort_values(by=['ts_code', 'trade_date'])
        df_all['avg_z_t_num_5'] = df_all.groupby('ts_code')['z_t_num'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df_all['avg_z_t_num_10'] = df_all.groupby('ts_code')['z_t_num'].transform(
            lambda x: x.rolling(window=10, min_periods=1).mean()
        )
        df_all['avg_up_num_5'] = df_all.groupby('ts_code')['up_num'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df_all['avg_up_num_10'] = df_all.groupby('ts_code')['up_num'].transform(
            lambda x: x.rolling(window=10, min_periods=1).mean()
        )
        df_avg = df_all.groupby('ts_code').tail(1)
        return df_avg[['ts_code', 'avg_z_t_num_5', 'avg_z_t_num_10', 'avg_up_num_5', 'avg_up_num_10']]
    except Exception as e:
        logging.error(f"Error calculating averages: {e}")
        return pd.DataFrame()


def filter_themes(df_latest, df_avg):
    """
    筛选“近期最强题材”和“近期升温题材”
    筛选条件：
      - 近期最强：当天涨停数 > 5日均值 且 5日均值 > 10日均值
      - 近期升温：当天升温值 > 5日均值 且 5日均值 > 10日均值
    返回两个 DataFrame（保留 trade_date 字段，用于后续匹配成分股）
    """
    try:
        df = pd.merge(df_latest, df_avg, on='ts_code', how='left')
        df_strong = df[(df['z_t_num'] > df['avg_z_t_num_5']) &
                       (df['avg_z_t_num_5'] > df['avg_z_t_num_10'])]
        df_rising = df[(df['up_num'] > df['avg_up_num_5']) &
                       (df['avg_up_num_5'] > df['avg_up_num_10'])]
        return df_strong.sort_values(by='z_t_num', ascending=False).head(5).reset_index(drop=True), \
            df_rising.sort_values(by='up_num', ascending=False).head(5).reset_index(drop=True)
    except Exception as e:
        logging.error(f"Error filtering themes: {e}")
        return pd.DataFrame(), pd.DataFrame()


def get_component_stocks(theme_ts_code, trade_dates):
    """
    获取指定题材在给定日期列表中（按顺序）有数据的成分股（字段 con_code）。
    如果第一个日期无数据，则尝试后续日期。
    """
    try:
        for t_date in trade_dates:
            df = pro.kpl_concept_cons(
                ts_code=theme_ts_code,
                trade_date=t_date,
                fields=["con_code"]
            )
            if not df.empty:
                return df['con_code'].dropna().unique().tolist()
        return []
    except Exception as e:
        logging.error(f"Error fetching component stocks for theme {theme_ts_code}: {e}")
        return []


def get_all_hot_money_details(trade_date, fallback_date=None):
    """
    一次性调用 pro.hm_detail，查询指定交易日的游资数据（字段 ts_code, hm_name）。
    若返回空，则使用备用日期。
    """
    df = pro.hm_detail(trade_date=trade_date, fields=["ts_code", "hm_name"])
    if df.empty and fallback_date:
        logging.info(f"{trade_date} 无游资数据，改用备用日期 {fallback_date}")
        df = pro.hm_detail(trade_date=fallback_date, fields=["ts_code", "hm_name"])
    return df


def compute_hot_money_counts_for_themes_once(df, latest_date, fallback_date=None):
    """
    针对传入的题材 DataFrame（均属于最新交易日），统计每个题材的总游资数：
      1. 对每个题材，先获取其成分股（若最新交易日无数据则用备用日期）；
      2. 汇总所有题材的成分股，并一次性调用 pro.hm_detail（只传 trade_date）获取游资数据；
      3. 按股票代码去重其 hm_name 后，对每个题材取其成分股对应的游资名称并集，计数作为该题材的“游资数”。
    """
    # 构建题材 -> 成分股 映射
    theme_to_components = {}
    for idx, row in df.iterrows():
        theme_code = row['ts_code']
        comps = get_component_stocks(theme_code, [latest_date])
        if not comps and fallback_date:
            comps = get_component_stocks(theme_code, [fallback_date])
        theme_to_components[theme_code] = comps

    # 汇总所有成分股（去重）
    all_components = set()
    for comps in theme_to_components.values():
        all_components.update(comps)
    all_components = list(all_components)

    # 一次性调用 hm_detail 查询所有成分股的游资数据
    hm_df = get_all_hot_money_details(latest_date, fallback_date)
    if not hm_df.empty:
        hm_df = hm_df[hm_df['ts_code'].isin(all_components)]

    # 构建映射：股票代码 -> 去重后的游资名称集合
    stock_to_hotmoney = {}
    if not hm_df.empty:
        for stock, group in hm_df.groupby('ts_code'):
            stock_to_hotmoney[stock] = set(group['hm_name'].dropna().unique())

    # 对每个题材，取其成分股对应的游资名称并集，计数作为“游资数”
    theme_hotmoney_count = {}
    for theme_code, comps in theme_to_components.items():
        hm_set = set()
        for comp in comps:
            if comp in stock_to_hotmoney:
                hm_set.update(stock_to_hotmoney[comp])
        theme_hotmoney_count[theme_code] = len(hm_set)

    df[' 游资数'] = df['ts_code'].apply(lambda code: theme_hotmoney_count.get(code, 0))
    return df


def format_number(x):
    """
    格式化数值：
      - 如果数值为整数则不显示小数点（例如 13.0 显示为 "13"）
      - 否则保留1位小数（例如 14.2）
    """
    try:
        if pd.isna(x):
            return ""
        x = float(x)
        if x.is_integer():
            return f"{int(x)}"
        else:
            return f"{x:.1f}"
    except Exception:
        return x


def display_table(df, title):
    """
    在 Streamlit 页面上显示 DataFrame 表格，
    对所有数值型数据按要求格式化。
    """
    st.subheader(title)
    df_formatted = df.copy()
    for col in df_formatted.columns:
        if pd.api.types.is_numeric_dtype(df_formatted[col]):
            df_formatted[col] = df_formatted[col].map(format_number)
    st.table(df_formatted.reset_index(drop=True))


# ==================== 主函数 ====================
def main():
    st.title("题材数据分析")
    st.markdown(
        """
        程序自动获取最新数据，筛选出“近期最强题材”与“近期升温题材”，
        并统计每个题材参与的总游资数（基于题材成分股）。
        """
    )

    try:
        st.info("开始执行，请稍候……")

        # ---------------- 获取最近 10 个交易日 ----------------
        trade_dates = get_last_n_trade_dates(n=10)
        if not trade_dates:
            st.error("未能获取有效交易日")
            return

        # ---------------- 获取这几天的题材数据 ----------------
        all_data = []
        for t_date in trade_dates:
            df_temp = get_themes_for_date(t_date)
            if not df_temp.empty:
                all_data.append(df_temp)
        if not all_data:
            st.error("未能获取到题材数据")
            return
        df_all = pd.concat(all_data, ignore_index=True)
        logging.info(f"合并题材数据行数: {len(df_all)}")

        # ---------------- 取最新交易日数据 ----------------
        latest_date = df_all['trade_date'].max()
        df_latest = df_all[df_all['trade_date'] == latest_date][['trade_date', 'ts_code', 'name', 'z_t_num', 'up_num']]
        logging.info(f"最新交易日: {latest_date}")

        # 备用日期：若最新交易日无游资数据，则用排序后第二个日期
        fallback_date = None
        trade_dates_sorted = sorted(trade_dates, reverse=True)
        if len(trade_dates_sorted) >= 2:
            fallback_date = trade_dates_sorted[1]

        # ---------------- 计算均值（基于最近 10 天数据） ----------------
        df_avg = calculate_avg(df_all)
        if df_avg.empty:
            st.error("计算均值失败")
            return

        # ---------------- 筛选题材 ----------------
        df_filtered_z, df_filtered_up = filter_themes(df_latest, df_avg)
        if df_filtered_z.empty and df_filtered_up.empty:
            st.warning("未筛选出符合条件的题材")
            return

        # ---------------- 统计游资数据（一次性调用 hm_detail） ----------------
        df_filtered_z = compute_hot_money_counts_for_themes_once(df_filtered_z, latest_date, fallback_date)
        df_filtered_up = compute_hot_money_counts_for_themes_once(df_filtered_up, latest_date, fallback_date)

        # ---------------- 调整列顺序 ----------------
        cols = ['trade_date', 'ts_code', 'name', ' 游资数', 'z_t_num', 'up_num',
                'avg_z_t_num_5', 'avg_z_t_num_10', 'avg_up_num_5', 'avg_up_num_10']
        df_filtered_z = df_filtered_z[cols]
        df_filtered_up = df_filtered_up[cols]

        # ---------------- 在页面上显示结果 ----------------
        display_table(df_filtered_z, "近期最强题材")
        display_table(df_filtered_up, "近期升温题材")

        # ---------------- 获取所有题材对应的成分股，写入文件 ----------------
        all_stock_codes = set()
        for theme in pd.concat([df_filtered_z, df_filtered_up]).drop_duplicates(subset=['ts_code'])['ts_code']:
            comps = get_component_stocks(theme, [latest_date])
            all_stock_codes.update(comps)
        st.write(f"成分股总数: {len(all_stock_codes)}")

        output_folder = "date"
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, '成分股.txt')
        if os.path.exists(output_file):
            os.remove(output_file)
        with open(output_file, 'w', encoding='utf-8') as f:
            for code in sorted(all_stock_codes):
                f.write(f"{code}\n")
        st.success(f"成分股文件已保存至：{output_file}")

        # 返回保存的文件路径以便主脚本调用 Git 更新
        return output_file

    except Exception as e:
        logging.error(f"执行过程中出错: {e}")
        st.error(f"程序执行出错：{e}")



    except Exception as e:
        logging.error(f"执行过程中出错: {e}")
        st.error(f"程序执行出错：{e}")


if __name__ == "__main__":
    main()
