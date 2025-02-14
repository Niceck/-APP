import streamlit as st
import tushare as ts
import pandas as pd
import pandas_ta as ta
import datetime as dt
import os
import logging
import ast  # 用于解析字符串表示的列表
import time
import json
import numpy as np


# 设置页面基本配置
# st.set_page_config(page_title="放量题材股票筛选与评分系统", layout="wide")

# 设置日志记录
logging.basicConfig(filename='error.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


# -------------------------- 各功能函数 --------------------------
def save_selected_stocks(selected_stocks, file_name):
    """
    保存筛选后的股票代码到指定文件。
    """
    # 设置文件路径为相对路径的 'date' 文件夹
    file_path = os.path.join("date", file_name)

    # 确保 'date' 文件夹存在
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # 写入文件
    with open(file_path, "w", encoding='utf-8') as file:
        for stock_code in selected_stocks:
            file.write(f"{stock_code}\n")

    st.success(f"股票列表已保存到: {file_path}")
def get_trade_calendar():
    """获取近两年的交易日历，并过滤至今日之前"""
    today = dt.datetime.today()
    today_str = today.strftime('%Y%m%d')

    two_years_ago = today.replace(year=today.year - 2)
    start_date_str = two_years_ago.strftime('%Y%m%d')

    try:
        df = pro.trade_cal(exchange='SSE', start_date=start_date_str, end_date=today_str, fields='cal_date,is_open')
        df = df[df['cal_date'] <= today_str]
        df = df.sort_values(by='cal_date')
        return df
    except Exception as e:
        logging.error(f"获取交易日历出错: {e}")
        st.error("获取交易日历时出错，请查看 error.log。")
        return pd.DataFrame()

def get_latest_trade_days(trade_cal_df, max_tries=10):
    """获取最近的交易日列表，最多回溯 max_tries 天"""
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return []
    return open_days[-max_tries:]

def get_component_stocks(concept_code, trade_date):
    """根据题材代码和交易日期获取成分股 (已改为 con_code)"""
    try:
        df_cons = pro.kpl_concept_cons(ts_code=concept_code, trade_date=trade_date, fields=["con_code"])
        if df_cons.empty:
            st.info(f"题材代码 {concept_code} 在 {trade_date} 没有成分股。")
            return set()
        return set(df_cons['con_code'].tolist())
    except Exception as e:
        logging.error(f"{concept_code} 在 {trade_date} 获取成分股出错: {e}")
        st.error(f"题材代码 {concept_code} 获取成分股时出错，请查看 error.log。")
        return set()

def load_stock_pool(file_path):
    """从单个文件加载股票池"""
    if not os.path.exists(file_path):
        st.warning(f"文件 {file_path} 不存在，跳过。")
        return set()
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            stock_codes = {line.strip() for line in file if line.strip()}
        st.info(f"从本地文件 {file_path} 加载了 {len(stock_codes)} 只股票。")
        return stock_codes
    except Exception as e:
        logging.error(f"加载本地股票池文件 {file_path} 失败: {e}")
        st.error(f"加载本地股票池文件 {file_path} 时出错，请查看 error.log。")
        return set()

def get_union_stock_pools(file_paths):
    """
    从多个本地文件加载股票池，并计算它们的并集。
    """
    union_set = set()
    for idx, file_path in enumerate(file_paths, start=1):
        stocks = load_stock_pool(file_path)
        if stocks:
            union_set.update(stocks)
    st.info(f"所有输入文件的股票池并集总数: {len(union_set)}")
    return union_set

def get_intersection_with_shareholder(stock_pool, shareholder_pool):
    """
    计算股票池与股东股票池的交集
    """
    intersection = stock_pool & shareholder_pool
    st.info(f"股票池与股东股票池交集后的股票总数: {len(intersection)}")
    return intersection

def technical_stock_selection(stock_code, start_date, end_date):
    """进行技术面筛选，返回符合条件的股票代码"""
    try:
        df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
        if df.empty or not all(col in df.columns for col in ['close', 'vol']):
            return None

        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['vol'] = pd.to_numeric(df['vol'], errors='coerce')
        if df['close'].isnull().any() or df['vol'].isnull().any():
            return None

        df = df.sort_values(by='trade_date').reset_index(drop=True)
        if len(df) < 60:
            return None

        for n in [5, 10, 20, 60]:
            df[f'SMA{n}'] = df['close'].rolling(window=n).mean()

        latest_close_price = df['close'].iloc[-1]
        smas = [f'SMA{n}' for n in [5, 10, 20, 60]]
        if not all(latest_close_price > df[sma].iloc[-1] for sma in smas):
            return None

        df['AVG_VOL10'] = df['vol'].rolling(window=10).mean()
        recent_volume_condition = (
            (df['vol'] >= 2 * df['AVG_VOL10']) &
            ((df['close'] < df['close'].shift(-1)) | (df['close'] > df['close'].shift(1)))
        ).iloc[-60:].any()

        if not recent_volume_condition:
            return None

        rsi = talib.RSI(df['close'].values, timeperiod=6)
        df['RSI'] = rsi
        if df['RSI'].isnull().all() or not 45 < df['RSI'].iloc[-1]:
            return None

        limit_data_start = df['trade_date'].iloc[0]
        limit_data_end = df['trade_date'].iloc[-1]
        limit_df = pro.limit_list_d(start_date=limit_data_start, end_date=limit_data_end, ts_code=stock_code)
        if limit_df.empty:
            return None

        limit_df['limit_times'] = limit_df['limit_times'].fillna(0)
        limit_count = limit_df['limit_times'].sum()

        if limit_count < 1:
            return None

        return stock_code
    except Exception as e:
        logging.error(f"{stock_code} 筛选出错: {e}")
        return None

def fetch_moneyflow_data(stock_code):
    """
    获取当日净值(net_amount)、5日主力净值(net_d5_amount)、大单占比(buy_lg_amount_rate)
    单位分别是 万、万、%。
    """
    try:
        df = pro.moneyflow_ths(ts_code=stock_code, limit=1,
                               fields=["net_amount", "net_d5_amount", "buy_lg_amount_rate"])
        if df.empty:
            st.info(f"没有找到 {stock_code} 的资金流数据。")
            return None, None, None

        net_d5_amount = pd.to_numeric(df['net_d5_amount'].iloc[0], errors='coerce')
        net_amount = pd.to_numeric(df['net_amount'].iloc[0], errors='coerce')
        buy_lg_amount_rate = pd.to_numeric(df['buy_lg_amount_rate'].iloc[0], errors='coerce')

        net_d5_amount = net_d5_amount if net_d5_amount is not None else 0.0
        net_amount = net_amount if net_amount is not None else 0.0
        buy_lg_amount_rate = buy_lg_amount_rate if buy_lg_amount_rate is not None else 0.0

        return net_d5_amount, net_amount, buy_lg_amount_rate
    except Exception as e:
        logging.error(f"{stock_code} 获取资金流数据出错: {e}")
        st.error(f"获取 {stock_code} 的资金流数据时出错，请查看 error.log。")
        return None, None, None

def fetch_stock_basic():
    """获取所有股票的基本信息，并返回 ts_code -> ts_name 的映射字典"""
    try:
        df_basic = pro.stock_basic(exchange='', list_status='L',
                                   fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,list_date')
        stock_basic_mapping = pd.Series(df_basic.name.values, index=df_basic.ts_code).to_dict()
        return stock_basic_mapping
    except Exception as e:
        logging.error(f"获取股票基本信息出错: {e}")
        st.error("获取股票基本信息时出错，请查看 error.log。")
        return {}

def get_stock_concepts(stock_code):
    """获取指定股票的 concept 标签，并进行去重处理"""
    try:
        df = pro.ths_hot(ts_code=stock_code, fields=["concept"])
        if df.empty:
            return "无"
        concepts_set = set()
        for concept_entry in df['concept'].dropna():
            try:
                parsed_concepts = ast.literal_eval(concept_entry)
                if isinstance(parsed_concepts, list):
                    concepts_set.update(parsed_concepts)
                else:
                    concepts_set.add(concept_entry)
            except (ValueError, SyntaxError):
                concepts_set.add(concept_entry)
        return "; ".join(sorted(concepts_set)) if concepts_set else "无"
    except Exception as e:
        logging.error(f"获取 {stock_code} 的 concept 失败: {e}")
        return "获取失败"

def fetch_institution_data(stock_code):
    """
    获取机构总占比(hold_ratio)
    """
    try:
        df = pro.ccass_hold(ts_code=stock_code, limit=1, fields=["hold_ratio", "ts_code", "name"])
        if df.empty:
            st.info(f"没有找到 {stock_code} 的机构持仓数据。")
            return 0.0
        hold_ratio = pd.to_numeric(df['hold_ratio'].iloc[0], errors='coerce')
        hold_ratio = hold_ratio if hold_ratio is not None else 0.0
        return hold_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取机构持仓数据出错: {e}")
        st.error(f"获取 {stock_code} 的机构持仓数据时出错，请查看 error.log。")
        return 0.0

def fetch_northbound_ratio(stock_code):
    """获取北向持股比例 ratio（%）"""
    try:
        df = pro.hk_hold(ts_code=stock_code, limit=1, fields=["ratio"])
        if df.empty:
            return 0.0
        ratio = pd.to_numeric(df['ratio'].iloc[0], errors='coerce')
        return ratio if ratio is not None else 0.0
    except Exception as e:
        logging.error(f"{stock_code} 获取北向占比出错: {e}")
        st.error(f"获取 {stock_code} 的北向占比数据时出错，请查看 error.log。")
        return 0.0

def fetch_circ_mv_and_volume_ratio(stock_code):
    """
    获取股票的流通市值(circ_mv, 单位: 万) 和 量比(volume_ratio)
    若没数据则返回 (0.0, 0.0)
    """
    try:
        df = pro.daily_basic(ts_code=stock_code, limit=1, fields=["circ_mv", "volume_ratio"])
        if df.empty:
            return 0.0, 0.0
        circ_mv = pd.to_numeric(df['circ_mv'].iloc[0], errors='coerce')
        volume_ratio = pd.to_numeric(df['volume_ratio'].iloc[0], errors='coerce')
        circ_mv = circ_mv if circ_mv is not None else 0.0
        volume_ratio = volume_ratio if volume_ratio is not None else 0.0
        return circ_mv, volume_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取 daily_basic 出错: {e}")
        st.error(f"获取 {stock_code} 的 daily_basic 数据时出错，请查看 error.log。")
        return 0.0, 0.0

def fetch_hm_detail_5days(stock_code, trade_cal_df):
    """
    获取近5个交易日的游资数据（hm_detail），包含 buy_amount(万), sell_amount(万), net_amount(万)
    返回 (df_merged, yz_5d_sum, True/False)
    """
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return None, 0.0, False

    last_day = open_days[-1]
    df_test = pro.hm_detail(ts_code=stock_code, trade_date=last_day)
    if df_test.empty:
        if len(open_days) >= 2:
            last_day = open_days[-2]
        else:
            return None, 0.0, False

    if last_day not in open_days:
        return None, 0.0, False

    idx = open_days.index(last_day)
    if idx - 4 < 0:
        five_days = open_days[0: idx + 1]
    else:
        five_days = open_days[idx - 4: idx + 1]

    frames = []
    for d in five_days:
        df_hm = pro.hm_detail(
            ts_code=stock_code,
            trade_date=d,
            fields=["trade_date", "buy_amount", "sell_amount", "net_amount", "hm_name"]
        )
        if not df_hm.empty:
            for col in ["buy_amount", "sell_amount", "net_amount"]:
                df_hm[col] = pd.to_numeric(df_hm[col], errors='coerce').fillna(0) / 10000.0
            df_hm["trade_date"] = df_hm["trade_date"].astype(str)
            frames.append(df_hm)
        time.sleep(0.05)

    if not frames:
        return None, 0.0, False

    df_merged = pd.concat(frames, ignore_index=True)
    yz_5d_sum = df_merged["net_amount"].sum()
    return df_merged, yz_5d_sum, True

def get_recent_kpl_concept_cons(trade_cal_df, max_tries=10):
    """
    从最近的 max_tries 个交易日内，获取最近成功的 kpl_concept_cons 数据 (已改为 con_code)。
    """
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return pd.DataFrame(columns=['name', 'con_code', 'hot_num', 'desc'])

    recent_days = open_days[-max_tries:]
    for trade_date in reversed(recent_days):
        try:
            df_kpl = pro.kpl_concept_cons(
                trade_date=trade_date,
                fields=['name', 'con_code', 'hot_num', 'desc']
            )
            if not df_kpl.empty:
                st.info(f"成功获取到 {trade_date} 的 kpl_concept_cons 数据，共 {len(df_kpl)} 条。")
                df_kpl['trade_date'] = trade_date
                return df_kpl
            else:
                st.info(f"{trade_date} 的 kpl_concept_cons 数据为空，回退前一交易日...")
        except Exception as e:
            logging.error(f"获取 kpl_concept_cons 出错, 交易日 {trade_date}: {e}")
            st.error(f"获取 kpl_concept_cons 出错, 交易日 {trade_date}，请查看 error.log。")
        time.sleep(0.2)
    return pd.DataFrame(columns=['name', 'con_code', 'hot_num', 'desc'])

def aggregate_concept_info(df_kpl):
    """
    按 con_code 聚合 name/desc/hot_num
    """
    if df_kpl.empty:
        return pd.DataFrame(columns=['con_code', 'total_hot_num', 'combined_name', 'combined_desc'])

    df_kpl['hot_num'] = pd.to_numeric(df_kpl['hot_num'], errors='coerce').fillna(0)

    grouped = df_kpl.groupby('con_code').agg({
        'name': lambda x: ';'.join(sorted(set(x.dropna()))),
        'desc': lambda x: ';'.join(sorted(set(x.dropna()))),
        'hot_num': 'sum'
    }).reset_index()

    grouped.rename(columns={
        'name': 'combined_name',
        'desc': 'combined_desc',
        'hot_num': 'total_hot_num'
    }, inplace=True)

    return grouped

def zscore(series: pd.Series) -> pd.Series:
    mean_val = series.mean()
    std_val = series.std()
    if std_val == 0:
        return pd.Series([0] * len(series), index=series.index)
    return (series - mean_val) / std_val

def display_5days_hm_detail(stock_code, stock_name, df_5days):
    st.markdown(f"### {stock_code} ({stock_name}) 近5日游资交易明细")
    if df_5days is None or df_5days.empty:
        st.info("没有游资交易数据。")
        return

    df_display = df_5days.copy()
    df_display.rename(columns={
        "trade_date": "交易日期",
        "buy_amount": "买入金额(万)",
        "sell_amount": "卖出金额(万)",
        "net_amount": "净额(万)",
        "hm_name": "游资名称"
    }, inplace=True)

    # 转换为整数，去除小数
    df_display["买入金额(万)"] = df_display["买入金额(万)"].astype(int)
    df_display["卖出金额(万)"] = df_display["卖出金额(万)"].astype(int)
    df_display["净额(万)"] = df_display["净额(万)"].astype(int)

    # 计算净额合计
    total_net = df_display["净额(万)"].sum()
    total_row = pd.DataFrame({
        "交易日期": ["合计"],
        "买入金额(万)": [0],   # 用 0 替代空字符串
        "卖出金额(万)": [0],   # 用 0 替代空字符串
        "净额(万)": [total_net],
        "游资名称": [""]
    })
    df_display = pd.concat([df_display, total_row], ignore_index=True)

    # 居中表格内容
    st.markdown("<style>table{width:100%; text-align: center;}</style>", unsafe_allow_html=True)
    st.dataframe(df_display, use_container_width=True, hide_index=True)


def fetch_margin_6d_ratio(stock_code, circ_mv, trade_cal_df):
    """
    计算近6日的融资融券净流入占比(%)。
    """
    try:
        df = pro.margin_detail(ts_code=stock_code, limit=6, fields=["rzye", "rqye", "trade_date"])
        if df.empty or len(df) < 6:
            return 0.0

        df['rzye'] = pd.to_numeric(df['rzye'], errors='coerce').fillna(0.0) / 10000.0
        df['rqye'] = pd.to_numeric(df['rqye'], errors='coerce').fillna(0.0) / 10000.0

        df = df.sort_values(by='trade_date').reset_index(drop=True)
        df['net_inflow'] = (df['rzye'].diff()) - (df['rqye'].diff())
        net_inflow_5d = df['net_inflow'].iloc[1:6].sum()

        if circ_mv == 0:
            return 0.0

        margin_ratio = (net_inflow_5d / circ_mv) * 100.0
        return margin_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取近6日融资融券数据出错: {e}")
        st.error(f"获取 {stock_code} 的融资融券数据时出错，请查看 error.log。")
        return 0.0

# -------------------------- 主流程 --------------------------

def get_cache_key(params: dict) -> str:
    """
    根据参数字典生成唯一的缓存键（使用 JSON 字符串，保证键按顺序排序）
    """
    return json.dumps(params, sort_keys=True)

def main():
    st.title("放量题材股票筛选与评分系统")
    st.markdown(""" 
    本系统将基于技术面、资金流、机构、北向、游资等数据进行股票筛选，并计算 AI 评分， 
    最终展示评分系统和近5日游资交易明细。 
    """)

    # ==================== 参数设置 ====================
    default_shareholder_pool = st.text_input("香港中央结算股东池", "date/股东.txt")
    extra_pools_input = st.text_input("可多选间隔空格：date/涨停板.txt date/游资.txt date/RSI选股.txt "
                                      "date/机构调研.txt date/扣非.txt","date/成分股.txt")
    concept_codes_input = st.text_input("题材代码（多个代码用空格分隔，留空则不使用）", "")
    run_button = st.button("开始筛选")

    # 构建当前参数字典
    current_params = {
        "default_shareholder_pool": default_shareholder_pool,
        "extra_pools_input": extra_pools_input,
        "concept_codes_input": concept_codes_input
    }
    cache_key = get_cache_key(current_params)

    # 如果缓存字典不存在，则初始化
    if "flts_result_cache" not in st.session_state:
        st.session_state["flts_result_cache"] = {}

    # ==================== 缓存判断 ====================
    if run_button:
        if cache_key in st.session_state["flts_result_cache"]:
            st.info("加载缓存结果...")
            cached = st.session_state["flts_result_cache"][cache_key]
            final_df = cached["final_df"]
            hm_detail_map = cached["hm_detail_map"]

            st.subheader("股票评分统计表")
            st.markdown("<style>table{width:100%; text-align: center;}</style>", unsafe_allow_html=True)
            st.dataframe(
                final_df[['股票代码', '股票名称', '当日净值占比(%)', '5日主力净值占比(%)', '游资净额占比(%)',
                          '大单占比(%)', '机构占比(%)', '北向占比(%)', '量比', '融资融券净流入占比(%)', 'AI评分',
                          '人气值',
                          '题材名称', '概念标签', '描述']],
                use_container_width=True, hide_index=True
            )

            st.subheader("各股票近5日游资交易明细")
            # 按“总游资净额”降序排列
            sorted_df = final_df.sort_values(by="总游资净额", ascending=False)
            for idx, row in sorted_df.iterrows():
                stock_code = row['股票代码']
                stock_name = row['股票名称']
                total_yz = row['总游资净额']
                # 从缓存中获取该股票的游资明细
                if stock_code in hm_detail_map:
                    df_5d, stock_name_cached, total_yz_cached = hm_detail_map[stock_code]
                else:
                    df_5d = None
                with st.expander(f"{stock_code} ({stock_name}) - 总游资净额: {total_yz:.2f}"):
                    display_5days_hm_detail(stock_code, stock_name, df_5d)

            selected_stock_text = "\n".join(final_df['股票代码'].tolist())
            st.download_button(
                label="下载筛选后股票列表",
                data=selected_stock_text,
                file_name="放量题材.txt",
                mime="text/plain"
            )
            st.success(f"\n强势题材股票总数: {len(final_df)}")
            return

        # ==================== 开始执行筛选流程 ====================
        st.info("开始执行股票筛选流程，请耐心等待……")
        progress_text = st.empty()

        # 1) 获取股票基本信息映射
        progress_text.text("正在获取股票基本信息……")
        stock_basic_mapping = fetch_stock_basic()
        if not stock_basic_mapping:
            st.error("无法获取股票基本信息，程序终止。")
            return

        # 2) 加载股东股票池
        progress_text.text("加载股东股票池文件……")
        shareholder_stock_pool = load_stock_pool(default_shareholder_pool)
        if not shareholder_stock_pool:
            st.error("股东股票池为空，程序终止。")
            return

        # 3) 额外股票池
        if extra_pools_input.strip():
            extra_file_paths = extra_pools_input.strip().split()
            st.info("加载额外股票池文件并计算并集……")
            extra_stock_pool = get_union_stock_pools(extra_file_paths)
            if not extra_stock_pool:
                st.error("额外股票池并集为空，无法与股东股票池计算交集。程序终止。")
                return
            selected_stocks_intersection = get_intersection_with_shareholder(extra_stock_pool, shareholder_stock_pool)
        else:
            st.info("未输入额外股票池文件，程序终止。")
            return

        if not selected_stocks_intersection:
            st.error("最终股票池为空，程序终止。")
            return

        # 4) 输入题材代码（选填）
        concept_date = None
        if concept_codes_input.strip():
            concept_codes = [code.strip() for code in concept_codes_input.split() if code.strip()]
            if concept_codes:
                trade_cal_df = get_trade_calendar()
                if trade_cal_df.empty:
                    st.error("交易日历获取失败，程序终止。")
                    return

                recent_trade_days = get_latest_trade_days(trade_cal_df, max_tries=10)
                if not recent_trade_days:
                    st.error("未找到有效的交易日，程序终止。")
                    return

                all_concept_stocks = set()
                valid_concept_codes = []
                for trade_date in reversed(recent_trade_days):
                    temp_concept_stocks = set()
                    temp_valid_concept_codes = []
                    for concept_code in concept_codes:
                        stocks = get_component_stocks(concept_code, trade_date)
                        if stocks:
                            temp_concept_stocks.update(stocks)
                            temp_valid_concept_codes.append(concept_code)
                    if temp_concept_stocks:
                        all_concept_stocks = temp_concept_stocks
                        valid_concept_codes = temp_valid_concept_codes
                        concept_date = trade_date
                        st.success(f"成功获取到 {concept_date} 的成分股数据。")
                        break
                    else:
                        st.info(f"{trade_date} 的成分股数据为空，尝试回退到上一个交易日。")
                    time.sleep(0.1)
                if all_concept_stocks:
                    selected_stocks_intersection = selected_stocks_intersection & all_concept_stocks
                    st.info(f"题材代码与股票池交集后的股票池总数: {len(selected_stocks_intersection)}")
            else:
                st.info("未检测到有效的题材代码输入。")
        else:
            st.info("未输入题材代码，使用现有股票池进行筛选。")

        if not selected_stocks_intersection:
            st.error("最终股票池为空，程序终止。")
            return

        # 5) 技术面筛选
        if concept_date:
            start_date_dt = dt.datetime.strptime(concept_date, '%Y%m%d') - dt.timedelta(days=120)
        else:
            start_date_dt = dt.datetime.today() - dt.timedelta(days=120)
        start_date = start_date_dt.strftime('%Y%m%d')
        end_date = concept_date if concept_date else dt.datetime.today().strftime('%Y%m%d')

        st.info("开始进行技术面筛选……")
        final_selected_stocks = []
        progress_bar = st.progress(0)
        selected_list = list(selected_stocks_intersection)
        total = len(selected_list)
        for idx, stock_code in enumerate(selected_list):
            result = technical_stock_selection(stock_code, start_date, end_date)
            if result:
                final_selected_stocks.append(result)
            progress_bar.progress((idx + 1) / total)
            time.sleep(0.05)
        progress_bar.empty()
        if not final_selected_stocks:
            st.error("没有符合技术面条件的股票。程序终止。")
            return

        # 6) 获取概念/人气值数据
        trade_cal_df = get_trade_calendar()
        if trade_cal_df.empty:
            st.error("交易日历获取失败，跳过 kpl_concept_cons 数据获取。")
            df_kpl_final = pd.DataFrame()
        else:
            df_kpl = get_recent_kpl_concept_cons(trade_cal_df, max_tries=10)
            if df_kpl.empty:
                st.info("在最近的交易日范围内，kpl_concept_cons 数据均为空。")
                df_kpl_final = pd.DataFrame()
            else:
                df_kpl_final = aggregate_concept_info(df_kpl)

        concept_info_dict = {}
        if not df_kpl_final.empty:
            for idx, row in df_kpl_final.iterrows():
                code = row['con_code']
                concept_info_dict[code] = {
                    'hot_num': row['total_hot_num'],
                    'concept_names': row['combined_name'],
                    'desc': row['combined_desc']
                }

        # 7) 收集最终数据：资金流、北向、流通市值、量比、近5日游资数据及融资融券数据
        final_save_path = os.path.join("date", "放量题材.txt")

        final_data = []
        # hm_detail_map 将保存：(df_5days, stock_name, 总游资净额)
        hm_detail_map = {}

        st.info("开始获取资金流、北向、流通市值、量比、近5日游资数据及融资融券数据……")
        for stock_code in final_selected_stocks:
            stock_name = stock_basic_mapping.get(stock_code, '未知')

            # (1) 流通市值 & 量比
            circ_mv, volume_ratio = fetch_circ_mv_and_volume_ratio(stock_code)
            if circ_mv <= 0:
                continue

            # (2) 资金流数据
            net_d5_amount, net_amount, buy_lg_amount_rate = fetch_moneyflow_data(stock_code)
            net_d5_amount = net_d5_amount if net_d5_amount else 0.0
            net_amount = net_amount if net_amount else 0.0
            buy_lg_amount_rate = buy_lg_amount_rate if buy_lg_amount_rate else 0.0

            ratio_today = (net_amount / circ_mv) * 100
            ratio_5day = (net_d5_amount / circ_mv) * 100

            # (3) 机构 & 北向数据
            hold_ratio = fetch_institution_data(stock_code)
            northbound_ratio = fetch_northbound_ratio(stock_code)

            # (4) 近5日游资数据 —— 这里返回 (df_5days, yz_5d_sum, has_data_5d)
            df_5days, yz_5d_sum, has_data_5d = fetch_hm_detail_5days(stock_code, trade_cal_df)
            if not has_data_5d:
                continue
            yz_5d_ratio = (yz_5d_sum / circ_mv) * 100
            # 将总游资净额存入 hm_detail_map，便于后续展示时使用
            hm_detail_map[stock_code] = (df_5days, stock_name, yz_5d_sum)

            # (5) 人气值、题材名称、描述
            hot_num_val = 0
            concept_name_val = "无"
            desc_val = "无"
            if stock_code in concept_info_dict:
                hot_num_val = concept_info_dict[stock_code]['hot_num']
                concept_name_val = concept_info_dict[stock_code]['concept_names']
                desc_val = concept_info_dict[stock_code]['desc']

            # (6) 概念标签
            concepts = get_stock_concepts(stock_code)
            if len(concepts) > 30:
                concepts = concepts[:27] + '...'

            # (7) 融资融券净流入占比
            margin_ratio = fetch_margin_6d_ratio(stock_code, circ_mv, trade_cal_df)

            final_data.append({
                '股票代码': stock_code,
                '股票名称': stock_name,
                '当日净值占比(%)': float(ratio_today),
                '5日主力净值占比(%)': float(ratio_5day),
                '游资净额占比(%)': float(yz_5d_ratio),
                '大单占比(%)': float(buy_lg_amount_rate),
                '机构占比(%)': float(hold_ratio),
                '北向占比(%)': float(northbound_ratio),
                '量比': float(volume_ratio),
                '融资融券净流入占比(%)': float(margin_ratio),
                '人气值': float(hot_num_val),
                '题材名称': concept_name_val,
                '概念标签': concepts,
                '描述': desc_val,
                '总游资净额': float(yz_5d_sum)
            })

        if not final_data:
            st.error("5日无游资数据或流通市值=0，最终无可选股票。程序结束。")
            return

        # ========== 构建 DataFrame 并计算 Z-Score 与 AI评分 ==========
        final_df = pd.DataFrame(final_data)

        numeric_cols = [
            '当日净值占比(%)', '5日主力净值占比(%)', '游资净额占比(%)',
            '大单占比(%)', '机构占比(%)', '北向占比(%)', '量比', '人气值',
            '融资融券净流入占比(%)'
        ]
        for col in numeric_cols:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)

        final_df['z_ratio_today'] = zscore(final_df['当日净值占比(%)'])
        final_df['z_ratio_5day'] = zscore(final_df['5日主力净值占比(%)'])
        final_df['z_yz_5d_ratio'] = zscore(final_df['游资净额占比(%)'])
        final_df['z_dd_ratio'] = zscore(final_df['大单占比(%)'])
        final_df['z_jg_ratio'] = zscore(final_df['机构占比(%)'])
        final_df['z_bx_ratio'] = zscore(final_df['北向占比(%)'])
        final_df['z_vol_ratio'] = zscore(final_df['量比'])
        final_df['z_hot_num'] = zscore(final_df['人气值'])
        final_df['z_margin_ratio'] = zscore(final_df['融资融券净流入占比(%)'])

        final_df['AI评分'] = (
                1.0 * final_df['z_ratio_today'] +
                1.2 * final_df['z_ratio_5day'] +
                1.5 * final_df['z_yz_5d_ratio'] +
                1.0 * final_df['z_dd_ratio'] +
                0.8 * final_df['z_jg_ratio'] +
                0.8 * final_df['z_bx_ratio'] +
                0.6 * final_df['z_vol_ratio'] +
                0.1 * final_df['z_hot_num'] +
                1.0 * final_df['z_margin_ratio']
        )

        for col in ['当日净值占比(%)', '5日主力净值占比(%)', '游资净额占比(%)',
                    '大单占比(%)', '机构占比(%)', '北向占比(%)', '量比', '人气值',
                    '融资融券净流入占比(%)', 'AI评分']:
            final_df[col] = final_df[col].round(1)

        # ==================== 展示最终结果 ====================
        st.success(f"\n强势题材股票总数: {len(final_df)}")
        st.subheader("股票评分统计表")
        st.markdown("<style>table{width:100%; text-align: center;}</style>", unsafe_allow_html=True)
        st.dataframe(
            final_df[['股票代码', '股票名称', '当日净值占比(%)', '5日主力净值占比(%)', '游资净额占比(%)',
                      '大单占比(%)', '机构占比(%)', '北向占比(%)', '量比', '融资融券净流入占比(%)', 'AI评分', '人气值',
                      '题材名称', '概念标签', '描述']],
            use_container_width=True, hide_index=True
        )

        st.subheader("各股票近5日游资交易明细")
        # 对股票按照“总游资净额”降序排列
        sorted_df = final_df.sort_values(by="总游资净额", ascending=False)
        for idx, row in sorted_df.iterrows():
            stock_code = row['股票代码']
            stock_name = row['股票名称']
            total_yz = row['总游资净额']
            if stock_code in hm_detail_map:
                df_5d, stock_name_cached, total_yz_cached = hm_detail_map[stock_code]
            else:
                df_5d = None
            with st.expander(f"{stock_code} ({stock_name}) - 总游资净额: {total_yz:.2f}"):
                display_5days_hm_detail(stock_code, stock_name, df_5d)

        selected_stock_text = "\n".join(final_df['股票代码'].tolist())
        st.download_button(
            label="下载筛选后股票列表",
            data=selected_stock_text,
            file_name="放量题材.txt",
            mime="text/plain"
        )


        # 将此次结果缓存到字典中，使用当前参数生成的 cache_key
        st.session_state["flts_result_cache"][cache_key] = {
            "final_df": final_df,
            "hm_detail_map": hm_detail_map
        }

# 仅在直接运行该模块时执行 main()
if __name__ == "__main__":
    main()




