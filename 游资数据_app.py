import tushare as ts
import pandas as pd
import datetime as dt
import os
import logging
import sys
import time
from tqdm import tqdm  # tqdm 在后台调用，界面上使用 Streamlit 的进度条
import streamlit as st
from git_utils import git_update, git_push  # 导入 Git 操作函数

# ------------------- 全局设置 -------------------
# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

# 配置日志（错误信息写入 error.log）
logging.basicConfig(filename='error.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s:%(message)s')

# 默认参数设置
num_days = 1
default_target_institutions = ['陈小群']
use_institution_filter = False  # 全局变量，是否启用机构过滤


# ------------------- 工具函数 -------------------

def get_latest_trade_date():
    """获取今天是否为交易日，如果是返回今天，否则返回 None"""
    today = dt.datetime.today().strftime('%Y%m%d')
    try:
        df = pro.trade_cal(start_date=today, end_date=today, fields='cal_date,is_open')
        if df.empty or df.iloc[0]['is_open'] == 0:
            return None
        return today
    except Exception as e:
        logging.error(f"获取交易日期失败: {e}")
        return None


def rollback_date(latest_trade_date, max_retries=7):
    """
    回溯找前一个交易日，如果最新交易日获取失败，则往前找（最多 max_retries 次）
    """
    end_date = dt.datetime.strptime(latest_trade_date, "%Y%m%d")
    for _ in range(max_retries):
        end_date -= dt.timedelta(days=1)
        rollback_date_str = end_date.strftime("%Y%m%d")
        try:
            df = pro.trade_cal(start_date=rollback_date_str, end_date=rollback_date_str, fields='cal_date,is_open')
            if not df.empty and df.iloc[0]['is_open'] == 1:
                return rollback_date_str
        except Exception as e:
            logging.error(f"获取交易日历时出错: {e}")
    return None


def save_selected_stocks(selected_ts_codes, file_name):
    """将选中的股票代码保存到指定文件中"""
    # 设置文件路径为相对路径的 'date' 文件夹
    file_path = os.path.join("date", file_name)

    # 确保 'date' 文件夹存在
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        with open(file_path, "w", encoding='utf-8') as file:
            for ts_code in selected_ts_codes:
                file.write(f"{ts_code}\n")
        logging.info(f"选定的股票代码已成功保存到: {file_path}")
    except Exception as e:
        logging.error(f"保存选定股票代码时出错: {e}")


def get_trade_calendar(required_days):
    """
    获取足够的交易日历（跨年度），返回交易日字符串列表
    """
    today = dt.datetime.today()
    today_str = today.strftime('%Y%m%d')
    all_cal_dates = set()
    current_year = today.year
    years_back = 2  # 回溯2年
    for year in range(current_year, current_year - years_back, -1):
        start_of_year = dt.datetime(year, 1, 1).strftime('%Y%m%d')
        try:
            df = pro.trade_cal(exchange='', start_date=start_of_year, end_date=today_str, fields='cal_date,is_open')
            df = df[(df['is_open'] == 1) & (df['cal_date'] <= today_str)]
            all_cal_dates.update(df['cal_date'].tolist())
        except Exception as e:
            logging.error(f"获取交易日历 for year {year} 时出错: {e}")
            sys.exit()
    all_cal_dates = sorted(all_cal_dates)
    if len(all_cal_dates) < required_days:
        logging.error(f"交易日历不足 {required_days} 天。")
        sys.exit()
    return all_cal_dates


def get_last_n_trading_days(available_days, n=10):
    """从 available_days 中取出最后 n 个交易日"""
    if len(available_days) < n:
        logging.error(f"可用交易日数量不足 {n} 天。")
        sys.exit()
    selected_days = available_days[-n:]
    return selected_days


def split_list(lst, n):
    """将列表 lst 按每组 n 个元素分割"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_hm_detail_by_days(dates, batch_size=500):
    """
    根据指定日期列表获取游资净买入数据，并对数据量较大的日期采用批次处理
    返回合并后的 DataFrame
    """
    all_data = pd.DataFrame()
    progress_bar = st.progress(0)
    total = len(dates)
    for i, d in enumerate(dates):
        try:
            df = pro.hm_detail(start_date=d, end_date=d, fields=["ts_code", "hm_name", "trade_date", "net_amount"])
            if df is not None and not df.empty:
                if len(df) >= 2000:
                    unique_ts_codes = df['ts_code'].unique().tolist()
                    for batch in split_list(unique_ts_codes, batch_size):
                        ts_code_str = ','.join(batch)
                        try:
                            df_batch = pro.hm_detail(start_date=d, end_date=d, ts_code=ts_code_str,
                                                     fields=["ts_code", "hm_name", "net_amount"])
                            if df_batch is not None and not df_batch.empty:
                                all_data = pd.concat([all_data, df_batch], ignore_index=True)
                        except Exception as e:
                            logging.error(f"{d} 批次 {ts_code_str} 获取hm_detail出错: {e}")
                else:
                    all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            logging.error(f"{d} 获取hm_detail出错: {e}")
        progress_bar.progress((i + 1) / total)
    return all_data


def filter_by_institutions(df, target_institutions):
    """
    根据目标机构过滤数据（若 use_institution_filter 为 True）
    假设 hm_name 字符串中各机构用逗号分隔
    """
    if df.empty:
        return df
    if not use_institution_filter or not target_institutions:
        return df
    df['hm_list'] = df['hm_name'].apply(lambda x: [name.strip() for name in x.split(',')])
    mask = df['hm_list'].apply(lambda hm_list: bool(set(hm_list).intersection(target_institutions)))
    df_filtered = df[mask].copy()
    df_filtered.drop(columns='hm_list', inplace=True)
    return df_filtered


def get_stock_names(ts_codes):
    """
    批量获取股票名称，返回 ts_code 到 name 的字典映射
    """
    batch_size = 500
    ts_name_dict = {}
    batches = list(split_list(ts_codes, batch_size))
    progress_bar = st.progress(0)
    total_batches = len(batches)
    for i, batch in enumerate(batches):
        try:
            df_basic = pro.stock_basic(ts_code=','.join(batch), fields='ts_code,name')
            if df_basic is not None and not df_basic.empty:
                ts_name_dict.update(dict(zip(df_basic['ts_code'], df_basic['name'])))
        except Exception as e:
            logging.error(f"获取股票名称时出错: {e}")
        progress_bar.progress((i + 1) / total_batches)
    return ts_name_dict


# ------------------- 主程序 -------------------
def main():
    global use_institution_filter  # 在函数最开始声明全局变量
    st.title("游资净买入数据分析")
    st.markdown("本应用用于获取指定交易日内的游资净买入数据，并根据目标机构过滤、净买入金额等条件筛选股票。")

    # 侧边栏参数设置
    st.sidebar.header("参数设置")
    num_days_input = st.sidebar.number_input("请输入要获取的交易天数", min_value=1, max_value=300, value=num_days,
                                             step=1)
    target_institutions_input = st.sidebar.text_input("请输入目标机构（多个机构用逗号分隔）",
                                                      value=",".join(default_target_institutions))
    use_filter = st.sidebar.checkbox("启用机构过滤", value=use_institution_filter)

    # 更新全局变量
    use_institution_filter = use_filter
    try:
        num_days_value = int(num_days_input)
    except Exception:
        num_days_value = num_days
    target_institutions = [inst.strip() for inst in target_institutions_input.split(",") if inst.strip()]

    if st.button("开始分析"):
        # 1. 获取最新交易日期
        with st.spinner("正在获取最新交易日期..."):
            latest_trade_date = get_latest_trade_date()
        if not latest_trade_date:
            latest_trade_date = rollback_date(dt.datetime.today().strftime('%Y%m%d'))
        if not latest_trade_date:
            st.error("无法获取最新的交易日期，程序退出。")
            return
        st.success(f"最新交易日期：{latest_trade_date}")

        # 2. 获取足够的交易日历
        available_days = get_trade_calendar(required_days=num_days_value)
        last_days = get_last_n_trading_days(available_days, n=num_days_value)
        st.write(f"分析交易日期：{last_days}")

        # 3. 获取游资净买入数据
        st.info("正在获取游资净买入数据，请稍候...")
        hm_data = fetch_hm_detail_by_days(last_days)
        if hm_data.empty:
            st.error("未获取到任何游资净买入数据，程序退出。")
            return
        st.success("游资净买入数据获取完成。")

        # 4. 根据目标机构过滤数据（若启用过滤）
        filtered_data = filter_by_institutions(hm_data, target_institutions)
        if filtered_data.empty:
            st.error("经过机构过滤后未获取到任何数据，程序退出。")
            return

        # 5. 获取筛选后的股票代码及对应股票名称
        selected_ts_codes_by_institutions = filtered_data['ts_code'].unique().tolist()
        st.info(f"筛选后股票数量：{len(selected_ts_codes_by_institutions)}")

        st.info("正在获取股票名称...")
        ts_name_dict = get_stock_names(selected_ts_codes_by_institutions)

        # 6. 构建结果
        results = []
        for code in selected_ts_codes_by_institutions:
            ts_name = ts_name_dict.get(code, "未知名称")
            trade_dates = filtered_data[filtered_data['ts_code'] == code]['trade_date'].unique()
            trade_dates_str = ', '.join(trade_dates) if len(trade_dates) > 0 else '无日期信息'
            hm_names = filtered_data[filtered_data['ts_code'] == code]['hm_name'].unique()
            hm_names_str = ', '.join(hm_names) if len(hm_names) > 0 else "无游资信息"
            sum_net_amount_filtered = int(filtered_data[filtered_data['ts_code'] == code]['net_amount'].sum() / 10_000)
            sum_net_amount_all = int(hm_data[hm_data['ts_code'] == code]['net_amount'].sum() / 10_000)
            results.append({
                'ts_code': code,
                '交易日期': trade_dates_str,
                '股票名称': ts_name,
                '游资名称': hm_names_str,
                '本人净额(万)': sum_net_amount_filtered,
                '全部游资净额(万)': sum_net_amount_all
            })

        results_df = pd.DataFrame(results)

        st.subheader("重点关注游资股票")
        st.dataframe(results_df)

        # 7. 筛选本人净额和全部净额均为正的股票
        filtered_stocks = results_df[(results_df['本人净额(万)'] > 0) & (results_df['全部游资净额(万)'] > 0)]
        st.subheader("本人净额和全部净额均为正的股票")
        st.dataframe(filtered_stocks)

        # 8. 保存结果并提供下载
        file_name = "游资.txt"
        file_path = os.path.join("date", file_name)
        final_ts_codes = filtered_stocks['ts_code'].tolist()

        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding='utf-8') as file:
                for ts_code in final_ts_codes:
                    file.write(f"{ts_code}\n")
            st.success(f"已将筛选结果保存到 {file_path}")
        except Exception as e:
            logging.error(f"保存选定股票代码时出错: {e}")
            st.error("保存文件时出错。")

        # 8.1 调用 Git 更新：若文件存在则更新到 Git
        if os.path.exists(file_path):
            git_update(file_path, update_mode="update")
            git_push(branch="main")

        try:
            with open(file_path, "r", encoding='utf-8') as f:
                file_content = f.read()
            st.download_button(
                label="下载筛选结果",
                data=file_content,
                file_name=file_name,
                mime="text/plain"
            )
        except Exception as e:
            logging.error(f"读取保存文件时出错: {e}")
            st.error("读取保存文件时出错。")


if __name__ == "__main__":
    main()
