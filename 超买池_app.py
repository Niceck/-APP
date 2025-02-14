import streamlit as st
import tushare as ts
import pandas as pd
import pandas_ta as ta
import datetime as dt
import os
import logging
import ast

# ------------------------ 基础配置 ------------------------
# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

logging.basicConfig(
    filename='error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# ------------------------ 功能函数 ------------------------
def get_latest_trade_date():
    """获取今天的交易日期，如果今天不是交易日则返回 None"""
    today = dt.datetime.today().strftime('%Y%m%d')
    try:
        df = pro.trade_cal(start_date=today, end_date=today)
        if df.empty or df.iloc[0]['is_open'] == 0:
            return None
        return today
    except Exception as e:
        logging.error(f"获取交易日期失败: {e}")
        return None


def rollback_date(latest_trade_date, max_retries=7):
    """
    回撤日期，最多回撤 max_retries 次，每次回撤一天，直至获取到有效的交易日
    """
    end_date = dt.datetime.strptime(latest_trade_date, "%Y%m%d")
    for _ in range(max_retries):
        end_date -= dt.timedelta(days=1)
        rollback_date_str = end_date.strftime("%Y%m%d")
        try:
            df = pro.trade_cal(start_date=rollback_date_str, end_date=rollback_date_str)
            if not df.empty and df.iloc[0]['is_open'] == 1:
                return rollback_date_str
        except Exception as e:
            logging.error(f"回撤获取交易日期失败: {e}")
            continue
    st.error("超过最大回退次数(7)，仍未获取到有效的交易数据，退出程序。")
    return None

def save_selected_stocks(selected_stocks, filename):
    """
    保存选股结果到文件（仅保存股票代码）
    selected_stocks 为字典列表，每个字典中包含 'ts_code' 键
    """
    try:
        # 确保 'date' 文件夹存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # 写入文件，自动覆盖旧文件
        with open(filename, "w", encoding='utf-8') as file:
            for stock in selected_stocks:
                file.write(f"{stock['ts_code']}\n")

        st.success(f"股票列表已保存到: {filename}")
    except Exception as e:
        logging.error(f"保存选股结果失败: {e}")
        st.error("保存选股结果失败，请查看日志。")




def get_all_a_stocks():
    """
    获取所有 A 股普通股票列表，
    仅筛选 symbol 符合特定格式的股票
    """
    try:
        df_stocks = pro.stock_basic(exchange='', list_status='L',
                                    fields='ts_code,symbol,name,area,industry,list_date')
        # 使用正则筛选普通股票（如000、002、300、600、601、688、9、4、69开头）
        df_stocks = df_stocks[df_stocks['symbol'].str.match(r'^(000|002|300|600|601|688|9|4|69)\d{3}$')]
        st.info(f"总共筛选出的普通股票数量: {len(df_stocks)}")
        return df_stocks
    except Exception as e:
        logging.error(f"获取 A 股列表失败: {e}")
        st.error("获取 A 股列表失败，请查看日志。")
        return pd.DataFrame()


def rsi_strategy(stock_code, start_date, end_date):
    """
    对单个股票使用 RSI 策略：
    1. 获取指定时间区间内的日线数据
    2. 计算 RSI（周期=6），判断最近 3 天 RSI 均大于等于 80，则认为符合条件
    """
    try:
        df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
        if df.empty or 'close' not in df.columns:
            return None
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        if df['close'].isna().sum() > 0:
            return None
        if len(df) < 14:
            return None
        df = df.sort_values(by='trade_date', ascending=True)
        df['RSI'] = talib.RSI(df['close'].values, timeperiod=6)
        recent_rsi = df['RSI'].iloc[-3:]
        if all(rsi >= 80 for rsi in recent_rsi):
            return stock_code
    except Exception as e:
        logging.error(f"{stock_code} 选股出错: {e}")
        return None


def load_local_stock_pool(file_path):
    """加载本地股票池文件，返回股票代码的集合"""
    if not os.path.exists(file_path):
        st.warning(f"本地股票池文件不存在: {file_path}")
        return set()
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            local_stocks = set(line.strip() for line in file if line.strip())
        st.info(f"加载本地股票池，共 {len(local_stocks)} 只股票")
        return local_stocks
    except Exception as e:
        logging.error(f"加载本地股票池失败: {e}")
        st.error("加载本地股票池失败，请查看日志。")
        return set()


def get_stock_name(stock_code):
    """获取指定股票的名称"""
    try:
        df = pro.stock_basic(ts_code=stock_code, fields="ts_code,name")
        if not df.empty:
            return df.iloc[0]['name']
        return "未知名称"
    except Exception as e:
        logging.error(f"获取 {stock_code} 名称失败: {e}")
        return "获取失败"


def get_stock_concepts(stock_code):
    """获取指定股票的 concept 标签，并去重返回字符串"""
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


# ------------------------ 主流程 ------------------------
def main():
    """
    入口函数，实现基于当前参数的缓存机制，不同参数组合各自独立缓存，
    切换参数后不会清除之前计算过的缓存数据，当再次切换回之前的参数时直接加载缓存结果。
    """
    st.title("RSI 策略选股系统")
    st.markdown("""
    该系统基于 Tushare 数据，通过 RSI 指标策略对 A 股股票进行选股，并支持使用本地股票池（例如股东股票池）进行筛选。
    """)

    st.header("参数设置")
    use_local_stock_pool = st.checkbox("使用本地股票池", value=True)
    local_stock_pool_path = st.text_input("优选股东股票池", "date/股东.txt")
    run_button = st.button("开始 RSI 选股")

    # 构造当前参数的唯一标识符
    params_key = f"use_local_stock_pool_{use_local_stock_pool}_path_{local_stock_pool_path}"

    # 初始化缓存字典
    if "rsi_cache" not in st.session_state:
        st.session_state["rsi_cache"] = {}

    if run_button:
        if params_key in st.session_state["rsi_cache"]:
            st.info("加载缓存结果...")
            result_df = st.session_state["rsi_cache"][params_key]
            st.dataframe(result_df, use_container_width=True, hide_index=True)
            csv_data = result_df.to_csv(index=False)
            st.download_button(
                label="下载 RSI 选股结果",
                data=csv_data,
                file_name="RSI选股.txt",
                mime="text/csv"
            )
            return

        st.subheader("RSI 选股处理中……")
        # 1. 获取最新交易日期（优先使用今天，如果今天不是交易日则回撤）
        latest_trade_date = get_latest_trade_date()
        if not latest_trade_date:
            st.info("今天不是交易日，尝试回撤获取交易日")
            latest_trade_date = rollback_date(dt.datetime.today().strftime('%Y%m%d'))

        if not latest_trade_date:
            st.error("无法获取最新交易日，退出程序")
            return
        else:
            st.write(f"**使用交易日期：** {latest_trade_date}")

        # 2. 确定选股时间范围（结束日为最新交易日，起始日回溯 90 天）
        end_date = dt.datetime.strptime(latest_trade_date, '%Y%m%d')
        start_date = end_date - dt.timedelta(days=90)
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')

        # 3. 获取股票池
        if use_local_stock_pool:
            stock_pool = load_local_stock_pool(local_stock_pool_path)
            if not stock_pool:
                st.error("本地股票池为空或加载失败，退出程序")
                return
            else:
                df_stocks = pd.DataFrame({'ts_code': list(stock_pool)})
        else:
            df_stocks = get_all_a_stocks()

        if df_stocks.empty:
            st.error("股票池为空，退出程序")
            return

        selected_stocks_rsi = []
        progress_bar = st.progress(0)
        total = len(df_stocks)
        # 4. 对股票池中每只股票进行 RSI 策略选股
        for i, code in enumerate(df_stocks['ts_code']):
            res = rsi_strategy(code, start_date_str, end_date_str)
            if res:
                stock_name = get_stock_name(res)
                concepts = get_stock_concepts(res)
                selected_stocks_rsi.append({
                    'ts_code': res,
                    'name': stock_name,
                    'concept': concepts
                })
            progress_bar.progress((i + 1) / total)
        progress_bar.empty()

        # 5. 展示并保存结果
        if selected_stocks_rsi:
            st.success("最终符合条件的股票如下：")
            result_df = pd.DataFrame(selected_stocks_rsi).reset_index(drop=True)
            st.dataframe(result_df, use_container_width=True, hide_index=True)

            # 缓存当前参数对应的计算结果
            st.session_state["rsi_cache"][params_key] = result_df

            # 自动保存到相对路径的 'date' 文件夹
            file_name_rsi = "RSI选股.txt"
            file_path_rsi = os.path.join("date", file_name_rsi)  # 使用相对路径

            # 调用保存函数
            save_selected_stocks(selected_stocks_rsi, file_path_rsi)

            # 提供下载按钮（CSV 格式下载）
            csv_data = result_df.to_csv(index=False)
            st.download_button(
                label="下载 RSI 选股结果",
                data=csv_data,
                file_name=file_name_rsi,
                mime="text/csv"
            )
        else:
            st.info("没有股票符合条件，未生成文件。")


if __name__ == "__main__":
    main()
