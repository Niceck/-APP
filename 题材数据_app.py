import time
import tushare as ts
import pandas as pd
import datetime as dt
import logging
import os
import streamlit as st

# ====================  全局设置  ====================

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')

logging.basicConfig(
    filename='script.log',
    level=logging.INFO,  # 设置为 INFO 级别
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


# ====================  工具函数  ====================



def get_last_n_trade_dates(n=10):
    """获取最近的 n 个交易日列表，返回形如 ['20250109','20250108', …]"""
    try:
        today = dt.datetime.today()
        today_str = today.strftime('%Y%m%d')
        trade_cal = pro.trade_cal(exchange='', end_date=today_str, is_open=1)
        if trade_cal.empty:
            logging.error("获取交易日历失败，返回空数据")
            return []
        trade_cal = trade_cal.sort_values(by='cal_date', ascending=False)
        trade_dates = trade_cal['cal_date'].head(n).tolist()
        return trade_dates
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
        df_themes = pro.kpl_concept(
            trade_date=trade_date,
            ts_code="",
            name="",
            limit="",
            offset="",
            fields=["trade_date", "ts_code", "name", "z_t_num", "up_num"]
        )
        if df_themes.empty:
            logging.warning(f"当天({trade_date}) kpl_concept 接口返回空")
            return pd.DataFrame()

        for col in ['z_t_num', 'up_num']:
            if col not in df_themes.columns:
                logging.error(f"'{col}' 不在题材数据中")
                return pd.DataFrame()
            df_themes[col] = pd.to_numeric(df_themes[col], errors='coerce').fillna(0)

        return df_themes

    except Exception as e:
        logging.error(f"Error fetching themes for date {trade_date}: {e}")
        return pd.DataFrame()


def calculate_avg(df_all_themes):
    """
    计算5日和10日的平均涨停数、升温值
    返回包含：
        [ts_code, avg_z_t_num_5, avg_z_t_num_10, avg_up_num_5, avg_up_num_10]
    的 DataFrame
    """
    try:
        df_all_themes = df_all_themes.sort_values(by=['ts_code', 'trade_date'])
        df_all_themes['avg_z_t_num_5'] = df_all_themes.groupby('ts_code')['z_t_num'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df_all_themes['avg_z_t_num_10'] = df_all_themes.groupby('ts_code')['z_t_num'].transform(
            lambda x: x.rolling(window=10, min_periods=1).mean()
        )
        df_all_themes['avg_up_num_5'] = df_all_themes.groupby('ts_code')['up_num'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df_all_themes['avg_up_num_10'] = df_all_themes.groupby('ts_code')['up_num'].transform(
            lambda x: x.rolling(window=10, min_periods=1).mean()
        )

        df_avg = df_all_themes.groupby('ts_code').tail(1)
        df_avg = df_avg[[
            'ts_code',
            'avg_z_t_num_5', 'avg_z_t_num_10',
            'avg_up_num_5', 'avg_up_num_10'
        ]]
        return df_avg
    except Exception as e:
        logging.error(f"Error calculating averages: {e}")
        return pd.DataFrame()


def filter_themes(df_latest_themes, df_avg):
    """
    筛选“近期最强题材”和“近期升温题材”。
    参数：
        df_latest_themes: 最新交易日所有题材数据 (含列 ts_code, name, z_t_num, up_num)
        df_avg: 各题材的 5日/10日 均值 (avg_z_t_num_5, avg_z_t_num_10, avg_up_num_5, avg_up_num_10)
    返回：df_filtered_z, df_filtered_up 两个 DataFrame
    """
    try:
        df_combined = pd.merge(df_latest_themes, df_avg, on='ts_code', how='left')

        # 近期最强
        df_filtered_z = df_combined[
            (df_combined['z_t_num'] > df_combined['avg_z_t_num_5']) &
            (df_combined['avg_z_t_num_5'] > df_combined['avg_z_t_num_10'])
            ].sort_values(by='z_t_num', ascending=False).head(5).reset_index(drop=True)

        # 近期升温
        df_filtered_up = df_combined[
            (df_combined['up_num'] > df_combined['avg_up_num_5']) &
            (df_combined['avg_up_num_5'] > df_combined['avg_up_num_10'])
            ].sort_values(by='up_num', ascending=False).head(5).reset_index(drop=True)

        # 重命名成中文
        rename_dict = {
            'ts_code': ' 题材代码',
            'name': ' 题材名称',
            'z_t_num': '   涨停数',
            'up_num': '   升温值',
            'avg_z_t_num_5': 'ma5涨停',
            'avg_z_t_num_10': 'ma10涨停',
            'avg_up_num_5': 'ma5升温',
            'avg_up_num_10': 'ma10升温'
        }
        df_filtered_z = df_filtered_z.rename(columns=rename_dict)
        df_filtered_up = df_filtered_up.rename(columns=rename_dict)

        return df_filtered_z, df_filtered_up
    except Exception as e:
        logging.error(f"Error filtering themes: {e}")
        return pd.DataFrame(), pd.DataFrame()


def get_component_stocks(theme_ts_code, trade_dates):
    """
    获取某个题材在 trade_dates（降序）中最新有数据的成分股列表。
    如果第一个交易日没查到数据，会自动往后找，直到找到为止。

    注意：此处的字段已改为 con_code。
    """
    try:
        for trade_date in trade_dates:
            df_components = pro.kpl_concept_cons(
                ts_code=theme_ts_code,
                trade_date=trade_date,
                fields=["con_code"]
            )
            if not df_components.empty:
                # 使用 con_code 而不是 cons_code
                return df_components['con_code'].dropna().unique().tolist()
        return []
    except Exception as e:
        logging.error(f"Error fetching component stocks for theme {theme_ts_code}: {e}")
        return []


def display_table(df, title):
    """
    在 Streamlit 页面上显示 DataFrame 表格
    """
    st.subheader(title)

    # 去除索引列并保留1位小数
    df = df.round(1).reset_index(drop=True)  # 确保应用了四舍五入和去掉索引

    st.table(df)  # 展示最终处理后的表格



def add_hot_money_count(df, trade_dates, latest_trade_date):
    """
    为 df 的每个题材统计去重后的游资数，并写入「 游资数」列。
    - df: 必须至少包含列 [' 题材代码']
    - trade_dates: 最近 n 个交易日的列表(降序)
    - latest_trade_date: 用于统计游资的参考交易日
    """
    df[' 游资数'] = 0
    st.write("开始获取游资数据...")
    progress_bar = st.progress(0)
    total = len(df)
    for i in range(total):
        row = df.iloc[i]
        theme_code = row[' 题材代码']

        # 1) 获取该题材的成分股
        component_stocks = get_component_stocks(theme_code, trade_dates)

        # 2) 汇总该题材所有成分股在 latest_trade_date 出现的游资名称
        hot_money_names = set()
        for stock_code in component_stocks:
            # 每次调接口前，sleep 0.2 秒，保证 1 分钟最多 300 次
            time.sleep(0.2)
            df_hm = pro.hm_detail(
                trade_date=latest_trade_date,
                ts_code=stock_code,
                fields=["hm_name", "ts_code"]
            )
            if not df_hm.empty:
                for name in df_hm['hm_name'].dropna().unique():
                    hot_money_names.add(name)

        # 3) 写回游资数
        df.at[i, ' 游资数'] = len(hot_money_names)
        progress_bar.progress((i + 1) / total)
    return df


# ====================  主函数  ====================

def main():
    st.title("题材数据分析")
    st.markdown(
        """
        分别筛选出“近期最强题材”与“近期升温题材”，
        并计算每个题材的游资数，同时将各题材对应的成分股写入文件。
        """
    )

    # 检查是否有缓存的结果
    result_key = "theme_analysis_result"  # 定义缓存键名
    if result_key in st.session_state:
        st.write("加载缓存数据...")
        # 使用缓存数据
        result_data = st.session_state[result_key]
        display_cached_results(result_data)
        return

    if st.button("开始分析"):
        try:
            st.info("程序开始执行，请耐心等待……")

            # ===================== 文件保存路径设置 =====================
            output_folder = "date"
            output_file = os.path.join(output_folder, '成分股.txt')

            # 打开文件并写入内容，自动覆盖已有的文件
            with open(output_file, "w", encoding="utf-8") as f:
                # 这里可以添加文件的写入逻辑
                f.write("你的文件内容")
            logging.info(f"文件已保存到: {output_file}")

            # ===================== 获取最近 10 个交易日 =====================
            trade_dates = get_last_n_trade_dates(n=10)
            if not trade_dates:
                st.error("没有获取到有效的交易日期")
                logging.error("没有获取到有效的交易日期")
                return

            # ===================== 获取每日题材数据 =====================
            all_themes_data = []
            progress_bar_themes = st.progress(0)
            for i, trade_date in enumerate(trade_dates):
                df_themes = get_themes_for_date(trade_date)
                if not df_themes.empty:
                    all_themes_data.append(df_themes)
                progress_bar_themes.progress((i + 1) / len(trade_dates))
            if not all_themes_data:
                st.error("没有获取到任何题材数据")
                logging.error("没有获取到任何题材数据")
                return

            # ===================== 合并数据、计算均值 =====================
            df_all_themes = pd.concat(all_themes_data, ignore_index=True)
            logging.info(f"合并后总数据行数: {len(df_all_themes)}")

            df_avg = calculate_avg(df_all_themes)
            if df_avg.empty:
                st.error("计算平均值时出错或结果为空")
                logging.error("计算平均值时出错或结果为空")
                return

            # ===================== 获取最新交易日的题材数据 =====================
            latest_trade_date = df_all_themes['trade_date'].max()
            logging.info(f"Latest trade date: {latest_trade_date}")

            df_latest_themes = df_all_themes[
                df_all_themes['trade_date'] == latest_trade_date
                ][['ts_code', 'name', 'z_t_num', 'up_num']]

            # ===================== 筛选题材 =====================
            df_filtered_z, df_filtered_up = filter_themes(df_latest_themes, df_avg)
            if df_filtered_z.empty and df_filtered_up.empty:
                st.warning("没有符合条件的题材被筛选出来")
                logging.warning("没有符合条件的题材被筛选出来")
                return

            # ===================== 获取游资数据 =====================
            df_filtered_z = add_hot_money_count(df_filtered_z, trade_dates, latest_trade_date)
            df_filtered_up = add_hot_money_count(df_filtered_up, trade_dates, latest_trade_date)

            # 调整列顺序
            new_cols = [
                ' 题材代码',
                ' 题材名称',
                ' 游资数',
                '   涨停数',
                '   升温值',
                'ma5涨停',
                'ma10涨停',
                'ma5升温',
                'ma10升温'
            ]
            df_filtered_z = df_filtered_z[new_cols]
            df_filtered_up = df_filtered_up[new_cols]

            # ===================== 在页面上显示结果 =====================
            display_table(df_filtered_z, "近期最强题材")
            display_table(df_filtered_up, "近期升温题材")

            # ===================== 获取所有题材对应的成分股 =====================
            df_filtered_all = pd.concat([df_filtered_z, df_filtered_up], ignore_index=True)
            df_filtered_all = df_filtered_all.drop_duplicates(subset=[' 题材代码'])

            all_stock_codes = set()
            st.write("正在获取各题材的成分股……")
            progress_bar_components = st.progress(0)
            total_themes = len(df_filtered_all[' 题材代码'])
            for i, theme_ts_code in enumerate(df_filtered_all[' 题材代码']):
                component_stocks = get_component_stocks(theme_ts_code, trade_dates)
                all_stock_codes.update(component_stocks)
                progress_bar_components.progress((i + 1) / total_themes)

            st.write(f"获取到的成分股总数: {len(all_stock_codes)}")
            logging.info(f"Total unique component stocks collected: {len(all_stock_codes)}")

            # ===================== 写入文件 =====================
            if os.path.exists(output_file):
                os.remove(output_file)
                logging.info(f"已删除存在的文件: {output_file}")

            with open(output_file, 'w', encoding='utf-8') as f:
                for code in sorted(all_stock_codes):
                    f.write(f"{code}\n")
            logging.info(f"Component stock codes have been written to {output_file}")
            st.success(f"成分股文件已保存至：{output_file}")

            # 将分析结果缓存到 session_state
            st.session_state[result_key] = {
                'filtered_z': df_filtered_z,
                'filtered_up': df_filtered_up,
                'all_stock_codes': all_stock_codes,
                'output_file': output_file
            }

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            st.error(f"程序执行出错：{e}")

# 显示缓存的结果
def display_cached_results(result_data):
    display_table(result_data['filtered_z'], "近期最强题材")
    display_table(result_data['filtered_up'], "近期升温题材")
    st.write(f"成分股文件已保存至：{result_data['output_file']}")
    st.write(f"获取到的成分股总数: {len(result_data['all_stock_codes'])}")
