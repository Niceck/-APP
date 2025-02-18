import time
import tushare as ts
import pandas as pd
import datetime as dt
import logging
import os
import streamlit as st
import plotly.express as px

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

# 定义全局颜色标准
HOT_MONEY_COLOR_SCALE = px.colors.sequential.Blues

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
    如果第一个日期无数据，则尝试后续日期（回撤一天）。
    """
    try:
        for t_date in trade_dates:
            logging.info(f"Fetching component stocks for theme {theme_ts_code} on {t_date}")
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
    theme_to_components = {}
    for idx, row in df.iterrows():
        theme_code = row['ts_code']
        trade_dates_to_try = [latest_date]
        if fallback_date:
            trade_dates_to_try.append(fallback_date)
        comps = get_component_stocks(theme_code, trade_dates_to_try)
        theme_to_components[theme_code] = comps

    all_components = set()
    for comps in theme_to_components.values():
        all_components.update(comps)
    all_components = list(all_components)

    hm_df = get_all_hot_money_details(latest_date, fallback_date)
    if not hm_df.empty:
        hm_df = hm_df[hm_df['ts_code'].isin(all_components)]

    stock_to_hotmoney = {}
    if not hm_df.empty:
        for stock, group in hm_df.groupby('ts_code'):
            stock_to_hotmoney[stock] = set(group['hm_name'].dropna().unique())

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


# ============== 绘制图表函数（Plotly 柱状图） ==============
def plot_themes_bar_chart(df, title, y_column="z_t_num", range_color=None):
    """
    使用 Plotly 的柱状图展示题材数据：
      - x 轴显示：交易日期、题材名称和题材代码（多行显示）
      - y 轴显示：指定的数值（默认为涨停数，可传入 "up_num" 表示升温数）
      - 鼠标悬停时直接显示交易日期、数值、游资数、题材代码和题材名称
      - 最强题材与升温题材均采用相同的游资颜色标准（统一使用全局 HOT_MONEY_COLOR_SCALE）
    """
    if df.empty:
        st.warning(f"{title} 数据为空，无法生成图表")
        return

    df_chart = df.copy()
    df_chart["label"] = df_chart["trade_date"].astype(str) + "\n" + df_chart["name"] + "\n(" + df_chart["ts_code"] + ")"
    df_chart = df_chart.rename(columns={" 游资数": "hot_money_count"})

    fig = px.bar(
        df_chart,
        x="label",
        y=y_column,
        color="hot_money_count",
        text=y_column,
        custom_data=["trade_date", "hot_money_count", "ts_code", "name"],
        title=title,
        labels={
            "label": "交易日期/题材名称 (代码)",
            y_column: "数值",
            "hot_money_count": "游资数",
            "trade_date": "交易日期"
        },
        color_continuous_scale=HOT_MONEY_COLOR_SCALE,
        range_color=range_color
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>" +
                      "交易日期: %{customdata[0]}<br>" +
                      "数值: %{y}<br>" +
                      "游资数: %{customdata[1]}<br>" +
                      "题材代码: %{customdata[2]}<br>" +
                      "题材名称: %{customdata[3]}<extra></extra>",
        textposition="outside"
    )
    fig.update_layout(
        xaxis_title="交易日期/题材名称 (代码)",
        yaxis_title=("涨停数" if y_column == "z_t_num" else "升温数"),
        legend_title="游资数",
        margin=dict(l=40, r=40, t=80, b=40),
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)


# ==================== 主函数 ====================
def main():
    st.title("题材数据分析")
    st.markdown(
        """
        获取最新数据，筛选出“近期最强题材”和“近期升温题材”
        """
    )

    # 定义缓存键
    result_key = "theme_analysis_result"

    # 当点击“开始分析”按钮时，先检查缓存
    if st.button("开始分析"):
        if result_key in st.session_state:
            st.info("加载缓存结果……")
            cached = st.session_state[result_key]
            st.write(f"成分股文件已保存至：{cached['output_file']}")
            st.write(f"成分股总数: {cached['stock_count']}")
            # 显示图表
            cols = st.columns(2)
            with cols[0]:
                st.subheader("近期最强题材 - 数据图")
                plot_themes_bar_chart(cached["df_filtered_z_display"], "近期最强题材 - 涨停数", y_column="z_t_num", range_color=cached["range_color"])
            with cols[1]:
                st.subheader("近期升温题材 - 数据图")
                plot_themes_bar_chart(cached["df_filtered_up_display"], "近期升温题材 - 升温数", y_column="up_num", range_color=cached["range_color"])
            return

        try:
            st.info("开始执行，请稍候……")
            progress = st.progress(0)
            progress_value = 0

            # ---------------- Step 1: 获取最近 10 个交易日 ----------------
            trade_dates = get_last_n_trade_dates(n=10)
            if not trade_dates:
                st.error("未能获取有效交易日")
                return
            progress_value = 10
            progress.progress(progress_value)

            # ---------------- Step 2: 获取这几天的题材数据 ----------------
            all_data = []
            for t_date in trade_dates:
                df_temp = get_themes_for_date(t_date)
                if not df_temp.empty:
                    all_data.append(df_temp)
            progress_value = 20
            progress.progress(progress_value)

            # ---------------- Step 3: 合并题材数据 ----------------
            if not all_data:
                st.error("未能获取到题材数据")
                return
            df_all = pd.concat(all_data, ignore_index=True)
            logging.info(f"合并题材数据行数: {len(df_all)}")
            progress_value = 30
            progress.progress(progress_value)

            # ---------------- Step 4: 取最新交易日数据 ----------------
            latest_date = df_all['trade_date'].max()
            df_latest = df_all[df_all['trade_date'] == latest_date][['trade_date', 'ts_code', 'name', 'z_t_num', 'up_num']]
            logging.info(f"最新交易日: {latest_date}")
            progress_value = 40
            progress.progress(progress_value)

            # ---------------- Step 5: 确定备用日期 ----------------
            fallback_date = None
            trade_dates_sorted = sorted(trade_dates, reverse=True)
            if len(trade_dates_sorted) >= 2:
                fallback_date = trade_dates_sorted[1]
            progress_value = 45
            progress.progress(progress_value)

            # ---------------- Step 6: 计算均值（基于最近 10 天数据） ----------------
            df_avg = calculate_avg(df_all)
            if df_avg.empty:
                st.error("计算均值失败")
                return
            progress_value = 50
            progress.progress(progress_value)

            # ---------------- Step 7: 筛选题材 ----------------
            df_filtered_z, df_filtered_up = filter_themes(df_latest, df_avg)
            if df_filtered_z.empty and df_filtered_up.empty:
                st.warning("未筛选出符合条件的题材")
                return
            progress_value = 60
            progress.progress(progress_value)

            # ---------------- Step 8: 统计游资数据 ----------------
            df_filtered_z = compute_hot_money_counts_for_themes_once(df_filtered_z, latest_date, fallback_date)
            df_filtered_up = compute_hot_money_counts_for_themes_once(df_filtered_up, latest_date, fallback_date)
            progress_value = 70
            progress.progress(progress_value)

            # ---------------- Step 9: 调整列顺序并准备输出 ----------------
            cols_display = ['trade_date', 'ts_code', 'name', ' 游资数', 'z_t_num', 'up_num']
            df_filtered_z_display = df_filtered_z[cols_display].reset_index(drop=True)
            df_filtered_up_display = df_filtered_up[cols_display].reset_index(drop=True)
            df_filtered_z_display.index = df_filtered_z_display.index + 1
            df_filtered_up_display.index = df_filtered_up_display.index + 1
            progress_value = 80
            progress.progress(progress_value)

            # ---------------- Step 10: 获取所有题材对应的成分股，并写入文件 ----------------
            all_stock_codes = set()
            df_concat = pd.concat([df_filtered_z, df_filtered_up]).drop_duplicates(subset=['ts_code'])
            for theme in df_concat['ts_code']:
                trade_dates_to_try = [latest_date]
                if fallback_date:
                    trade_dates_to_try.append(fallback_date)
                comps = get_component_stocks(theme, trade_dates_to_try)
                all_stock_codes.update(comps)
            output_folder = "date"
            os.makedirs(output_folder, exist_ok=True)
            output_file = os.path.join(output_folder, '成分股.txt')
            if os.path.exists(output_file):
                os.remove(output_file)
            with open(output_file, 'w', encoding='utf-8') as f:
                for code in sorted(all_stock_codes):
                    f.write(f"{code}\n")
            progress_value = 100
            progress.progress(progress_value)

            # ---------------- 统一计算两个数据集的游资数最大值 ----------------
            global_hot_money_max = max(df_filtered_z_display[" 游资数"].max(), df_filtered_up_display[" 游资数"].max())
            range_color = (0, global_hot_money_max)

            # ---------------- 显示图表 ----------------
            cols = st.columns(2)
            with cols[0]:
                st.subheader("近期最强题材 - 数据图")
                plot_themes_bar_chart(df_filtered_z_display, "近期最强题材 - 涨停数", y_column="z_t_num", range_color=range_color)
            with cols[1]:
                st.subheader("近期升温题材 - 数据图")
                plot_themes_bar_chart(df_filtered_up_display, "近期升温题材 - 升温数", y_column="up_num", range_color=range_color)

            st.success(f"成分股文件已保存至：{output_file}")
            st.write(f"成分股总数: {len(all_stock_codes)}")

            # ---------------- 缓存结果 ----------------
            st.session_state[result_key] = {
                "df_filtered_z_display": df_filtered_z_display,
                "df_filtered_up_display": df_filtered_up_display,
                "output_file": output_file,
                "stock_count": len(all_stock_codes),
                "range_color": range_color
            }

        except Exception as e:
            logging.error(f"执行过程中出错: {e}")
            st.error(f"程序执行出错：{e}")


if __name__ == "__main__":
    main()
