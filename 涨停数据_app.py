import tushare as ts
import pandas as pd
from collections import defaultdict
import os
import streamlit as st

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


def save_to_txt(stock_codes, filename):
    """将股票代码保存到txt文件，自动覆盖旧文件"""
    try:
        output_folder = "date"
        filename = os.path.basename(filename)
        output_file = os.path.join(output_folder, filename)
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write('\n'.join(stock_codes))
        st.success(f"股票代码已成功保存到文件：{output_file}")
    except Exception as e:
        st.error(f"保存文件失败: {e}")


def fetch_all_themes(pro, ts_codes):
    """
    针对最近十天所有需要查询的 ts_code 使用一个进度条获取主题数据，
    返回：{ts_code: theme} 的字典
    """
    theme_dict = {}
    total = len(ts_codes)
    progress = st.progress(0)
    for i, ts_code in enumerate(ts_codes):
        try:
            df_theme = pro.kpl_list(ts_code=ts_code, limit=1, fields=["theme", "ts_code", "name"])
            theme = df_theme.iloc[0]['theme'] if not df_theme.empty else ""
            theme_dict[ts_code] = theme
        except Exception as e:
            st.error(f"获取 {ts_code} 主题失败: {e}")
            theme_dict[ts_code] = ""
        progress.progress((i + 1) / total)
    return theme_dict


def run_analysis(token):
    # ------------------ 1. 拉取数据 ------------------
    pro = ts.pro_api(token)
    try:
        df = pro.limit_step(limit='1000', offset='')
    except Exception as e:
        return {"error": f"数据拉取失败: {e}"}

    # ------------------ 2. 剔除 ST 股票 ------------------
    df = df[~df['name'].str.contains('ST', case=False)]
    if df.empty:
        return {"error": "未获取到数据（或全部是ST），请检查接口或参数是否正确。"}

    # ------------------ 3. 数据预处理 ------------------
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date', ascending=False)
    df['nums'] = pd.to_numeric(df['nums'], errors='coerce')

    # 只获取最近10个唯一的交易日期（格式：MM.DD）
    last_10_dates = df['trade_date'].dt.strftime('%m.%d').unique()[:10]

    # ------------------ 4. 收集这十天所有需要查询主题的股票代码（去重） ------------------
    all_ts_codes = set()
    for date in last_10_dates:
        day_data = df[df['trade_date'].dt.strftime('%m.%d') == date]
        ts_codes = day_data['ts_code'].tolist()
        all_ts_codes.update(ts_codes)
    all_ts_codes = list(all_ts_codes)

    # ------------------ 5. 使用一个进度条获取所有主题信息 ------------------
    theme_dict = fetch_all_themes(pro, all_ts_codes)

    # ------------------ 6. 逐日整理股票数据，并填入对应的主题 ------------------
    stocks_data_per_date = {}
    data_per_date = defaultdict(list)
    for date in last_10_dates:
        day_data = df[df['trade_date'].dt.strftime('%m.%d') == date][['name', 'nums', 'ts_code']]
        if day_data.empty:
            continue
        day_data_sorted = day_data.sort_values(by='nums', ascending=False).copy()
        day_data_sorted['nums'] = day_data_sorted['nums'].astype(int)
        day_data_sorted['theme'] = day_data_sorted['ts_code'].apply(lambda x: theme_dict.get(x, ''))
        stocks_data_per_date[date] = day_data_sorted
        formatted_data = day_data_sorted.apply(
            lambda row: f"{row['name']}, {row['nums']}, {row['theme']}" if not pd.isna(row['nums']) else "",
            axis=1
        ).tolist()
        data_per_date[date] = formatted_data

    # ------------------ 7. 计算每个交易日的连板数统计（nums >= 2） ------------------
    count_per_date = {}
    for date in last_10_dates:
        day_df = df[df['trade_date'].dt.strftime('%m.%d') == date]
        count_nums = day_df['nums'].value_counts().to_dict()
        count_per_date[date] = {k: v for k, v in count_nums.items() if k >= 2}
    counts_df = pd.DataFrame(count_per_date).fillna(0).astype(int).T
    counts_df = counts_df.sort_index(ascending=False)
    if counts_df.empty:
        return {"error": "没有足够的数据进行晋级率计算。"}

    # ------------------ 8. 确定需要显示的连板数范围（默认：3连板及以上） ------------------
    if not counts_df.columns.empty:
        max_num = counts_df.columns.astype(int).max()
    else:
        max_num = 2
    rate_columns = sorted([k for k in range(3, max_num + 1)], reverse=True)

    # ------------------ 9. 计算各档晋级率（当日 k 连板 vs 前日 k-1 连板） ------------------
    rates_dict = defaultdict(dict)
    stocks_with_k_dict = defaultdict(lambda: defaultdict(list))
    for i in range(len(last_10_dates) - 1):
        current_date = last_10_dates[i]
        prev_date = last_10_dates[i + 1]
        for k in rate_columns:
            prev_k = k - 1
            prev_count = counts_df.at[prev_date, prev_k] if (prev_k in counts_df.columns) else 0
            current_count = counts_df.at[current_date, k] if (k in counts_df.columns) else 0
            if prev_count > 0:
                rate = (current_count / prev_count) * 100
                rates_dict[current_date][k] = f"{int(rate)}%"
            else:
                rates_dict[current_date][k] = "N/A"
            current_stocks = stocks_data_per_date.get(current_date, pd.DataFrame())
            stocks_with_k = current_stocks[current_stocks['nums'] == k]['name'].tolist()
            if stocks_with_k:
                stocks_with_k_dict[current_date][k].extend(stocks_with_k)
    rates_df = pd.DataFrame(rates_dict).T
    rates_df = rates_df.reindex(last_10_dates).fillna("N/A")
    last_date = last_10_dates[-1]
    rates_df.loc[last_date] = {k: "N/A" for k in rate_columns}
    rates_df = rates_df[rate_columns]

    # ------------------ 10. 计算“当日的晋级率” ------------------
    daily_total_rate = {}
    for i in range(len(last_10_dates) - 1):
        current_date = last_10_dates[i]
        prev_date = last_10_dates[i + 1]
        curr_3_plus_sum = 0
        prev_2_plus_sum = 0
        if (current_date in counts_df.index) and (prev_date in counts_df.index):
            for col in counts_df.columns:
                if col >= 3:
                    curr_3_plus_sum += counts_df.at[current_date, col]
                if col >= 2:
                    prev_2_plus_sum += counts_df.at[prev_date, col]
        if prev_2_plus_sum > 0:
            daily_rate_value = curr_3_plus_sum / prev_2_plus_sum * 100
            daily_total_rate[current_date] = f"{int(daily_rate_value)}%"
        else:
            daily_total_rate[current_date] = "N/A"
    daily_total_rate[last_date] = "N/A"

    # ------------------ 11. 计算近5天 & 近10天平均成功率 ------------------
    recent_10_dates = last_10_dates[:10]
    recent_5_dates = last_10_dates[:5]
    avg_success_rate_10 = {}
    avg_success_rate_5 = {}
    for k in rate_columns:
        daily_rates_10 = []
        for d in recent_10_dates:
            val_str = rates_df.at[d, k] if d in rates_df.index else "N/A"
            daily_rates_10.append(0 if val_str == "N/A" else int(val_str.replace('%', '')))
        avg_success_rate_10[k] = sum(daily_rates_10) / 10
        daily_rates_5 = []
        for d in recent_5_dates:
            val_str = rates_df.at[d, k] if d in rates_df.index else "N/A"
            daily_rates_5.append(0 if val_str == "N/A" else int(val_str.replace('%', '')))
        avg_success_rate_5[k] = sum(daily_rates_5) / 5

    # ------------------ 12. 生成【平均成功率与打板股票】数据（表1） ------------------
    today = last_10_dates[0]
    table_rows = []
    if today in rates_df.index:
        day_data_sorted_today = stocks_data_per_date.get(today, pd.DataFrame())
        for k in sorted(rate_columns):
            today_rate_str = rates_df.at[today, k]
            preferred_stocks = "无"
            secondary_stocks = "无"
            if today_rate_str != "N/A":
                today_rate_val = int(today_rate_str.replace('%', ''))
                k_minus_1_stocks = day_data_sorted_today.loc[
                    day_data_sorted_today['nums'] == (k - 1), 'name'
                ].tolist()
                if (today_rate_val > avg_success_rate_5[k]) and (today_rate_val > avg_success_rate_10[k]):
                    preferred_stocks = ", ".join(k_minus_1_stocks) if k_minus_1_stocks else "无"
                elif (today_rate_val > avg_success_rate_10[k]) and (today_rate_val < avg_success_rate_5[k]):
                    secondary_stocks = ", ".join(k_minus_1_stocks) if k_minus_1_stocks else "无"
            table_rows.append({
                "连板数": f"{k}连板",
                "5日平均": f"{avg_success_rate_5[k]:.2f}%",
                "10日平均": f"{avg_success_rate_10[k]:.2f}%",
                "当天晋级率": today_rate_str,
                "优选打板股票": preferred_stocks,
                "次选打板股票": secondary_stocks
            })
    result_df = pd.DataFrame(table_rows).fillna("")

    # ------------------ 13. 构造【每日连板晋级率】数据（表2） ------------------
    data_per_date_rates = defaultdict(list)
    for date in last_10_dates:
        for k in rate_columns:
            rate = rates_df.at[date, k]
            if date != last_date and rate != "N/A":
                stocks = stocks_with_k_dict[date][k]
                if stocks:
                    first_stock = stocks[0]
                    entry = f"{k}连板 {rate} {first_stock}"
                    data_per_date_rates[date].append(entry)
                    for stock in stocks[1:]:
                        prefix_length = len(f"{k}连板 {rate} ")
                        entry = ' ' * prefix_length + stock
                        data_per_date_rates[date].append(entry)
            elif date == last_date:
                entry = f"{k}连板 N/A"
                data_per_date_rates[date].append(entry)
                stocks = stocks_with_k_dict[date][k]
                for stock in stocks:
                    prefix_length = len(f"{k}连板 N/A ")
                    entry = ' ' * prefix_length + stock
                    data_per_date_rates[date].append(entry)
    header_list = [f"{date} ({daily_total_rate.get(date, 'N/A')})" for date in last_10_dates]
    max_entries = max(len(entries) for entries in data_per_date_rates.values()) if data_per_date_rates else 0
    daily_rates_rows = []
    for i in range(max_entries):
        row = {}
        for idx, date in enumerate(last_10_dates):
            row[header_list[idx]] = data_per_date_rates[date][i] if i < len(data_per_date_rates[date]) else ""
        daily_rates_rows.append(row)
    daily_rates_df = pd.DataFrame(daily_rates_rows).fillna("")

    # ------------------ 14. 整理【涨停板股票数据】（表3） ------------------
    # 在构造表3时，我们将每日晋级率信息（daily_total_rate）加到列标题中
    stocks_table = {}
    for date in last_10_dates:
        header = f"{date} ({daily_total_rate.get(date, 'N/A')})"
        stocks_table[header] = pd.Series(data_per_date.get(date, []))
    stocks_df = pd.DataFrame(stocks_table).fillna("")

    # ------------------ 15. 获取最新交易日股票代码 ------------------
    recent_date = last_10_dates[0]
    recent_date_stocks = df[df['trade_date'].dt.strftime('%m.%d') == recent_date]['ts_code'].tolist()

    return {
        "result_df": result_df,
        "daily_rates_df": daily_rates_df,
        "stocks_df": stocks_df,
        "recent_date": recent_date,
        "recent_date_stocks": recent_date_stocks
    }


def set_table_css():
    """
    注入自定义 CSS，用于设置 st.dataframe 显示时的单元格宽度
    """
    st.markdown(
        """
        <style>
        /* 针对 st.dataframe 内部表格单元格设置最小和最大宽度 */
        div[data-testid="stDataFrameContainer"] table td {
            min-width: 150px;
            max-width: 300px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        div[data-testid="stDataFrameContainer"] table th {
            min-width: 150px;
            max-width: 300px;
            text-align: center;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


def display_results(results):
    # 注入自定义 CSS 样式
    set_table_css()

    st.header(f"（{results['recent_date']}）平均成功率与打板股票")
    st.dataframe(results["result_df"], use_container_width=True)

    st.header("每日连板晋级率")
    st.dataframe(results["daily_rates_df"], use_container_width=True)

    st.header("涨停板股票数据（含每日晋级率）")
    st.dataframe(results["stocks_df"], use_container_width=True)

    st.info(f"最新交易日（{results['recent_date']}）的股票数量: {len(results['recent_date_stocks'])}")
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    save_to_txt(results["recent_date_stocks"], os.path.join(desktop_path, "涨停板.txt"))


def main():
    st.title("股票连板数据分析")
    st.markdown("本页面展示连板统计、晋级率及股票推荐等信息。")

    # 检查是否已有缓存数据
    result_key = "stock_analysis_result"
    if result_key in st.session_state:
        st.write("加载缓存数据...")
        results = st.session_state[result_key]
        display_results(results)
        return

    if st.button("开始分析"):
        with st.spinner("正在分析，请稍候..."):
            results = run_analysis(tushare_token)
        if "error" in results:
            st.error(results["error"])
        else:
            st.session_state[result_key] = results
            display_results(results)


if __name__ == "__main__":
    main()
