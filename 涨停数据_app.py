import tushare as ts
import pandas as pd
from collections import defaultdict
import os
import streamlit as st
from tqdm import tqdm

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


def save_to_txt(stock_codes, filename):
    """将股票代码保存到txt文件，自动覆盖旧文件"""
    try:
        # 设置文件保存的相对路径：保存到 'date' 文件夹下
        output_folder = "date"

        # 如果 filename 是绝对路径，只提取文件名部分
        filename = os.path.basename(filename)

        # 确保文件保存路径是 'date' 文件夹下的相对路径
        output_file = os.path.join(output_folder, filename)

        # 打开文件并写入内容，自动覆盖旧文件
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write('\n'.join(stock_codes))

        st.success(f"股票代码已成功保存到文件：{output_file}")
    except Exception as e:
        st.error(f"保存文件失败: {e}")


def fetch_themes(pro, ts_codes):
    """
    获取所有 ts_code 对应的主题信息。
    取消缓存，避免重复查询
    """
    theme_dict = {}
    for ts_code in tqdm(ts_codes, desc="Fetching themes"):
        try:
            df_theme = pro.kpl_list(ts_code=ts_code, limit=1, fields=["theme", "ts_code", "name"])
            if not df_theme.empty:
                theme = df_theme.iloc[0]['theme']
            else:
                theme = ""
            theme_dict[ts_code] = theme
        except Exception as e:
            st.error(f"获取 ts_code {ts_code} 的主题失败: {e}")
            theme_dict[ts_code] = ""
    return theme_dict


# -------------------------------
# 分析函数（去除缓存功能）
# -------------------------------
def run_analysis(token):
    # =============== 1. 初始化 Tushare Pro 接口 ===============
    pro = ts.pro_api(token)

    # =============== 2. 拉取数据 ===============
    try:
        df = pro.limit_step(limit='1000', offset='')
    except Exception as e:
        return {"error": f"数据拉取失败: {e}"}

    # =============== 3. 剔除 ST 股票 ===============
    df = df[~df['name'].str.contains('ST', case=False)]
    if df.empty:
        return {"error": "未获取到数据（或全部是ST），请检查接口或参数是否正确。"}

    # =============== 4. 数据预处理 ===============
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date', ascending=False)
    df['nums'] = pd.to_numeric(df['nums'], errors='coerce')

    # 只获取最近10个唯一的交易日期（降序），格式为 MM.DD
    last_10_dates = df['trade_date'].dt.strftime('%m.%d').unique()[:11]

    # =============== 5. 逐日整理股票信息 + 主题 ===============
    theme_dict = {}
    stocks_data_per_date = {}
    data_per_date = defaultdict(list)

    for date in last_10_dates:
        day_data = df[df['trade_date'].dt.strftime('%m.%d') == date][['name', 'nums', 'ts_code']]
        if day_data.empty:
            continue
        day_data_sorted = day_data.sort_values(by='nums', ascending=False).copy()
        day_data_sorted['nums'] = day_data_sorted['nums'].astype(int)
        ts_codes = day_data_sorted['ts_code'].tolist()
        if ts_codes:
            theme_dict = fetch_themes(pro, ts_codes)
        else:
            theme_dict = {}
        day_data_sorted['theme'] = day_data_sorted['ts_code'].apply(lambda x: theme_dict.get(x, ''))
        stocks_data_per_date[date] = day_data_sorted
        formatted_data = day_data_sorted.apply(
            lambda row: f"{row['name']}, {row['nums']}, {row['theme']}" if not pd.isna(row['nums']) else "",
            axis=1
        ).tolist()
        data_per_date[date] = formatted_data

    # =============== 6. 计算每个交易日的连板数统计（nums >= 2） ===============
    count_per_date = {}
    for date in last_10_dates:
        day_df = df[df['trade_date'].dt.strftime('%m.%d') == date]
        count_nums = day_df['nums'].value_counts().to_dict()
        count_per_date[date] = {k: v for k, v in count_nums.items() if k >= 2}
    counts_df = pd.DataFrame(count_per_date).fillna(0).astype(int).T
    counts_df = counts_df.sort_index(ascending=False)
    if counts_df.empty:
        return {"error": "没有足够的数据进行晋级率计算。"}

    # =============== 7. 确定需要显示的连板数范围 (默认: 3连板及以上) ===============
    if not counts_df.columns.empty:
        max_num = counts_df.columns.astype(int).max()
    else:
        max_num = 2
    rate_columns = sorted([k for k in range(3, max_num + 1)], reverse=True)

    # =============== 8. 计算各档晋级率（当日 k 连板 vs 前日 k-1 连板） ===============
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
                rates_dict[current_date][k] = f"{int(rate):d}%"
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

    # =============== 8.1 计算“当日的晋级率” ===============
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

    # =============== 9. 计算近5天 & 近10天平均成功率 ===============
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

    # =============== 10. 生成【平均成功率与打板股票】数据 ===============
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

    # =============== 11. 构造【每日连板晋级率】数据 ===============
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

    # =============== 12. 整理【涨停板股票数据】 ===============
    stocks_table = {date: pd.Series(data) for date, data in data_per_date.items()}
    stocks_df = pd.DataFrame(stocks_table).fillna("")

    # =============== 13. 获取最新交易日股票代码 ===============
    recent_date = last_10_dates[0]
    recent_date_stocks = df[df['trade_date'].dt.strftime('%m.%d') == recent_date]['ts_code'].tolist()

    return {
        "result_df": result_df,
        "daily_rates_df": daily_rates_df,
        "stocks_df": stocks_df,
        "recent_date": recent_date,
        "recent_date_stocks": recent_date_stocks
    }


def display_results(results):
    st.header(f"（{results['recent_date']}）平均成功率与打板股票")
    # 使用 to_html(index=False) 输出 HTML 表格，隐藏序号
    html_result = results["result_df"].to_html(index=False)
    st.markdown(html_result, unsafe_allow_html=True)

    st.header("每日连板晋级率")
    # 表格样式：最小宽度 180px，文本右对齐
    html_daily = results["daily_rates_df"].style.hide(axis="index").set_table_styles([
        {'selector': 'th', 'props': [('min-width', '190px'), ('text-align', 'right')]},
        {'selector': 'td', 'props': [('min-width', '190px'), ('text-align', 'right')]}
    ]).to_html()

    st.markdown(html_daily, unsafe_allow_html=True)

    st.header("涨停板股票数据")
    html_stocks = results["stocks_df"].style.hide(axis="index").set_table_styles([
        {'selector': 'th', 'props': [('min-width', '300px')]},
        {'selector': 'td', 'props': [('min-width', '310px')]}
    ]).to_html()
    st.markdown(html_stocks, unsafe_allow_html=True)

    st.info(f"最新交易日（{results['recent_date']}）的股票数量: {len(results['recent_date_stocks'])}")
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    save_to_txt(results["recent_date_stocks"], os.path.join(desktop_path, "涨停板.txt"))

def main():
    st.title("股票连板数据分析")
    st.markdown("本页面展示连板统计、晋级率及股票推荐等信息。")

    # 检查是否有缓存的数据
    result_key = "stock_analysis_result"
    if result_key in st.session_state:
        st.write("加载缓存数据...")
        # 使用缓存数据
        results = st.session_state[result_key]
        display_results(results)
        return


    # 如果没有缓存，则运行分析
    if st.button("开始分析"):
        with st.spinner("正在分析，请稍候..."):
            results = run_analysis(tushare_token)
        if "error" in results:
            st.error(results["error"])
        else:
            # 缓存分析结果
            st.session_state[result_key] = results
            display_results(results)


if __name__ == "__main__":
    main()
