import tushare as ts
import pandas as pd
from collections import defaultdict
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


def fetch_all_themes(pro, ts_codes):
    """
    针对近 11 个交易日所有需要查询的 ts_code 使用进度条获取主题数据，
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
        return {"error": "未获取到数据（或全部为ST），请检查接口或参数是否正确。"}

    # ------------------ 3. 数据预处理 ------------------
    # 按交易日期降序排列（最新在前）
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date', ascending=False)
    df['nums'] = pd.to_numeric(df['nums'], errors='coerce')

    # 取 11 个交易日的数据（按 df 排序后的前 11 个日期），用于计算晋级率
    all_dates = list(df['trade_date'].dt.strftime('%m.%d').unique()[:11])
    # 用于显示（表2、表3、综合图）只显示最近 10 个交易日，即去掉最后一天
    display_dates = all_dates[:-1]

    # ------------------ 4. 收集这 11 天所有需要查询主题的股票代码（去重） ------------------
    all_ts_codes = set()
    for date in all_dates:
        day_data = df[df['trade_date'].dt.strftime('%m.%d') == date]
        ts_codes = day_data['ts_code'].tolist()
        all_ts_codes.update(ts_codes)
    all_ts_codes = list(all_ts_codes)

    # ------------------ 5. 使用进度条获取所有主题信息 ------------------
    theme_dict = fetch_all_themes(pro, all_ts_codes)

    # ------------------ 6. 逐日整理股票数据，并填入对应主题 ------------------
    stocks_data_per_date = {}
    data_per_date = defaultdict(list)
    for date in all_dates:
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
    for date in all_dates:
        day_df = df[df['trade_date'].dt.strftime('%m.%d') == date]
        count_nums = day_df['nums'].value_counts().to_dict()
        count_per_date[date] = {k: v for k, v in count_nums.items() if k >= 2}
    counts_df = pd.DataFrame(count_per_date).fillna(0).astype(int).T
    counts_df = counts_df.reindex(all_dates)
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
    # 用 all_dates 计算（共 11 个日期，故可计算出 10 个晋级率）
    for i in range(len(all_dates) - 1):
        current_date = all_dates[i]
        prev_date = all_dates[i + 1]
        for k in rate_columns:
            prev_count = counts_df.at[prev_date, k - 1] if (k - 1 in counts_df.columns) else 0
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
    rates_df = rates_df.reindex(all_dates).fillna("N/A")
    last_date = all_dates[-1]
    rates_df.loc[last_date] = {k: "N/A" for k in rate_columns}
    rates_df = rates_df[rate_columns]

    # ------------------ 10. 计算“每日总体晋级率” ------------------
    # 计算结果仅对 all_dates 中前 10 个日期有效（用第 i+1 天数据作对比）
    daily_total_rate = {}
    for i in range(len(all_dates) - 1):
        current_date = all_dates[i]
        prev_date = all_dates[i + 1]
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

    # ------------------ 11. 计算近5天 & 近11天平均成功率 ------------------
    recent_11_dates = all_dates[:11]  # 即 all_dates
    recent_5_dates = all_dates[:5]
    avg_success_rate_11 = {}
    avg_success_rate_5 = {}
    for k in rate_columns:
        daily_rates_11 = []
        for d in recent_11_dates:
            val_str = rates_df.at[d, k] if d in rates_df.index else "N/A"
            daily_rates_11.append(0 if val_str == "N/A" else int(val_str.replace('%', '')))
        avg_success_rate_11[k] = sum(daily_rates_11) / 11
        daily_rates_5 = []
        for d in recent_5_dates:
            val_str = rates_df.at[d, k] if d in rates_df.index else "N/A"
            daily_rates_5.append(0 if val_str == "N/A" else int(val_str.replace('%', '')))
        avg_success_rate_5[k] = sum(daily_rates_5) / 5

    # ------------------ 12. 生成【平均成功率与打板股票】数据（表1） ------------------
    # 取最近 10 个交易日中的最新一天作为“今日”
    today = display_dates[0]
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
                if (today_rate_val > avg_success_rate_5[k]) and (today_rate_val > avg_success_rate_11[k]):
                    preferred_stocks = ", ".join(k_minus_1_stocks) if k_minus_1_stocks else "无"
                elif (today_rate_val > avg_success_rate_11[k]) and (today_rate_val < avg_success_rate_5[k]):
                    secondary_stocks = ", ".join(k_minus_1_stocks) if k_minus_1_stocks else "无"
            table_rows.append({
                "连板数": f"{k}连板",
                "5日平均": f"{avg_success_rate_5[k]:.2f}%",
                "11日平均": f"{avg_success_rate_11[k]:.2f}%",
                "当天晋级率": today_rate_str,
                "优选打板股票": preferred_stocks,
                "次选打板股票": secondary_stocks
            })
    result_df = pd.DataFrame(table_rows).fillna("")
    # 设置索引从 1 开始
    result_df.index = range(1, len(result_df) + 1)

    # ------------------ 13. 构造【每日连板晋级率】数据（表2） ------------------
    data_per_date_rates = defaultdict(list)
    for date in display_dates:
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
    header_list = [f"{date} ({daily_total_rate.get(date, 'N/A')})" for date in display_dates]
    max_entries = max(len(entries) for entries in data_per_date_rates.values()) if data_per_date_rates else 0
    daily_rates_rows = []
    for i in range(max_entries):
        row = {}
        for idx, date in enumerate(display_dates):
            row[header_list[idx]] = data_per_date_rates[date][i] if i < len(data_per_date_rates[date]) else ""
        daily_rates_rows.append(row)
    daily_rates_df = pd.DataFrame(daily_rates_rows).fillna("")
    daily_rates_df.index = range(1, len(daily_rates_df) + 1)

    # ------------------ 14. 整理【涨停板股票数据】（含每日晋级率）（表3） ------------------
    stocks_table = {}
    for date in display_dates:
        header = f"{date} ({daily_total_rate.get(date, 'N/A')})"
        stocks_table[header] = pd.Series(data_per_date.get(date, []))
    stocks_df = pd.DataFrame(stocks_table).fillna("")
    stocks_df.index = range(1, len(stocks_df) + 1)

    # ------------------ 将最新一天的连板股票代码保存到文件“date/涨停板.txt” ------------------
    latest_date = display_dates[0]
    latest_date_stocks = stocks_data_per_date.get(latest_date, pd.DataFrame())
    latest_date_stocks = latest_date_stocks[['name', 'nums', 'theme']]
    file_path = "date/涨停板.txt"
    latest_date_stocks.to_csv(file_path, index=False, encoding='utf-8')
    st.success(f"最新一天的连板股票代码已保存到 {file_path}")

    # ------------------ 为综合图表准备数据 ------------------
    # 综合图表仅显示最近 10 个交易日数据（display_dates）
    display_counts_df = counts_df.loc[display_dates]
    # 连板最高板：若无数据则置 0
    highest_board_series = display_counts_df.apply(lambda row: row[row > 0].index.max() if (row > 0).any() else 0, axis=1)
    # 每日总体晋级率转换为数值（"N/A" 置 0）
    daily_rate_list = []
    for date in display_dates:
        val = daily_total_rate.get(date, "N/A")
        try:
            daily_rate_list.append(float(val.replace('%', '')))
        except:
            daily_rate_list.append(0)
    daily_rate_series = pd.Series(daily_rate_list, index=display_dates)

    return {
        "result_df": result_df,             # 表1：平均成功率与打板股票
        "daily_rates_df": daily_rates_df,   # 表2：每日连板晋级率
        "stocks_df": stocks_df,             # 表3：涨停板股票数据（含每日晋级率）
        "recent_date": display_dates[0],
        "recent_date_stocks": df[df['trade_date'].dt.strftime('%m.%d') == display_dates[0]]['ts_code'].tolist(),
        # 综合图表所需数据（只取最近 10 个交易日）
        "counts_df": display_counts_df,
        "highest_board_series": highest_board_series,
        "daily_rate_series": daily_rate_series,
        "display_dates": display_dates  # 顺序为【新->旧】，后续综合图表会反转为从远到近显示
    }


def display_composite_chart(results):
    """
    绘制综合图表：
      - X 轴显示最近 10 个交易日（按日期从远到近排序）
      - 柱状图：每日连板总数
      - 折线图：连板最高板
      - 折线图（右侧 Y 轴）：每日总体晋级率（%）
      - 文字位置优化：连板最高板数值放在线下方，避免与晋级率重叠
    """
    counts_df = results["counts_df"]
    highest_board_series = results["highest_board_series"]
    daily_rate_series = results["daily_rate_series"]

    # 将 display_dates 反转，确保 X 轴从远到近
    chart_dates = list(reversed(results["display_dates"]))

    # 每日连板总数
    daily_total_counts = counts_df.sum(axis=1).reindex(results["display_dates"])
    daily_total_counts = daily_total_counts.reindex(chart_dates)

    # 连板最高板
    highest_board = highest_board_series.reindex(results["display_dates"]).reindex(chart_dates)

    # 每日总体晋级率（%）
    daily_rate = daily_rate_series.reindex(results["display_dates"]).reindex(chart_dates)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # 柱状图：每日连板总数
    fig.add_trace(
        go.Bar(
            x=chart_dates,
            y=daily_total_counts,
            name="每日连板总数",
            marker_color="steelblue",
            text=daily_total_counts,              # 在柱子上方显示数据
            textposition="outside",
            hovertemplate='日期：%{x}<br>连板总数：%{y}<extra></extra>'
        ),
        secondary_y=False
    )
    # 折线图：连板最高板
    fig.add_trace(
        go.Scatter(
            x=chart_dates,
            y=highest_board,
            mode='lines+markers+text',
            text=highest_board,
            textposition='bottom center',         # 调整为线下方
            name="连板最高板",
            line=dict(color='tomato', width=2),
            hovertemplate='日期：%{x}<br>最高板：%{y}<extra></extra>'
        ),
        secondary_y=False
    )
    # 折线图：每日总体晋级率（%），右侧 Y 轴
    fig.add_trace(
        go.Scatter(
            x=chart_dates,
            y=daily_rate,
            mode='lines+markers+text',
            text=[f"{v}%" for v in daily_rate],
            textposition='top center',            # 保持在线顶部
            name="每日总体晋级率 (%)",
            line=dict(color='orange', width=2),
            hovertemplate='日期：%{x}<br>晋级率：%{y}%<extra></extra>'
        ),
        secondary_y=True
    )

    # 全局布局设置
    fig.update_layout(
        title="综合图表（近 10 个交易日）",
        xaxis_title="交易日期",
        yaxis_title="连板数 / 股票数量",
        hovermode="x unified",            # 鼠标悬停统一显示
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    # 左侧 Y 轴
    fig.update_yaxes(
        title_text="连板数 / 股票数量",
        secondary_y=False
    )
    # 右侧 Y 轴：范围设置为 0~120
    fig.update_yaxes(
        title_text="晋级率 (%)",
        range=[0, 120],
        secondary_y=True
    )

    # X 轴设为分类轴，并可调整刻度标签角度（如有需要可改为 tickangle=45）
    fig.update_xaxes(type='category', tickangle=0)

    st.plotly_chart(fig, use_container_width=True)


def set_table_css():
    st.markdown(
        """
        <style>
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
    set_table_css()

    # 先输出综合图表
    st.header("【综合图表】近 10 个交易日数据")
    display_composite_chart(results)

    # 再输出原来的三个表格
    st.header(f"（{results['recent_date']}）平均成功率与打板股票")
    st.dataframe(results["result_df"], use_container_width=True)

    st.header("每日连板晋级率")
    st.dataframe(results["daily_rates_df"], use_container_width=True)

    st.header("涨停板股票数据（含每日晋级率）")
    st.dataframe(results["stocks_df"], use_container_width=True)

    st.info(f"最新交易日（{results['recent_date']}）的连板股票数量: {len(results['recent_date_stocks'])}")


def main():
    st.title("股票连板数据分析")
    st.markdown("本页面展示连板统计、晋级率及股票推荐等信息")

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
