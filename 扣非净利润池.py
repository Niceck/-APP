import os
import time
import tushare as ts
import pandas as pd
import streamlit as st
from tqdm import tqdm
from ratelimit import limits, sleep_and_retry

# 从 secrets.toml 中读取 Tushare API Token
tushare_token = st.secrets.get("api_keys", {}).get("tushare_token", "your_default_token_here")
ts.set_token(tushare_token)
pro = ts.pro_api()
# =============== 2. 获取所有正常上市 A 股股票列表并过滤 ST ===============
stock_list = pro.stock_basic(
    exchange='',
    list_status='L',
    fields='ts_code,name'
)

# 过滤掉名称中含 'ST' 的股票
common_stocks = stock_list[~stock_list['name'].str.contains('ST', na=False)].copy()
common_stocks.reset_index(drop=True, inplace=True)
st.write(f"过滤 ST 后股票数量: {len(common_stocks)}")

# =============== 3. 定义受限的 API 调用函数 ===============
ONE_MINUTE = 60
CALLS_PER_MINUTE = 480  # 每分钟最多480次调用（可根据自身权限调整）

@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=ONE_MINUTE)
def get_fina_indicator(ts_code):
    """
    获取指定股票最近 30 条财务指标数据。
    返回 pd.DataFrame，含以下字段：
      - ts_code: 股票代码
      - ann_date: 公告日期
      - end_date: 报告期
      - netprofit_yoy: 归母净利润同比增速
      - dt_netprofit_yoy: 扣非净利润同比增速
      - q_netprofit_yoy: 单季度归母净利润同比增速
      - q_netprofit_qoq: 单季度净利润环比增速
    """
    df = pro.fina_indicator(
        ts_code=ts_code,
        limit=30,
        fields=[
            "ts_code",
            "ann_date",
            "end_date",
            "netprofit_yoy",
            "dt_netprofit_yoy",
            "q_netprofit_yoy",
            "q_netprofit_qoq",
        ]
    )
    return df

# =============== 4. 获取财务指标数据（增加无限重试） ===============
def fetch_fina_data():
    fina_data_list = []
    total = len(common_stocks)
    st.write("\n开始获取财务数据...")
    progress_bar = st.progress(0)  # 初始化进度条

    for idx, row in tqdm(common_stocks.iterrows(), total=total, desc="获取财务数据"):
        ts_code = row['ts_code']

        # 使用“无限重试”，直到取到非空 DataFrame 为止
        while True:
            try:
                df_part = get_fina_indicator(ts_code)
                if not df_part.empty:
                    fina_data_list.append(df_part)
                    break
                else:
                    st.write(f"\n股票 {ts_code} 返回空数据，再次重试...\n")
                    time.sleep(3)  # 等 3 秒再试
            except Exception as e:
                st.write(f"\n股票 {ts_code} 调用失败: {e}\n")
                time.sleep(3)  # 等 3 秒再试

        progress_bar.progress((idx + 1) / total)  # 更新进度条

    st.write("财务数据获取完成。")
    return fina_data_list

# =============== 5. 合并所有股票数据并去重 ===============
def process_data(fina_data_list):
    if not fina_data_list:
        st.write("未能获取到任何财务指标数据，请检查 Tushare 权限或调用频次。")
        return None

    df_all = pd.concat(fina_data_list, ignore_index=True)
    df_all.drop_duplicates(inplace=True)

    # =============== 6. 合并股票名称，并重命名为中文字段 ===============
    df_merged = df_all.merge(common_stocks[['ts_code', 'name']], on='ts_code', how='left')
    df_merged.rename(columns={
        'ts_code':           '股票代码',
        'name':              '股票名称',
        'ann_date':          '公告日期',
        'end_date':          '报告期',
        'netprofit_yoy':     '归母净利润同比增速',
        'dt_netprofit_yoy':  '扣非净利润同比增速',
        'q_netprofit_yoy':   '单季度归母净利润同比增速',
        'q_netprofit_qoq':   '单季度净利润环比增速'
    }, inplace=True)

    # 将“报告期”和“公告日期”转换为日期格式
    df_merged['报告期'] = pd.to_datetime(df_merged['报告期'], errors='coerce')
    df_merged['公告日期'] = pd.to_datetime(df_merged['公告日期'], errors='coerce')

    return df_merged

# =============== 7. 自定义筛选逻辑 ===============
def filter_by_latest_one(sub_df):
    sub_df = sub_df.dropna(subset=['报告期', '扣非净利润同比增速', '公告日期'])
    if sub_df.empty:
        return None

    # 找到该股票的最新报告期
    max_report_date = sub_df['报告期'].max()
    df_latest_period = sub_df[sub_df['报告期'] == max_report_date]
    if df_latest_period.empty:
        return None

    # 在最新报告期中，选“公告日期”最大的那条数据
    latest_idx = df_latest_period['公告日期'].idxmax()
    latest_record = df_latest_period.loc[latest_idx]

    # 示例逻辑：若最新扣非 > 历史所有扣非，则保留最新记录
    older_data = sub_df[sub_df['报告期'] < max_report_date]
    if older_data.empty:
        return latest_record

    older_max = older_data['扣非净利润同比增速'].max()
    if latest_record['扣非净利润同比增速'] > older_max:
        return latest_record
    else:
        return None

def filter_data(df_merged):
    df_filtered = (
        df_merged
        .groupby('股票代码', group_keys=False)
        .apply(filter_by_latest_one)
        .dropna(how='all')  # 剔除返回 None 的情况
        .reset_index(drop=True)
    )

    if df_filtered.empty:
        st.write("\n没有任何股票满足筛选条件。")
        return None

    return df_filtered

# =============== 8. 输出前100条数据（数值四舍五入且不显示索引） ===============
def output_top_100(df_filtered):
    df_filtered.sort_values(by='扣非净利润同比增速', ascending=False, inplace=True)
    df_top200 = df_filtered.head(200)

    # 对所有数值型数据四舍五入为整数（仅对 float 列操作）
    df_top200_disp = df_top200.copy()
    for col in df_top200_disp.select_dtypes(include=['float']).columns:
        df_top200_disp[col] = df_top200_disp[col].round(0).apply(lambda x: int(x) if pd.notnull(x) else x)

    st.write("\n【满足条件】且按扣非增速降序排名前200的股票中，前100如下：\n")
    st.dataframe(df_top200_disp.head(100), hide_index=True)
    return df_top200

# =============== 9. 保存文件 ===============
def save_top_100(df_top200):
    # 使用相对路径的 'date' 文件夹
    output_file = os.path.join("date", "扣非.txt")
    top_100_codes = df_top200.head(200)['股票代码'].tolist()

    # 打开文件，默认会自动覆盖同名文件
    with open(output_file, "w", encoding="utf-8") as f:
        for code in top_100_codes:
            f.write(f"{code}\n")

    st.write(f"\n前 100 股票代码已保存到: {output_file}")

# =============== 10. 主流程 ------------------------
def main():
    # 使用缓存避免重复计算：缓存获取的财务数据列表
    if "fina_data_list" not in st.session_state:
        st.session_state["fina_data_list"] = None

    if st.button("开始获取并处理财务数据"):
        if st.session_state["fina_data_list"] is None:
            fina_data_list = fetch_fina_data()
            st.session_state["fina_data_list"] = fina_data_list  # 缓存数据
        else:
            fina_data_list = st.session_state["fina_data_list"]
            st.write("使用缓存的财务数据...")

        df_merged = process_data(fina_data_list)
        if df_merged is None:
            return

        df_filtered = filter_data(df_merged)
        if df_filtered is None:
            return

        df_top200 = output_top_100(df_filtered)
        save_top_100(df_top200)

if __name__ == "__main__":
    main()
