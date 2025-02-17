import streamlit as st
import tushare as ts
import pandas as pd
import os
import ast
import logging
import time
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# 设置 Pandas 显示选项
pd.set_option('display.max_colwidth', None)

# 从 secrets.toml 中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]
ts.set_token(tushare_token)
pro = ts.pro_api()

def main():
    st.write("筛选包含“香港中央结算有限公司”的股票，并保存到文件。")
    # 指定保存文件的路径（相对路径）
    file_path = './date/股东.txt'

    # 获取所有上市普通股票代码（list_status='L' 表示上市股票）
    stock_list = pro.stock_basic(list_status='L', fields='ts_code')
    total_stocks = len(stock_list)
    st.write(f"共获取股票数量: {total_stocks}")

    qualified_stocks = []  # 用于存放符合条件的股票代码
    call_count = 0
    start_time = time.time()

    progress_bar = st.progress(0)

    # 遍历所有股票
    for i, (_, row) in enumerate(stock_list.iterrows()):
        ts_code = row['ts_code']
        try:
            # 调用 top10_holders 接口，只请求 holder_name 字段
            df = pro.top10_holders(ts_code=ts_code, limit=10, fields=["holder_name"])
            if not df.empty:
                # 对 holder_name 进行清洗：去除空格并统一小写
                df['holder_name_clean'] = df['holder_name'].apply(lambda x: str(x).strip().lower())
                # 判断是否包含 "香港中央结算有限公司"
                if any("香港中央结算有限公司" in name for name in df['holder_name_clean'].values):
                    if ts_code not in qualified_stocks:
                        qualified_stocks.append(ts_code)
        except Exception as e:
            st.error(f"处理股票 {ts_code} 时发生错误: {e}")

        # 控制 API 调用频率，每390次调用后，如果60秒内调用未满，则等待剩余时间
        call_count += 1
        if call_count >= 390:
            elapsed = time.time() - start_time
            if elapsed < 60:
                time.sleep(60 - elapsed)
            call_count = 0
            start_time = time.time()

        progress_bar.progress((i + 1) / total_stocks)

    # 将符合条件的股票代码写入文件（覆盖已存在的文件）
    save_dir = "date"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    with open(file_path, 'w', encoding='utf-8') as f:
        for stock in qualified_stocks:
            f.write(stock + "\n")

    st.write(f"筛选获得的股票总数量: {len(qualified_stocks)}")
    st.success(f"结果已保存到：{file_path}")

    # 显示结果表格，索引从 1 开始
    result_df = pd.DataFrame(qualified_stocks, columns=["股票代码"])
    result_df.index = range(1, len(result_df) + 1)
    st.dataframe(result_df, use_container_width=True)

# 仅在点击按钮后才执行 main()，利用 session_state 来记录是否点击
if "run_main" not in st.session_state:
    st.session_state.run_main = False

st.title("更新股东池")
st.write("请点击下面的【开始筛选】按钮开始执行筛选。")
if st.button("开始筛选"):
    st.session_state.run_main = True

if st.session_state.run_main:
    main()
