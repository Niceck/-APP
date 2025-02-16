import tushare as ts
import pandas as pd
import os
import streamlit as st
import base64


# Set pandas display option to show full column width
pd.set_option('display.max_colwidth', None)

# Read Tushare API token from secrets.toml
tushare_token = st.secrets.get("api_keys", {}).get("tushare_token", "your_default_token_here")

# Set Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

# Function to fetch data
def fetch_data(trade_date, ts_code, hm_name, start_date, end_date, limit, offset=0):
    df = pro.hm_detail(
        trade_date=trade_date,
        ts_code=ts_code,
        hm_name=hm_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        fields=["trade_date", "ts_code", "ts_name", "buy_amount", "sell_amount", "net_amount", "hm_name"]
    )
    return df

def main():
    st.title("恢恢游资库")
    st.markdown("本应用用于获取指定交易日内的指定游资买卖数据，不输入游资默认所有游资")
    st.markdown("左侧栏左下角设置参数")
    # Place parameter inputs in the left sidebar
    st.sidebar.header("参数设置")
    trade_date = st.sidebar.text_input("交易日期", "")
    ts_code = st.sidebar.text_input("股票代码", "")
    hm_name = st.sidebar.text_input("游资名称", "")
    start_date = st.sidebar.text_input("开始日期", "")
    end_date = st.sidebar.text_input("结束日期", "")
    limit = st.sidebar.number_input("查询的最大数据条数", min_value=1, value=100)

    # Query button in the sidebar
    if st.sidebar.button('查询数据'):
        # Fetch data
        df = fetch_data(trade_date, ts_code, hm_name, start_date, end_date, limit)

        # Check if data is returned
        if df.empty:
            st.warning("未获取到任何数据。请检查输入参数或网络连接。")
        else:
            # Convert amounts to '万' (without decimals)
            df['buy_amount'] = df['buy_amount'] // 10000
            df['sell_amount'] = df['sell_amount'] // 10000
            df['net_amount'] = df['net_amount'] // 10000

            # Rename columns to Chinese
            df.rename(columns={
                'trade_date': '交易日期',
                'ts_code': '股票代码',
                'ts_name': '股票名称',
                'buy_amount': '买入金额(万)',
                'sell_amount': '卖出金额(万)',
                'net_amount': '净买入金额(万)',
                'hm_name': '游资名称'
            }, inplace=True)

            # Display data table in the main page
            st.write("### 游资数据")
            st.dataframe(df, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
