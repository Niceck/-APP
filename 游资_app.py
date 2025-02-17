import tushare as ts
import pandas as pd
import streamlit as st

# 设置 Pandas 显示选项，确保完整显示内容
pd.set_option('display.max_colwidth', None)

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


# 拉取数据，只使用日期范围查询
def fetch_data(ts_code, hm_name, start_date, end_date, limit, offset=0):
    df = pro.hm_detail(
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
    st.title("参数设置")

    ts_code = st.text_input("输入股票代码（可留空）：XXXXXX.XX", "")
    hm_name = st.text_input("输入游资名称（可留空）", "")

    # 选择开始日期和结束日期（默认值设为当天）
    start_date = st.date_input("开始日期", value=pd.Timestamp.today())
    start_date_str = start_date.strftime("%Y%m%d") if start_date else ""
    end_date = st.date_input("结束日期", value=pd.Timestamp.today())
    end_date_str = end_date.strftime("%Y%m%d") if end_date else ""

    limit = st.number_input("查询的最大数据条数", min_value=1, value=100)

    if st.button('查询数据'):
        df = fetch_data(ts_code, hm_name, start_date_str, end_date_str, limit)

        if df.empty:
            st.warning("未获取到任何数据。请检查输入参数或网络连接。")
        else:
            # 转换金额单位为万（整数）
            df['buy_amount'] = df['buy_amount'] // 10000
            df['sell_amount'] = df['sell_amount'] // 10000
            df['net_amount'] = df['net_amount'] // 10000

            # 重命名列为中文
            df.rename(columns={
                'trade_date': '交易日期',
                'ts_code': '股票代码',
                'ts_name': '股票名称',
                'buy_amount': '买入金额(万)',
                'sell_amount': '卖出金额(万)',
                'net_amount': '净买入金额(万)',
                'hm_name': '游资名称'
            }, inplace=True)

            st.write("### 游资数据")
            st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
