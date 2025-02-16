import streamlit as st
import tushare as ts
import pandas as pd

tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

def get_qa_sz(ts_code, trade_date):
    """
    调用接口获取深圳数据，字段包括：ts_code, name, q, a, pub_time
    """
    try:
        df = pro.irm_qa_sz(
            ts_code=ts_code,
            trade_date=trade_date,
            fields="ts_code,name,q,a,pub_time"
        )
        df.rename(columns={
            'ts_code': '股票代码',
            'name': '股票名称',
            'q': '提问',
            'a': '回答',
            'pub_time': '发布时间'
        }, inplace=True)
        return df
    except Exception as e:
        st.error(f"获取深圳数据失败: {e}")
        return pd.DataFrame()

def get_qa_sh(ts_code, trade_date):
    """
    调用接口获取上海数据，字段包括：ts_code, name, q, a, pub_time
    """
    try:
        df = pro.irm_qa_sh(
            ts_code=ts_code,
            trade_date=trade_date,
            fields="ts_code,name,q,a,pub_time"
        )
        df.rename(columns={
            'ts_code': '股票代码',
            'name': '股票名称',
            'q': '提问',
            'a': '回答',
            'pub_time': '发布时间'
        }, inplace=True)
        return df
    except Exception as e:
        st.error(f"获取上海数据失败: {e}")
        return pd.DataFrame()

def style_df(df):
    """
    设置表格样式：
      - “股票代码”: 宽度80px，不换行
      - “股票名称”: 宽度100px，不换行
      - “发布时间”: 宽度80px，不换行
      - “提问”: 宽度400px，允许换行以显示完整内容
      - “回答”: 宽度400px，允许换行以显示完整内容
    """
    if df.empty:
        return df.style
    styled = df.style.set_properties(
        subset=["股票代码"],
        **{'min-width': '80px', 'max-width': '80px', 'white-space': 'nowrap'}
    ).set_properties(
        subset=["股票名称"],
        **{'min-width': '100px', 'max-width': '100px', 'white-space': 'nowrap'}
    ).set_properties(
        subset=["发布时间"],
        **{'min-width': '80px', 'max-width': '80px', 'white-space': 'nowrap'}
    ).set_properties(
        subset=["提问"],
        **{'min-width': '400px', 'max-width': '300px', 'white-space': 'pre-wrap'}
    ).set_properties(
        subset=["回答"],
        **{'min-width': '400px', 'max-width': '500px', 'white-space': 'pre-wrap'}
    )
    return styled

def main():
    st.title("董秘问答查询")
    st.markdown("请输入查询参数，获取深圳和上海的董秘回答数据。\n\n")

    # 侧边栏参数设置
    st.sidebar.header("参数设置")
    ts_code = st.sidebar.text_input("股票代码 (可留空)", value="")
    trade_date = st.sidebar.date_input("交易日期", value=pd.to_datetime("today"))
    trade_date_str = trade_date.strftime("%Y%m%d")

    if st.sidebar.button("开始查询"):
        st.subheader("深圳数据")
        df_sz = get_qa_sz(ts_code, trade_date_str)
        if not df_sz.empty:
            df_sz.index = range(1, len(df_sz) + 1)  # 索引从1开始
            st.markdown(style_df(df_sz).to_html(), unsafe_allow_html=True)
        else:
            st.info("未获取到深圳数据。")

        st.subheader("上海数据")
        df_sh = get_qa_sh(ts_code, trade_date_str)
        if not df_sh.empty:
            df_sh.index = range(1, len(df_sh) + 1)
            st.markdown(style_df(df_sh).to_html(), unsafe_allow_html=True)
        else:
            st.info("未获取到上海数据。")

if __name__ == "__main__":
    main()
