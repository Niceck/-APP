import streamlit as st
import tushare as ts
import pandas as pd

# 从 secrets.toml 文件中读取 Tushare API Token
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


def set_custom_css():
    """
    注入自定义 CSS，用于设置 st.dataframe 显示时的列宽
    假设表格列顺序固定为：
      1. 股票代码：80px，不换行
      2. 股票名称：100px，不换行
      3. 提问：400px，允许换行
      4. 回答：400px，允许换行
      5. 发布时间：80px，不换行
    """
    st.markdown(
        """
        <style>
        /* 股票代码 */
        div[data-testid="stDataFrameContainer"] table th:nth-child(1),
        div[data-testid="stDataFrameContainer"] table td:nth-child(1) {
            min-width: 80px;
            max-width: 80px;
            white-space: nowrap;
        }
        /* 股票名称 */
        div[data-testid="stDataFrameContainer"] table th:nth-child(2),
        div[data-testid="stDataFrameContainer"] table td:nth-child(2) {
            min-width: 100px;
            max-width: 100px;
            white-space: nowrap;
        }
        /* 提问 */
        div[data-testid="stDataFrameContainer"] table th:nth-child(3),
        div[data-testid="stDataFrameContainer"] table td:nth-child(3) {
            min-width: 400px;
            max-width: 400px;
            white-space: pre-wrap;
        }
        /* 回答 */
        div[data-testid="stDataFrameContainer"] table th:nth-child(4),
        div[data-testid="stDataFrameContainer"] table td:nth-child(4) {
            min-width: 400px;
            max-width: 400px;
            white-space: pre-wrap;
        }
        /* 发布时间 */
        div[data-testid="stDataFrameContainer"] table th:nth-child(5),
        div[data-testid="stDataFrameContainer"] table td:nth-child(5) {
            min-width: 80px;
            max-width: 80px;
            white-space: nowrap;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


def main():
    st.title("董秘问答查询")
    st.markdown("请输入查询参数，获取深圳和上海的董秘回答数据。")

    # 侧边栏参数设置
    st.sidebar.header("参数设置")
    ts_code = st.sidebar.text_input("股票代码 (可留空)", value="")
    trade_date = st.sidebar.date_input("交易日期", value=pd.to_datetime("today"))
    trade_date_str = trade_date.strftime("%Y%m%d")

    if st.sidebar.button("开始查询"):
        # 先注入自定义 CSS 设置列宽
        set_custom_css()

        st.subheader("深圳数据")
        df_sz = get_qa_sz(ts_code, trade_date_str)
        if not df_sz.empty:
            # 设置索引从1开始
            df_sz.index = range(1, len(df_sz) + 1)
            st.dataframe(df_sz, use_container_width=True)
        else:
            st.info("未获取到深圳数据。")

        st.subheader("上海数据")
        df_sh = get_qa_sh(ts_code, trade_date_str)
        if not df_sh.empty:
            df_sh.index = range(1, len(df_sh) + 1)
            st.dataframe(df_sh, use_container_width=True)
        else:
            st.info("未获取到上海数据。")


if __name__ == "__main__":
    main()
