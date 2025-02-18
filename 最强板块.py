import streamlit as st
import tushare as ts
import pandas as pd
import datetime as dt


def main():
    # ------------------ 初始化 Tushare 接口 ------------------
    tushare_token = st.secrets["api_keys"]["tushare_token"]
    ts.set_token(tushare_token)
    pro = ts.pro_api()

    # ------------------ 页面标题 ------------------
    st.title("板块查询")

    # ------------------ 使用 st.tabs 实现横向标签 ------------------
    tab1, tab2 = st.tabs(["涨停题材列表", "题材成分股查询"])

    # ================== 标签页1：涨停题材列表 ==================
    with tab1:
        st.header("涨停题材列表")

        # 使用日期选择框，默认选为当天
        trade_date = st.date_input("请选择交易日期", value=dt.datetime.today())
        trade_date_str = trade_date.strftime("%Y%m%d")  # 转换为字符串格式

        if st.button("查询涨停题材列表", key="btn_limit_cpt_list"):
            try:
                # 拉取数据
                df = pro.limit_cpt_list(
                    trade_date=trade_date_str,
                    ts_code="",
                    start_date="",
                    end_date="",
                    limit="",
                    offset="",
                    fields=[
                        "ts_code",
                        "name",
                        "trade_date",
                        "days",
                        "up_stat",
                        "cons_nums",
                        "up_nums",
                        "rank"
                    ]
                )
                if df.empty:
                    st.info("未查询到数据")
                else:
                    # 将字段名称转换为中文
                    df.rename(columns={
                        "ts_code": "题材代码",
                        "name": "题材名称",
                        "trade_date": "交易日期",
                        "days": "连续天数",
                        "up_stat": "涨停状态",
                        "cons_nums": "连板家数",
                        "up_nums": "涨停数量",
                        "rank": "排名"
                    }, inplace=True)
                    # 按“排名”升序排序，并重置索引
                    df = df.sort_values(by="排名", ascending=True).reset_index(drop=True)
                    # 将索引设置为从1开始
                    df.index = range(1, len(df) + 1)
                    st.dataframe(df)
            except Exception as e:
                st.error(f"查询失败：{e}")

    # ================== 标签页2：题材成分股查询 ==================
    with tab2:
        st.header("题材成分股查询")

        # 用户输入题材代码
        ts_code_input = st.text_input("请输入题材代码")

        if st.button("查询题材成分股", key="btn_ths_member"):
            if ts_code_input.strip() == "":
                st.info("请先输入题材代码再进行查询。")
            else:
                try:
                    df = pro.ths_member(
                        ts_code=ts_code_input,
                        fields=[
                            "ts_code",
                            "con_code",
                            "con_name"
                        ]
                    )
                    if df.empty:
                        st.info("未查询到数据")
                    else:
                        # 将字段名称转换为中文
                        df.rename(columns={
                            "ts_code": "题材代码",
                            "con_code": "成分股代码",
                            "con_name": "成分股名称"
                        }, inplace=True)
                        # 重置索引并设置为从1开始
                        df = df.reset_index(drop=True)
                        df.index = range(1, len(df) + 1)
                        st.dataframe(df)
                except Exception as e:
                    st.error(f"查询失败：{e}")


if __name__ == '__main__':
    main()
