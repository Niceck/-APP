import tushare as ts
import pandas as pd
import os
import streamlit as st

# 设置 Pandas 显示选项，确保 'rece_org' 列完全显示
pd.set_option('display.max_colwidth', None)

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()

# 拉取数据
def fetch_data():
    df = pro.stk_surv(
        ts_code="",
        trade_date="",
        start_date="",
        end_date="",
        limit="1000",  # 设置限制最大返回数量
        offset="0",
        fields=["ts_code", "name", "rece_org"]
    )
    return df

# 主函数，执行应用的核心逻辑
def main():
    try:
        df = fetch_data()

        # 检查是否成功获取数据
        if df.empty:
            st.warning("未获取到任何数据。请检查接口参数或网络连接。")
            return

        # 按 'ts_code' 和 'name' 分组，并将同一股票的 'rece_org' 合并为一个字符串
        df_grouped = df.groupby(['ts_code', 'name'])['rece_org'] \
                      .apply(lambda x: ', '.join(x.unique())).reset_index()

        # 计算每个股票的 'rece_org' 数量
        df_grouped['rece_org_count'] = df_grouped['rece_org'] \
                                       .apply(lambda x: len(x.split(', ')))
        # 根据 'rece_org_count' 进行降序排序
        df_grouped.sort_values(by='rece_org_count', ascending=False, inplace=True)
        df_grouped.reset_index(drop=True, inplace=True)
        df_grouped.drop(columns=['rece_org_count'], inplace=True)

        # 保存文件到相对路径的 'date' 文件夹
        output_folder = "date"
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, "机构调研.txt")

        # 如果文件存在，读取现有数据并去重；否则直接写入新的数据
        if os.path.exists(output_file):
            with open(output_file, "r", encoding="utf-8") as file:
                existing_data = file.readlines()
            # 获取文件中已保存的股票代码（去除重复）
            existing_codes = set(line.strip() for line in existing_data)
        else:
            existing_codes = set()

        # 获取新的股票数据（去重）
        new_data = set(df_grouped['ts_code'].tolist())

        # 合并现有和新的数据，并去重
        all_codes = existing_codes.union(new_data)

        # 将去重后的数据保存到文件
        with open(output_file, "w", encoding="utf-8") as file:
            for code in sorted(all_codes):
                file.write(f"{code}\n")

        st.success(f"股票数据已成功保存到文件：{output_file}")

        # ---------------- 输出表格 ----------------
        st.subheader("机构调研数据预览")
        st.table(df_grouped)

        # 返回文件路径以便主脚本调用 Git 更新（如果需要）
        return output_file

    except Exception as e:
        st.error(f"程序执行出错：{e}")
        return None

if __name__ == "__main__":
    main()
