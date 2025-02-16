import tushare as ts
import pandas as pd
import os
import logging
import time  # 用于 sleep
from datetime import datetime, timedelta
import streamlit as st
from git_utils import git_update, git_push  # 导入 Git 更新函数

# ============ 配置信息 ============ #
# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()
MAX_RETRIES = 5  # 拉取数据时的最大重试次数
RETRY_SLEEP = 5  # 单次重试等待秒数

# 日志配置
logging.basicConfig(
    filename='tushare_errors.log',
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 设置相对路径的 'date' 文件夹
DATE_FOLDER = "date"
os.makedirs(DATE_FOLDER, exist_ok=True)

# 文件路径定义：存储在 'date' 文件夹
CCTV_NEWS_FILE = os.path.join(DATE_FOLDER, 'cctv_news_data.csv')


# ============ 数据清洗函数 ============ #
def clean_df(df):
    """
    清洗 DataFrame 中的日期、标题和内容字段，统一格式，移除多余空格和换行符
    """
    df = df.copy()
    # 对日期进行处理：尝试转换为 datetime 后再格式化为 YYYYMMDD 字符串
    try:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y%m%d')
    except Exception as e:
        logging.error("日期转换错误", exc_info=True)
        df['date'] = df['date'].astype(str)
    # 对 title 和 content 字段：去除首尾空格，并将多个空白字符合并为一个空格
    for col in ['title', 'content']:
        df[col] = df[col].astype(str).apply(lambda x: " ".join(x.split()))
    return df


# ============ 工具函数 ============ #

def read_local_cache(file_path):
    """
    尝试读取本地 CSV，
    如果存在且不为空，则返回清洗后的 DataFrame，否则返回 None
    """
    if not os.path.exists(file_path):
        return None
    try:
        # 强制将 date 列作为字符串读取
        df = pd.read_csv(file_path, dtype={'date': str})
        if df.empty:
            return None
        df = clean_df(df)
        return df
    except Exception as e:
        st.write(f"读取本地缓存失败: {e}")
        logging.error("读取本地缓存失败", exc_info=True)
        return None


def get_start_date_from_df(df):
    """
    从已有数据中找出最大 date，
    返回该日期（YYYYMMDD）的字符串，作为增量数据拉取的起始日期
    注意：这里直接返回最大日期，因为 tushare 接口的 start_date 参数是包含该日期的，
    这样拉取的记录可能重复，但后续会通过合并去重解决。
    """
    try:
        # df 已经经过 clean_df 清洗，保证日期格式一致
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
        max_date = df['date'].max()
        if pd.isnull(max_date):
            return None
        return max_date.strftime('%Y%m%d')
    except Exception as e:
        st.write(f"解析本地数据最大日期出错: {e}")
        logging.error("解析本地数据最大日期出错", exc_info=True)
        return None


def fetch_cctv_data_full(pro, limit=1000):
    """
    使用 offset 分页循环，拉取【全量】数据（从最早到最新）。
    如果出现异常则等待 RETRY_SLEEP 秒后重试，最多重试 MAX_RETRIES 次。
    """
    all_data = []
    offset = 0
    call_count = 0
    success = True

    st.write("开始【全量】拉取新闻联播数据 (使用 offset) ...")
    while True:
        retries = 0
        df_cctv = pd.DataFrame()
        while True:
            try:
                df_cctv = pro.cctv_news(
                    limit=limit,
                    offset=offset,
                    fields=["date", "title", "content"]
                )
                call_count += 1
                break  # 成功后跳出重试循环
            except Exception as e:
                retries += 1
                st.write(f"cctv_news 数据拉取失败 (offset={offset}，第 {retries} 次重试): {e}")
                logging.error("cctv_news 数据拉取失败", exc_info=True)
                if retries < MAX_RETRIES:
                    st.write(f"等待 {RETRY_SLEEP} 秒后再次尝试...（已失败 {retries} 次）")
                    time.sleep(RETRY_SLEEP)
                else:
                    st.write("超过最大重试次数，拉取失败，退出...")
                    success = False
                    break
        if not success:
            break
        if df_cctv.empty:
            if offset == 0:
                st.write("cctv_news 数据为空，无任何数据。")
            else:
                st.write("cctv_news 数据拉取完成，无更多数据。")
            break

        all_data.append(df_cctv)
        st.write(f"  -> 第 {call_count} 次调用，offset={offset}，本次获取 {len(df_cctv)} 条。")
        offset += limit

    if success and all_data:
        try:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = clean_df(final_df)
            st.write(f"【全量】共拉取到 {len(final_df)} 条数据。")
            return final_df, True
        except Exception as e:
            st.write(f"合并 cctv_news 数据失败：{e}")
            logging.error("合并 cctv_news 数据失败", exc_info=True)
            return pd.DataFrame(), False
    else:
        return pd.DataFrame(), False


def fetch_cctv_data_increment(pro, start_date):
    """
    只调用一次，拉取【增量】数据（包含 start_date 当天的数据）。
    重试逻辑同上。
    """
    st.write(f"开始【增量】拉取新闻联播数据，从 {start_date} 起...")
    retries = 0
    df_cctv = pd.DataFrame()
    while True:
        try:
            df_cctv = pro.cctv_news(
                fields=["date", "title", "content"],
                start_date=start_date
            )
            break
        except Exception as e:
            retries += 1
            st.write(f"增量 cctv_news 数据拉取失败 (第 {retries} 次重试): {e}")
            logging.error("增量 cctv_news 数据拉取失败", exc_info=True)
            if retries < MAX_RETRIES:
                st.write(f"等待 {RETRY_SLEEP} 秒后再次尝试...（已失败 {retries} 次）")
                time.sleep(RETRY_SLEEP)
            else:
                st.write("超过最大重试次数，增量拉取失败，退出...")
                return pd.DataFrame(), False
    df_cctv = clean_df(df_cctv)
    if df_cctv.empty:
        st.write("  -> cctv_news 数据为空，无新数据。")
    else:
        st.write(f"【增量】本次拉取到 {len(df_cctv)} 条新数据。")
    return df_cctv, True


def merge_and_save(new_df, file_path):
    """
    将新数据与本地缓存合并，
    按 (date, title, content) 去重后保存到 CSV 文件中。
    """
    new_df = clean_df(new_df)
    if not os.path.exists(file_path):
        st.write("本地无缓存文件，直接保存新数据...")
        new_df.drop_duplicates(subset=["date", "title", "content"], keep='last', inplace=True)
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        st.write(f"已保存 {len(new_df)} 行数据到 {file_path}.")
        return

    try:
        old_df = pd.read_csv(file_path, dtype={'date': str})
        old_df = clean_df(old_df)
    except Exception as e:
        st.write(f"读取本地文件失败，直接覆盖保存新数据: {e}")
        logging.error("读取本地文件失败", exc_info=True)
        new_df.drop_duplicates(subset=["date", "title", "content"], keep='last', inplace=True)
        new_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        st.write(f"已保存 {len(new_df)} 行数据到 {file_path}.")
        return

    st.write(f"本地原有 {len(old_df)} 行数据，新拉取 {len(new_df)} 行，开始合并去重...")
    merged_df = pd.concat([old_df, new_df], ignore_index=True)
    merged_df = clean_df(merged_df)
    merged_df.drop_duplicates(subset=["date", "title", "content"], keep='last', inplace=True)
    merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    st.write(f"合并去重后，共 {len(merged_df)} 行，已覆盖保存到 {file_path}。")


# ============ 主逻辑 ============ #

def main():
    st.title("CCTV 新闻数据拉取与合并")

    # 1. 初始化 Tushare 接口
    try:
        pro = ts.pro_api(tushare_token)
        st.success("成功初始化 Tushare Pro 接口。")
    except Exception as e:
        st.error(f"初始化 Tushare Pro 接口失败: {e}")
        logging.error("初始化 Tushare Pro 接口失败", exc_info=True)
        return

    # 2. 尝试读取本地缓存数据
    local_df = read_local_cache(CCTV_NEWS_FILE)

    # 3. 判断是全量拉取还是增量拉取
    if local_df is None:
        # 本地没有缓存文件或文件为空，则执行全量拉取
        new_df, success = fetch_cctv_data_full(pro, limit=1000)
    else:
        # 已有缓存，则取其中最大日期作为增量拉取的起始日期
        start_date = get_start_date_from_df(local_df)
        if start_date is None:
            st.write("无法解析本地最大日期，将退回到【全量】拉取...")
            new_df, success = fetch_cctv_data_full(pro, limit=1000)
        else:
            new_df, success = fetch_cctv_data_increment(pro, start_date)

    # 4. 合并数据并去重保存
    if success and not new_df.empty:
        merge_and_save(new_df, CCTV_NEWS_FILE)
        # 输出合并后的数据预览
        try:
            merged_df = pd.read_csv(CCTV_NEWS_FILE, dtype={'date': str})
            st.subheader("合并后的新闻数据预览")
            st.dataframe(merged_df)
        except Exception as e:
            st.write(f"读取合并后的数据预览失败: {e}")
        # 执行 Git 更新操作：这里设置更新模式为 "update"
        if os.path.exists(CCTV_NEWS_FILE):
            git_update(CCTV_NEWS_FILE, update_mode="update")
            git_push(branch="main")
    else:
        st.write("无新数据或拉取失败，不更新本地文件。")

    st.write("数据拉取与保存流程结束。")

if __name__ == "__main__":
    main()
