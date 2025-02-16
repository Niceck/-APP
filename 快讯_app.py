import tushare as ts
import pandas as pd
import os
import time
import logging
from datetime import datetime
import streamlit as st

# 设置日志记录
logging.basicConfig(
    filename='tushare_errors.log',
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 设置相对路径的 'date' 文件夹
DATE_FOLDER = "date"

# 文件路径定义
NEWS_FILE = os.path.join(DATE_FOLDER, 'news_data.csv')
CACHE_FILE = os.path.join(DATE_FOLDER, 'news_cache.txt')  # 用于存储最新的 datetime

# 从 secrets.toml 文件中读取 Tushare API Token
tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


def save_data_update(df_new, save_file):
    """
    更新保存数据到指定文件。若文件存在，则合并并去重后保存；若不存在，则直接保存新数据。
    """
    try:
        # 确保文件夹存在
        os.makedirs(DATE_FOLDER, exist_ok=True)

        if os.path.exists(save_file):
            # 读取现有数据
            df_existing = pd.read_csv(save_file, encoding='utf-8-sig')
            st.info(f"读取到现有数据，共 {len(df_existing)} 条记录。")

            # 合并新数据与现有数据
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            st.info(f"合并后数据共 {len(df_combined)} 条记录。")

            # 去重，基于 'datetime' 和 'content' 两个字段
            before_dedup = len(df_combined)
            df_combined.drop_duplicates(subset=["datetime", "content"], inplace=True)
            after_dedup = len(df_combined)
            duplicates_removed = before_dedup - after_dedup
            if duplicates_removed > 0:
                st.info(f"去除了 {duplicates_removed} 条重复记录。")

            # 保存合并去重后的数据
            df_combined.to_csv(save_file, index=False, encoding='utf-8-sig')
            st.success(f"已更新保存数据到 {save_file}。")
        else:
            # 文件不存在，直接保存新数据
            df_new.to_csv(save_file, index=False, encoding='utf-8-sig')
            st.success(f"已保存新数据到 {save_file}。")
    except Exception as e:
        st.error(f"保存数据失败: {e}")
        logging.error("保存数据失败", exc_info=True)


def read_last_datetime(cache_file):
    """
    读取缓存文件中的最后一个 datetime。
    """
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                last_datetime_str = f.read().strip()
                last_datetime = datetime.strptime(last_datetime_str, '%Y-%m-%d %H:%M:%S')
                st.info(f"读取到缓存的最新 datetime: {last_datetime}")
                return last_datetime
        except Exception as e:
            st.error(f"读取缓存文件失败: {e}")
            logging.error("读取缓存文件失败", exc_info=True)
            return None
    else:
        st.info("缓存文件不存在。将拉取所有可用数据。")
        return None


def update_cache(cache_file, latest_datetime):
    """
    更新缓存文件中的最新 datetime。
    """
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(latest_datetime.strftime('%Y-%m-%d %H:%M:%S'))
        st.success(f"已更新缓存文件，最新 datetime 为: {latest_datetime}")
    except Exception as e:
        st.error(f"更新缓存文件失败: {e}")
        logging.error("更新缓存文件失败", exc_info=True)


def fetch_news_data(pro, last_datetime=None, limit=1000, calls_per_minute=10):
    """
    拉取新闻快讯数据，并处理分页。

    参数：
    - pro: Tushare Pro 接口实例。
    - last_datetime: 上次拉取数据的最新 datetime，用于只拉取新数据。
    - limit: 每次调用返回的记录数。
    - calls_per_minute: 每分钟最大调用次数。

    采用滑动窗口的方式记录过去60秒内的调用次数，超过限制则等待剩余时间。
    """
    all_data = []
    offset = 0
    api_call_timestamps = []  # 用于记录每次调用的时间戳

    st.info("开始拉取新闻快讯数据...")

    while True:
        current_time = time.time()
        # 清理超过60秒的时间戳
        api_call_timestamps = [t for t in api_call_timestamps if current_time - t < 60]
        if len(api_call_timestamps) >= calls_per_minute:
            sleep_time = 60 - (current_time - api_call_timestamps[0])
            st.info(f"达到每分钟调用上限, 等待 {sleep_time:.2f} 秒")
            time.sleep(sleep_time)
            continue

        params = {
            "limit": limit,
            "offset": offset,
            "fields": ["datetime", "content", "channels"]
        }

        if last_datetime:
            start_time_str = last_datetime.strftime('%Y%m%d%H%M%S')
            params["start_time"] = start_time_str  # 假设 API 支持该参数

        try:
            df_news = pro.news(**params)
            api_call_timestamps.append(time.time())  # 记录此次调用的时间

            if df_news.empty:
                st.info(f"news数据为空，无新数据。（调用次数：{len(api_call_timestamps)}）")
                break

            if last_datetime:
                df_news['datetime'] = pd.to_datetime(df_news['datetime'], format='%Y-%m-%d %H:%M:%S')
                df_news = df_news[df_news['datetime'] > last_datetime]
                if df_news.empty:
                    st.info(f"没有比缓存时间更新的数据。（调用次数：{len(api_call_timestamps)}）")
                    break

            all_data.append(df_news)
            st.info(f"已拉取到 {len(df_news)} 条news数据（调用次数：{len(api_call_timestamps)}，offset={offset}）。")
            offset += limit
        except Exception as e:
            st.error(f"news 数据拉取失败（调用次数：{len(api_call_timestamps) + 1}，offset={offset}）：{e}")
            logging.error("news 数据拉取失败", exc_info=True)
            break

    if all_data:
        try:
            final_df = pd.concat(all_data, ignore_index=True)
            st.info(f"共拉取到 {len(final_df)} 条新闻快讯数据。")

            # 对数据进行去重操作
            before_dedup = len(final_df)
            final_df.drop_duplicates(subset=["datetime", "content"], inplace=True)
            after_dedup = len(final_df)
            duplicates_removed = before_dedup - after_dedup
            if duplicates_removed > 0:
                st.info(f"去除了 {duplicates_removed} 条重复记录。")

            return final_df
        except Exception as e:
            st.error(f"合并news数据失败：{e}")
            logging.error("合并news数据失败", exc_info=True)
            return pd.DataFrame()
    else:
        st.info("未拉取到任何news数据。")
        return pd.DataFrame()


def main():
    st.title("新闻数据拉取与保存")

    try:
        pro = ts.pro_api(tushare_token)
        st.success("成功初始化Tushare Pro接口。")
    except Exception as e:
        st.error(f"初始化Tushare Pro接口失败: {e}")
        logging.error("初始化Tushare Pro接口失败", exc_info=True)
        return

    # 读取缓存中的最新 datetime
    last_datetime = read_last_datetime(CACHE_FILE)

    # 拉取新闻快讯数据
    news_df = fetch_news_data(
        pro,
        last_datetime=last_datetime,
        limit=1000,
        calls_per_minute=8  # 限制每分钟最多调用 10 次接口
    )

    if not news_df.empty:
        # 保存新数据并合并
        save_data_update(news_df, NEWS_FILE)

        # 更新缓存中的最新 datetime
        latest_datetime = news_df['datetime'].max()
        if isinstance(latest_datetime, str):
            latest_datetime = datetime.strptime(latest_datetime, '%Y-%m-%d %H:%M:%S')
        update_cache(CACHE_FILE, latest_datetime)
    else:
        st.info("没有新数据需要保存。")

    st.success("数据拉取并保存完成。")


if __name__ == "__main__":
    main()
