import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime

# -----------------------------
# 全局变量定义
# -----------------------------
# 文件路径定义：将文件路径改为相对路径的 'date' 文件夹
NEWS_FILE = os.path.join("date", 'news_data.csv')
CCTV_NEWS_FILE = os.path.join("date", 'cctv_news_data.csv')

# 预设默认关键词列表（用于统计）
DEFAULT_KEYWORDS = [
    "航天", "军工", "卫星", "半导体", "量子", "AI", "华为", "电池", "航运", "白酒",
    "券商", "粮食", "农业", "养殖", "地产", "鸿蒙", "大飞机", "旅游", "保险", "热泵",
    "生物科技", "网络安全", "新能源", "环保", "医疗", "机器人", "电力", "农机", "食品安全",
    "区块链", "电子商务", "大数据", "云计算", "智能制造", "教培", "农药", "游戏",
    "5G", "物联网", "智能家居", "自动驾驶", "光伏", "种子", "风电", "跨境支付",
    "基因编辑", "碳中和", "虚拟现实", "算力", "数字货币", "核电", "锂矿", "银发",
    "低空", "氢能源", "人工智能", "养老", "光纤", "生物", "医美", "中药", "火箭",
]

# 预设默认查询关键词列表（用于查询新闻内容）
DEFAULT_QUERY_KEYWORDS = [
    "宽松货币政策", "减税降费", "监管放松", "增量资金入场", "外资持续流入", "融资融券余额上升",
    "业绩大幅增长", "业绩超预期", "订单快速增长", "产业政策利好", "技术突破或新产品落地",
    "市场情绪回暖", "估值修复", "券商上调目标价", "利好催化剂出现", "涨价", "提价",
    "供不应求", "量价齐升", "需求旺盛", "满产满销", "库存紧张", "紧缺资源", "持续热销",
    "订单爆满", "供给侧收缩", "毛利率提升", "盈利能力增强", "业绩预增", "利润空间扩大",
    "市场份额提升", "渗透率快速提升", "下游需求旺季", "并购", "重组", "持续增长",
    "机构一致", "稳定增长", "销售火爆", "产能扩张", "国际市场开拓", "资金大幅流入",
    "业绩超预期", "政策大力扶持", "核心资产", "优质赛道", "景气度持续攀升", "互换便利",
    "新能源浪潮", "资本开支增加", "研发投入提升", "机构抱团", "行业龙头", "批准",
    "产销量两旺", "重点扶持", "政策红利", "国内需求复苏", "全球需求回暖", "国家发改委",
    "规模化生产", "议价能力增强", "上游原材料涨价", "下游需求回暖", "财政部",
    "回购股份", "大股东增持", "国家队加仓", "外资持续流入", "板块轮动", "同比预增", "外汇局",
    "行业集中度提升", "产能利用率提升", "业绩拐点已至", "重磅订单签约", "复牌", "宏观审慎",
    "频繁中标", "商业模式升级", "客户结构优化", "大客户合作加深", "降费", "大基金",
    "两会重点关注", "大合同", "增量业绩", "应用前景", "逆回购", "【央行：",
]

# -----------------------------
# 数据加载及统计函数
# -----------------------------
def load_and_filter_data(user_date):
    """
    从 CSV 文件加载新闻数据并过滤指定日期及以后的数据
    """
    data = {}
    if os.path.exists(NEWS_FILE):
        try:
            data['news'] = pd.read_csv(NEWS_FILE, encoding='utf-8-sig')
            st.info(f"已加载新闻快讯数据，共 {len(data['news'])} 条。")
        except Exception as e:
            st.error(f"加载新闻快讯数据失败: {e}")
            data['news'] = pd.DataFrame()
    else:
        st.warning("新闻快讯数据文件不存在。")
        data['news'] = pd.DataFrame()

    if os.path.exists(CCTV_NEWS_FILE):
        try:
            data['cctv_news'] = pd.read_csv(CCTV_NEWS_FILE, encoding='utf-8-sig')
            st.info(f"已加载新闻联播数据，共 {len(data['cctv_news'])} 条。")
        except Exception as e:
            st.error(f"加载新闻联播数据失败: {e}")
            data['cctv_news'] = pd.DataFrame()
    else:
        st.warning("新闻联播数据文件不存在。")
        data['cctv_news'] = pd.DataFrame()

    # 日期过滤：仅保留指定日期及以后的数据
    if user_date:
        if not data['news'].empty and 'datetime' in data['news'].columns:
            data['news']['date_str'] = pd.to_datetime(data['news']['datetime'], errors='coerce').dt.strftime('%Y%m%d')
            data['news'] = data['news'][data['news']['date_str'] >= user_date]
        if not data['cctv_news'].empty and 'date' in data['cctv_news'].columns:
            data['cctv_news'] = data['cctv_news'][data['cctv_news']['date'].astype(str) >= user_date]
    st.success("数据加载和日期过滤完成。")
    return data


def aggregate_counts(data, keywords):
    """
    分别统计新闻快讯和新闻联播数据中各关键词出现次数
    """
    counts_news = {keyword: 0 for keyword in keywords}
    counts_cctv_news = {keyword: 0 for keyword in keywords}

    # 统计 news 数据
    if 'news' in data and not data['news'].empty:
        for content in data['news'].get('content', []):
            if pd.isna(content):
                continue
            for keyword in keywords:
                if re.search(re.escape(keyword), content, re.IGNORECASE):
                    counts_news[keyword] += 1

    # 统计 cctv_news 数据
    if 'cctv_news' in data and not data['cctv_news'].empty:
        for content in data['cctv_news'].get('content', []):
            if pd.isna(content):
                continue
            for keyword in keywords:
                if re.search(re.escape(keyword), content, re.IGNORECASE):
                    counts_cctv_news[keyword] += 1

    return counts_news, counts_cctv_news


def query_keywords_in_data(data, query_keywords):
    """
    查询关键词的新闻内容
    """
    output = ""
    for keyword in query_keywords:
        # 新闻联播数据中包含该关键词
        if 'cctv_news' in data and not data['cctv_news'].empty:
            matched_cctv = data['cctv_news'][
                data['cctv_news']['content'].str.contains(re.escape(keyword), case=False, na=False)
            ]
            if not matched_cctv.empty:
                output += f"**新闻联播中包含 '{keyword}' 的记录：**\n"
                matched_cctv_sorted = matched_cctv.sort_values(by='date', ascending=False)
                for idx, row in matched_cctv_sorted.iterrows():
                    date = str(row['date'])
                    content = str(row['content'])
                    output += f"- {date}: {content}\n"
        # 新闻快讯数据中包含该关键词
        if 'news' in data and not data['news'].empty:
            matched_news = data['news'][
                data['news']['content'].str.contains(re.escape(keyword), case=False, na=False)
            ]
            if not matched_news.empty:
                output += f"\n**新闻快讯中包含 '{keyword}' 的记录：**\n"
                matched_news_sorted = matched_news.sort_values(by='datetime', ascending=False)
                for idx, row in matched_news_sorted.iterrows():
                    datetime_str = str(row['datetime'])
                    content = str(row['content'])
                    output += f"- {datetime_str}: {content}\n"
        if output:
            with st.expander(f"关键词： {keyword}"):
                st.markdown(output)
    st.success("关键词查询完成。")


# -----------------------------
# 主函数：使用 Streamlit 构建页面
# -----------------------------
def main():
    st.title("新闻数据关键词统计与查询")
    st.markdown("本应用用于统计桌面上新闻数据中指定关键词的出现次数，并查询相关新闻内容。")

    # 参数设置移到主页
    user_input = st.text_input("请输入额外的查询关键词（用空格分隔）：", "")
    if user_input:
        query_keywords = [kw.strip() for kw in user_input.split() if kw.strip()]
        st.write(f"已输入查询关键词，共 {len(query_keywords)} 个。")
    else:
        query_keywords = DEFAULT_QUERY_KEYWORDS.copy()
        st.write("未输入查询关键词，将使用默认查询关键词。")

    user_date = st.text_input("请输入要统计的起始日期 (YYYYMMDD格式)：", "")
    if user_date:
        if not re.match(r'^\d{8}$', user_date):
            st.warning("输入日期格式无效，将使用当天日期。")
            user_date = datetime.now().strftime('%Y%m%d')
        else:
            st.write(f"使用输入的起始日期：{user_date}")
    else:
        user_date = datetime.now().strftime('%Y%m%d')
        st.write(f"未输入起始日期，使用当天日期：{user_date}")

    # 开始统计按钮
    if st.button("开始统计和查询"):
        st.info("正在加载数据，请稍后...")
        data = load_and_filter_data(user_date)

        # 统计关键词出现次数
        keywords = DEFAULT_KEYWORDS.copy()
        st.write(f"使用关键词列表进行统计，共 {len(keywords)} 个关键词。")
        st.info("正在统计关键词出现次数...")
        counts_news, counts_cctv_news = aggregate_counts(data, keywords)

        # 构造统计结果 DataFrame
        df = pd.DataFrame([{
            '题材': kw,
            '新闻联播': counts_cctv_news.get(kw, 0),
            '新闻快讯': counts_news.get(kw, 0)
        } for kw in keywords])
        # 分别按新闻联播和新闻快讯排序，并取前30
        df_cctv_sorted = df.sort_values(by='新闻联播', ascending=False).head(30).reset_index(drop=True)
        df_news_sorted = df.sort_values(by='新闻快讯', ascending=False).head(30).reset_index(drop=True)

        st.subheader("关键词出现次数统计结果")
        col1, col2 = st.columns(2)
        with col1:
            st.write("按 **新闻联播** 排序（前30）：")
            st.dataframe(df_cctv_sorted)
        with col2:
            st.write("按 **新闻快讯** 排序（前30）：")
            st.dataframe(df_news_sorted)

        # 查询关键词的新闻内容
        if query_keywords:
            query_keywords_in_data(data, query_keywords)
        else:
            st.info("未输入查询关键词，跳过查询新闻内容。")

        st.success("统计和查询全部完成。")


# -----------------------------
# 程序入口
# -----------------------------
if __name__ == "__main__":
    main()
