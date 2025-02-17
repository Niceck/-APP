import streamlit as st
import importlib
import requests
from streamlit_lottie import st_lottie

# ------------------------------------------------------
# 1. 页面基础配置：页面标题、布局
# ------------------------------------------------------
st.set_page_config(page_title="恢恢数据分析 App", layout="wide")

# ------------------------------------------------------
# 2. 初始化 session_state 中的 selected_module 和反馈列表
# ------------------------------------------------------
if "selected_module" not in st.session_state:
    st.session_state["selected_module"] = None

if "feedback_list" not in st.session_state:
    st.session_state["feedback_list"] = []

# ------------------------------------------------------
# 3. 定义模块分组与模块映射
# ------------------------------------------------------
update_modules = ["游资查询", "连板查询", "董秘查询", "新闻筛选", "题材成分股查询"]
analysis_modules = ["评分系统"]
query_modules = [
    "更新题材池", "更新涨停池", "更新超买池", "更新游资池",
    "更新调研池", "更新扣非池", "更新股东池",
    "更新新闻快讯", "更新新闻联播"
]

module_map = {
    "更新题材池": "题材数据_app",
    "更新涨停池": "涨停数据_app",
    "更新超买池": "超买池_app",
    "新闻筛选": "新闻查询_app",
    "更新游资池": "游资数据_app",
    "评分系统": "放量题材_app",
    "更新扣非池": "扣非净利润池",
    "游资查询": "游资_app",
    "更新新闻快讯": "快讯_app",
    "更新新闻联播": "联播_app",
    "更新调研池": "调研_app",
    "题材成分股查询": "题材成分股",
    "董秘查询": "董秘查询",
    "更新股东池": "十大股东",
    "连板查询": "连板查询"
}

# ------------------------------------------------------
# 4. 侧边栏：功能导航区
# ------------------------------------------------------
st.sidebar.title("功能导航")

# “查询数据”功能
update_expander = st.sidebar.expander("查询数据", expanded=False)
for module in update_modules:
    if update_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# “分析数据”功能
analysis_expander = st.sidebar.expander("分析数据", expanded=False)
for module in analysis_modules:
    if analysis_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# “更新数据”功能
query_expander = st.sidebar.expander("更新数据（无需更新）", expanded=False)
for module in query_modules:
    if query_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

st.sidebar.markdown("---")

# ------------------------------------------------------
# 5. 返回主页的回调函数
# ------------------------------------------------------
def return_home():
    st.session_state["selected_module"] = None
    try:
        st.experimental_rerun()
    except Exception:
        pass

# ------------------------------------------------------
# 6. 根据 selected_module 动态加载并运行对应模块
# ------------------------------------------------------
selected_module = st.session_state["selected_module"]

if selected_module:
    st.sidebar.button("返回主页", on_click=return_home)
    result_key = f"{module_map.get(selected_module)}_result"
    module_cache_prefix = f"{module_map.get(selected_module)}_"
    try:
        if result_key not in st.session_state:
            module_name = module_map.get(selected_module)
            module_app = importlib.import_module(module_name)
            module_app.main()
        else:
            st.write("加载缓存数据...")
            st.session_state[result_key]
    except Exception as e:
        st.error(f"调用 {selected_module} 模块时出错: {e}")
else:
    # ------------------------------------------------------
    # 7. 主页内容：欢迎页
    # ------------------------------------------------------
    # 注入自定义 CSS，用于闪烁图标动画
    st.markdown("""
    <style>
    @keyframes blink {
        0% { opacity: 1; }
        50% { opacity: 0.3; }
        100% { opacity: 1; }
    }
    .flashing-icon {
        animation: blink 1.5s infinite;
        width: 36px;
        height: 36px;
        vertical-align: middle;
    }
    </style>
    """, unsafe_allow_html=True)

    # 主页标题增加左右闪烁的星形图标装饰
    st.markdown("""
    <div style="text-align: center; margin-bottom: 20px;">
        <img src="https://img.icons8.com/fluency/48/000000/star.png" class="flashing-icon" style="margin-right: 10px;">
        <span style="font-size: 2.5rem; font-weight: bold;">欢迎使用 恢恢数据分析 App</span>
        <img src="https://img.icons8.com/fluency/48/000000/star.png" class="flashing-icon" style="margin-left: 10px;">
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ### 应用功能
    - **查询数据**：游资数据、机构数据、题材成分股资金流向等
    - **分析选股**：股票池层层筛选、计算评分，助力选股决策
    - **更新数据**：已自动更新，无需更新
    - **数据刷新**：通常在每个交易日结束两小时左右完成更新
    """)
    st.info("请在左侧栏中选择相应功能模块开始使用。")

    # ------------------------------------------------------
    # 8. 动画展示区：宽屏深色宇宙/银河系动画
    # ------------------------------------------------------
    def load_lottie_url(url: str):
        r = requests.get(url)
        if r.status_code != 200:
            return None
        return r.json()

    # 使用一个深色宇宙/银河系动画的有效 URL
    lottie_space_url = "https://assets7.lottiefiles.com/packages/lf20_x62chJ.json"
    lottie_space_json = load_lottie_url(lottie_space_url)
    if lottie_space_json:
        st_lottie(
            lottie_space_json,
            speed=1,
            width=1000,   # 宽屏展示
            height=500,   # 适当增高
            key="lottie_space"
        )
    else:
        st.warning("无法加载动画，可能是链接无效。")

    # ------------------------------------------------------
    # 9. 留言反馈区：用户提交反馈并实时显示
    # ------------------------------------------------------
    st.subheader("留言反馈")
    feedback_input = st.text_area("请输入您的反馈意见：", key="feedback_input")
    if st.button("提交反馈"):
        if feedback_input.strip():
            st.session_state["feedback_list"].append(feedback_input.strip())
            st.success("感谢您的反馈，我们会持续改进！")
        else:
            st.warning("请先输入内容再提交。")

    if st.session_state["feedback_list"]:
        with st.expander("查看所有反馈记录", expanded=False):
            for idx, fb in enumerate(st.session_state["feedback_list"], start=1):
                st.write(f"{idx}. {fb}")
