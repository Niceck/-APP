import streamlit as st
import importlib
import requests
from streamlit_lottie import st_lottie

# ------------------------------------------------------
# 1. 页面基础配置：页面标题、布局
# ------------------------------------------------------
st.set_page_config(page_title="恢恢数据分析 App", layout="wide")

# ------------------------------------------------------
# 2. 初始化 session_state 中的 selected_module
# ------------------------------------------------------
if "selected_module" not in st.session_state:
    st.session_state["selected_module"] = None

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

# 查询数据功能
update_expander = st.sidebar.expander("查询数据", expanded=False)
for module in update_modules:
    if update_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 分析数据功能
analysis_expander = st.sidebar.expander("分析数据", expanded=False)
for module in analysis_modules:
    if analysis_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 更新数据功能
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
    st.title("欢迎使用 恢恢数据分析 App")
    st.markdown("这是一个功能强大的 **股票数据分析工具**。")
    st.markdown("""
    ### 应用功能
    - **查询数据**：游资数据、机构数据、题材成分股资金流向等
    - **分析选股**：股票池层层筛选、计算评分，助力选股决策
    - **更新数据**：已自动更新，无需更新
    - **数据刷新**：通常在每个交易日结束两小时左右完成更新
    """)
    st.info("请在左侧栏中选择相应功能模块开始使用。")


    # ------------------------------------------------------
    # 8. 动画与互动效果
    # ------------------------------------------------------
    # 示例1：使用 Lottie 动画（如加载一段交易或数据流动的动画）
    def load_lottieurl(url: str):
        r = requests.get(url)
        if r.status_code != 200:
            return None
        return r.json()


    # 例如：加载一段 Lottie 动画
    lottie_url = "https://assets7.lottiefiles.com/packages/lf20_rnnlxazi.json"  # 示例动画链接
    lottie_json = load_lottieurl(lottie_url)
    if lottie_json:
        st_lottie(lottie_json, speed=1, height=300, key="lottie_animation")

    # 示例2：自定义 CSS 动画，创建渐变背景和闪烁图标
    animated_css = """
    <style>
    /* 渐变背景动画 */
    @keyframes gradient {
      0% {background-position: 0% 50%;}
      50% {background-position: 100% 50%;}
      100% {background-position: 0% 50%;}
    }
    .animated-background {
      background: linear-gradient(270deg, #ff9a9e, #fad0c4, #fad0c4);
      background-size: 600% 600%;
      animation: gradient 8s ease infinite;
      padding: 1rem;
      border-radius: 10px;
      color: white;
      text-align: center;
      font-size: 1.2rem;
      font-weight: bold;
    }
    /* 图标闪烁动画 */
    @keyframes blink {
      0%, 100% { opacity: 1; }
      50% { opacity: 0; }
    }
    .blinking-icon {
      animation: blink 1s infinite;
      font-size: 2rem;
      color: #ff4500;
    }
    </style>
    """
    st.markdown(animated_css, unsafe_allow_html=True)

    # 显示带有动画效果的区块
    st.markdown('<div class="animated-background">欢迎来到恢恢数据分析 App！</div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="text-align: center;"><span class="blinking-icon">🔥</span> 今天是交易好日子 <span class="blinking-icon">🔥</span></div>',
        unsafe_allow_html=True)

    # 示例3：留言反馈区（互动性）
    with st.expander("留言反馈"):
        feedback = st.text_area("请输入您的反馈意见：")
        if st.button("提交反馈"):
            st.success("感谢您的反馈，我们会持续改进！")
