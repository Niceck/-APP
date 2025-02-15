import streamlit as st
import importlib

# 设置页面配置（必须放在所有 Streamlit 命令的最前面）
st.set_page_config(page_title="Stock Analysis App", layout="wide")

# 将本地图片转换为 base64 编码
image_path = "yinhe.png"
with open(image_path, "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode()

# CSS 设置背景图片
st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url("data:image/png;base64,{encoded_image}");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
        min-height: 100vh;
    }}
    </style>
    """, unsafe_allow_html=True
)

# 初始化 session_state 中的 selected_module
if "selected_module" not in st.session_state:
    st.session_state["selected_module"] = None

# 定义模块分组
update_modules = ["游资查询", "查询数据2"]
analysis_modules = ["评分系统", "新闻筛选"]
query_modules = ["更新题材池", "更新涨停池", "更新超买池",
    "更新游资池", "更新调研池", "更新扣非池",
    "更新新闻快讯", "更新新闻联播"]  # 查询数据模块列表

# 在侧边栏创建三个展开器
update_expander = st.sidebar.expander("查询数据", expanded=False)
analysis_expander = st.sidebar.expander("分析数据", expanded=False)
query_expander = st.sidebar.expander("更新数据", expanded=False)  # 可根据需求设置 expanded 参数

# 在“更新数据”展开器中显示按钮
for module in update_modules:
    if update_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 在“分析数据”展开器中显示按钮
for module in analysis_modules:
    if analysis_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 在“查询数据”展开器中显示按钮
for module in query_modules:
    if query_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

st.sidebar.markdown("---")

# 模块名称与对应模块文件的映射
module_map = {
    "更新题材池": "题材数据_app",
    "更新涨停池": "涨停数据_app",
    "更新超买池": "超买池_app",
    "新闻筛选": "新闻查询_app",
    "更新游资池": "游资数据_app",
    "评分系统": "放量题材_app",
    "更新扣非池": "扣非净利润股票池",
    "游资查询": "游资_app",
    "更新新闻快讯": "快讯_app",
    "更新新闻联播": "联播_app",
    "更新调研池": "调研_app",
    "查询数据1": "查询数据1_app",  # 根据实际模块文件调整映射名称
    "查询数据2": "查询数据2_app"   # 根据实际模块文件调整映射名称
}

# 获取当前选中的模块
selected_module = st.session_state["selected_module"]

if selected_module:
    result_key = f"{module_map.get(selected_module)}_result"
    module_cache_prefix = f"{module_map.get(selected_module)}_"  # 使用模块名称作为前缀

    try:
        # 如果缓存中没有结果，则动态导入模块并执行
        if result_key not in st.session_state:
            module_name = module_map.get(selected_module)
            module_app = importlib.import_module(module_name)
            # 执行模块的主函数
            module_app.main()
        else:
            st.write("加载缓存数据...")
            st.session_state[result_key]
    except Exception as e:
        st.error(f"调用 {selected_module} 模块时出错: {e}")
else:
    # 主页展示内容
    st.title("欢迎使用 Stock Analysis App")
    st.write("这是一个功能强大的股票数据分析工具。")
    st.markdown("""
    ### 应用功能
    - **查询数据**：游资数据、机构数据、资金流向等
    - **分析数据**：股票池筛选分析、新闻筛选分析等
    - **更新数据**：更新题材池、涨停池、超买池、游资池、调研池、扣非池、新闻快讯、新闻联播等数据    
    """)
    st.info("请在左侧栏中选择相应模块开始使用。")

# 示例函数：渲染 HTML 表格
def render_html_table(results):
    html_daily, html_stocks = "", ""
    if "daily_rates_df" in results:
        html_daily = results["daily_rates_df"].style.hide(axis="index").to_html()
    if "stocks_df" in results:
        html_stocks = results["stocks_df"].style.hide(axis="index").to_html()
    return html_daily, html_stocks
