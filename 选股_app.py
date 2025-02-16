import streamlit as st
import importlib
import os
from git_utils import git_update, git_push

# ==================== Streamlit 页面配置 ====================
st.set_page_config(page_title="恢恢数据分析 App", layout="wide")

# 初始化 session_state 中的 selected_module
if "selected_module" not in st.session_state:
    st.session_state["selected_module"] = None

# ==================== 定义各模块 ====================
# 模块分组
update_modules = ["游资查询", "董秘查询", "新闻筛选", "题材及成分股查询"]
analysis_modules = ["评分系统"]
query_modules = [
    "更新题材池", "更新涨停池", "更新超买池",
    "更新游资池", "更新调研池", "更新扣非池",
    "更新新闻快讯", "更新新闻联播"
]

# 在侧边栏创建展开器
update_expander = st.sidebar.expander("查询数据", expanded=False)
analysis_expander = st.sidebar.expander("分析数据", expanded=False)
query_expander = st.sidebar.expander("更新数据", expanded=False)

# 在“查询数据”展开器中显示按钮（对应 update_modules 分组）
for module in update_modules:
    if update_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 在“分析数据”展开器中显示按钮
for module in analysis_modules:
    if analysis_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

# 在“更新数据”展开器中显示按钮（对应 query_modules 分组）
for module in query_modules:
    if query_expander.button(module, key=module, use_container_width=True, help=f"点击运行 {module}"):
        st.session_state["selected_module"] = module

st.sidebar.markdown("---")

# ==================== 模块名称与对应文件映射 ====================
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
    "题材及成分股查询": "题材及成分股查询",
    "董秘查询": "董秘查询"
}

# ==================== 模块对应的更新类型 ====================
# replace 表示覆盖式更新；update 表示追加更新；None 表示无需执行 Git 更新
module_update_mode = {
    "更新题材池": "replace",
    "更新涨停池": "replace",
    "更新超买池": "replace",
    "更新扣非池": "replace",
    "更新新闻快讯": "update",
    "更新新闻联播": "update",
    "更新游资池": "update",
    "更新调研池": "update",
    "游资查询": None,
    "董秘查询": None,
    "新闻筛选": None,
    "题材及成分股查询": None,
    "评分系统": None
}

# ==================== 获取当前选中的模块 ====================
selected_module = st.session_state["selected_module"]

if selected_module:
    result_key = f"{module_map.get(selected_module)}_result"
    try:
        # 如果缓存中没有结果，则动态导入模块并执行
        if result_key not in st.session_state:
            module_name = module_map.get(selected_module)
            module_app = importlib.import_module(module_name)
            # 运行模块主函数，并获取返回的文件保存路径
            # 注意：各模块需自行修改 main() 函数以返回保存文件的路径（如 "date/xxx.txt"）
            file_path = module_app.main()
            
            # 根据模块名称获取对应的更新模式
            update_mode = module_update_mode.get(selected_module)
            
            # 如果 update_mode 为 None，则不执行 Git 更新操作
            if update_mode is not None:
                if file_path and os.path.exists(file_path):
                    git_update(file_path, update_mode=update_mode)
                    git_push(branch="main")
                else:
                    st.warning("模块未返回有效的文件路径，未执行 Git 更新。")
            else:
                st.info(f"{selected_module} 模块无需 Git 更新。")
        else:
            st.write("加载缓存数据...")
            st.session_state[result_key]
    except Exception as e:
        st.error(f"调用 {selected_module} 模块时出错: {e}")
else:
    # 首页默认展示内容
    st.title("欢迎使用 恢恢数据分析 App")
    st.write("这是一个功能强大的股票数据分析工具。")
    st.markdown("""
    ### 应用功能
    - **查询数据**：游资数据、机构数据、题材成分股资金流向等
    - **分析选股**：股票池层层筛选分析选股
    - **更新数据**：更新题材池、涨停池、超买池、游资池、调研池、扣非池、新闻快讯、新闻联播等数据每个交易日17点前更新完成    
    """)
    st.info("请在左侧栏中选择相应模块开始使用。")

# ==================== 示例：渲染 HTML 表格函数 ====================
def render_html_table(results):
    html_daily, html_stocks = "", ""
    if "daily_rates_df" in results:
        html_daily = results["daily_rates_df"].style.hide(axis="index").to_html()
    if "stocks_df" in results:
        html_stocks = results["stocks_df"].style.hide(axis="index").to_html()
    return html_daily, html_stocks
