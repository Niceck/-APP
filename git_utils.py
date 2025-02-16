import subprocess
import datetime
import streamlit as st

def git_update(file_path, update_mode="replace", branch="main"):
    """
    根据更新模式执行 Git 更新：
    - update_mode="replace"：表示文件被覆盖替换
    - update_mode="update"：表示文件被追加更新
    """
    try:
        subprocess.run(["git", "add", file_path], check=True)
        if update_mode == "replace":
            commit_message = f"Replace {file_path} on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            commit_message = f"Update {file_path} on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        st.write(f"{file_path} 提交成功: {commit_message}")
    except subprocess.CalledProcessError as e:
        st.error(f"提交 {file_path} 时出错：{e}")

def git_push(branch="main"):
    try:
        subprocess.run(["git", "push", "origin", branch], check=True)
        st.write("所有改动已成功推送到 Git 仓库！")
    except subprocess.CalledProcessError as e:
        st.error(f"推送 Git 时出错：{e}")
