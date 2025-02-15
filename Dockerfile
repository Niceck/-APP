FROM python:3.10-slim

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y build-essential libta-lib0-dev

# 设置工作目录
WORKDIR /app

# 将应用代码复制到容器
COPY . .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 运行 Streamlit 应用
CMD ["streamlit", "run", "选股_app.py"]
