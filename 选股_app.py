import os
import sys
import requests

if not os.path.isdir("/tmp/ta-lib"):
    # 下载 ta-lib 源码
    with open("/tmp/ta-lib-0.4.0-src.tar.gz", "wb") as file:
        response = requests.get(
            "http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz"
        )
        file.write(response.content)
    # 解压源码
    os.system("tar -zxvf /tmp/ta-lib-0.4.0-src.tar.gz -C /tmp")
    # 编译并安装
    os.chdir("/tmp/ta-lib")
    os.system("./configure --prefix=/home/appuser")
    os.system("make")
    os.system("make install")
    # 安装 Python 包
    os.system(
        'pip3 install --global-option=build_ext --global-option="-L/home/appuser/lib/" --global-option="-I/home/appuser/include/" ta-lib'
    )
