import tushare as ts
import time
from tqdm import tqdm

tushare_token = st.secrets["api_keys"]["tushare_token"]

# 设置 Tushare API Token
ts.set_token(tushare_token)
pro = ts.pro_api()


# 指定保存路径，保存在现有的 date 文件夹中（如果文件已存在，则覆盖）
file_path = './date/股东.txt'
qualified_stocks = []  # 用于存放符合条件的股票代码

# 获取所有普通股票代码（list_status='L' 表示上市股票）
stock_list = pro.stock_basic(list_status='L', fields='ts_code')
total_stocks = len(stock_list)

# 限制每分钟最多API调用500次
call_count = 0
start_time = time.time()

with tqdm(total=total_stocks, desc="筛选股票进度", ncols=100) as pbar:
    for _, row in stock_list.iterrows():
        ts_code = row['ts_code']
        try:
            # 调用 top10_holders 接口时只请求 holder_name 字段（不再请求 ts_code）
            df = pro.top10_holders(ts_code=ts_code, limit=10, fields=["holder_name"])
            if not df.empty:
                # 通过strip()去掉空格并且使用lower()统一大小写（确保准确匹配）
                df['holder_name_clean'] = df['holder_name'].apply(lambda x: str(x).strip().lower())

                # 判断股东名称中是否包含“香港中央结算有限公司”（忽略大小写和空格）
                if any("香港中央结算有限公司" in name for name in df['holder_name_clean'].values):
                    # 如果符合条件，加入到合格的股票列表中（避免重复）
                    if ts_code not in qualified_stocks:
                        qualified_stocks.append(ts_code)

        except Exception as e:
            print(f"处理股票 {ts_code} 时发生错误: {e}")

        # 控制API调用频率，每500次调用后，若60秒内调用未达到，则等待剩余时间
        call_count += 1
        if call_count >= 390:
            elapsed = time.time() - start_time
            if elapsed < 60:
                time.sleep(60 - elapsed)
            call_count = 0
            start_time = time.time()

        pbar.update(1)

# 将符合条件的股票代码写入到文件（覆盖已存在的文件）
with open(file_path, 'w', encoding='utf-8') as f:
    for stock in qualified_stocks:
        f.write(stock + "\n")

# 输出筛选结果
print(f"筛选获得的股票总数量: {len(qualified_stocks)}")
