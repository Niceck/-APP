import tushare as ts
import pandas as pd
import datetime as dt
from tqdm import tqdm
import os
import logging
import ast  # 用于解析字符串表示的列表
import time  # 用于控制API调用频率

# 设置日志记录
logging.basicConfig(filename='error.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 设置您的 Tushare API Token
ts.set_token('fdc47a6452a744f01bc6b486c5f53d7a04d6e321124cbd0f766bba30')  # TODO: 替换为自己的 Token
pro = ts.pro_api()


def save_selected_stocks(selected_stocks, file_path):
    """
    保存筛选后的股票代码到指定文件。
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding='utf-8') as file:
        for stock_code in selected_stocks:
            file.write(f"{stock_code}\n")
    print(f"股票列表已保存到: {file_path}")


def get_trade_calendar():
    """获取近两年的交易日历，并过滤至今日之前"""
    today = dt.datetime.today()
    today_str = today.strftime('%Y%m%d')

    # 计算两年前的日期
    two_years_ago = today.replace(year=today.year - 2)
    start_date_str = two_years_ago.strftime('%Y%m%d')

    try:
        df = pro.trade_cal(exchange='SSE', start_date=start_date_str, end_date=today_str, fields='cal_date,is_open')
        df = df[df['cal_date'] <= today_str]
        df = df.sort_values(by='cal_date')
        return df
    except Exception as e:
        logging.error(f"获取交易日历出错: {e}")
        print("获取交易日历时出错，请查看 error.log。")
        return pd.DataFrame()


def get_latest_trade_days(trade_cal_df, max_tries=10):
    """获取最近的交易日列表，最多回溯 max_tries 天"""
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return []
    return open_days[-max_tries:]


def get_component_stocks(concept_code, trade_date):
    """根据题材代码和交易日期获取成分股"""
    try:
        df_cons = pro.kpl_concept_cons(ts_code=concept_code, trade_date=trade_date)
        if df_cons.empty:
            print(f"题材代码 {concept_code} 在 {trade_date} 没有成分股。")
            return set()
        return set(df_cons['cons_code'].tolist())
    except Exception as e:
        logging.error(f"{concept_code} 在 {trade_date} 获取成分股出错: {e}")
        print(f"题材代码 {concept_code} 获取成分股时出错，请查看 error.log。")
        return set()


def load_stock_pool(file_path):
    """从单个文件加载股票池"""
    if not os.path.exists(file_path):
        print(f"文件 {file_path} 不存在，跳过。")
        return set()
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            stock_codes = {line.strip() for line in file if line.strip()}
        print(f"从本地文件 {file_path} 加载了 {len(stock_codes)} 只股票。")
        return stock_codes
    except Exception as e:
        logging.error(f"加载本地股票池文件 {file_path} 失败: {e}")
        print(f"加载本地股票池文件 {file_path} 时出错，请查看 error.log。")
        return set()


def get_union_stock_pools(file_paths):
    """
    从多个本地文件加载股票池，并计算它们的并集。
    """
    union_set = set()
    for idx, file_path in enumerate(file_paths, start=1):
        stocks = load_stock_pool(file_path)
        if stocks:
            union_set.update(stocks)
    print(f"所有输入文件的股票池并集总数: {len(union_set)}")
    return union_set


def fetch_moneyflow_data(stock_code):
    """
    获取当日净值(net_amount)、5日主力净值(net_d5_amount)、大单占比(buy_lg_amount_rate)
    单位分别是 万、万、%。
    """
    try:
        df = pro.moneyflow_ths(ts_code=stock_code, limit=1,
                               fields=["net_amount", "net_d5_amount", "buy_lg_amount_rate"])
        if df.empty:
            print(f"没有找到 {stock_code} 的资金流数据。")
            return None, None, None

        net_d5_amount = pd.to_numeric(df['net_d5_amount'].iloc[0], errors='coerce')  # 万
        net_amount = pd.to_numeric(df['net_amount'].iloc[0], errors='coerce')  # 万
        buy_lg_amount_rate = pd.to_numeric(df['buy_lg_amount_rate'].iloc[0], errors='coerce')  # %

        net_d5_amount = net_d5_amount if net_d5_amount is not None else 0.0
        net_amount = net_amount if net_amount is not None else 0.0
        buy_lg_amount_rate = buy_lg_amount_rate if buy_lg_amount_rate is not None else 0.0

        return net_d5_amount, net_amount, buy_lg_amount_rate
    except Exception as e:
        logging.error(f"{stock_code} 获取资金流数据出错: {e}")
        print(f"获取 {stock_code} 的资金流数据时出错，请查看 error.log。")
        return None, None, None


def fetch_stock_basic():
    """获取所有股票的基本信息，并返回 ts_code -> ts_name 的映射字典"""
    try:
        df_basic = pro.stock_basic(exchange='', list_status='L',
                                   fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,list_date')
        stock_basic_mapping = pd.Series(df_basic.name.values, index=df_basic.ts_code).to_dict()
        return stock_basic_mapping
    except Exception as e:
        logging.error(f"获取股票基本信息出错: {e}")
        print("获取股票基本信息时出错，请查看 error.log。")
        return {}


def get_stock_concepts(stock_code):
    """获取指定股票的 concept 标签，并进行去重处理"""
    try:
        df = pro.ths_hot(ts_code=stock_code, fields=["concept"])
        if df.empty:
            return "无"
        concepts_set = set()
        for concept_entry in df['concept'].dropna():
            try:
                parsed_concepts = ast.literal_eval(concept_entry)
                if isinstance(parsed_concepts, list):
                    concepts_set.update(parsed_concepts)
                else:
                    concepts_set.add(concept_entry)
            except (ValueError, SyntaxError):
                concepts_set.add(concept_entry)
        return "; ".join(sorted(concepts_set)) if concepts_set else "无"
    except Exception as e:
        logging.error(f"获取 {stock_code} 的 concept 失败: {e}")
        return "获取失败"


def fetch_institution_data(stock_code):
    """
    获取机构总占比(hold_ratio)
    """
    try:
        df = pro.ccass_hold(ts_code=stock_code, limit=1, fields=["hold_ratio", "ts_code", "name"])
        if df.empty:
            print(f"没有找到 {stock_code} 的机构持仓数据。")
            return 0.0
        hold_ratio = pd.to_numeric(df['hold_ratio'].iloc[0], errors='coerce')
        hold_ratio = hold_ratio if hold_ratio is not None else 0.0  # %
        return hold_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取机构持仓数据出错: {e}")
        print(f"获取 {stock_code} 的机构持仓数据时出错，请查看 error.log。")
        return 0.0


def fetch_northbound_ratio(stock_code):
    """获取北向持股比例 ratio（%）"""
    try:
        df = pro.hk_hold(ts_code=stock_code, limit=1, fields=["ratio"])
        if df.empty:
            return 0.0
        ratio = pd.to_numeric(df['ratio'].iloc[0], errors='coerce')
        return ratio if ratio is not None else 0.0
    except Exception as e:
        logging.error(f"{stock_code} 获取北向占比出错: {e}")
        print(f"获取 {stock_code} 的北向占比数据时出错，请查看 error.log。")
        return 0.0


def fetch_circ_mv_and_volume_ratio(stock_code):
    """
    获取股票的流通市值(circ_mv, 单位: 万) 和 量比(volume_ratio)
    若没数据则返回 (0.0, 0.0)
    """
    try:
        df = pro.daily_basic(ts_code=stock_code, limit=1, fields=["circ_mv", "volume_ratio"])
        if df.empty:
            return 0.0, 0.0
        circ_mv = pd.to_numeric(df['circ_mv'].iloc[0], errors='coerce')
        volume_ratio = pd.to_numeric(df['volume_ratio'].iloc[0], errors='coerce')
        circ_mv = circ_mv if circ_mv is not None else 0.0
        volume_ratio = volume_ratio if volume_ratio is not None else 0.0
        return circ_mv, volume_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取 daily_basic 出错: {e}")
        print(f"获取 {stock_code} 的 daily_basic 数据时出错，请查看 error.log。")
        return 0.0, 0.0


def fetch_hm_detail_5days(stock_code, trade_cal_df):
    """
    获取近5个交易日的游资数据（hm_detail），包含 buy_amount(万), sell_amount(万), net_amount(万)
    返回 (df_merged, yz_5d_sum, True/False)
      - df_merged：5日明细的合并DataFrame
      - yz_5d_sum：5日净买入总和
      - True/False：是否有有效数据
    """
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return None, 0.0, False

    last_day = open_days[-1]
    df_test = pro.hm_detail(ts_code=stock_code, trade_date=last_day)
    if df_test.empty:
        # 回退1日
        if len(open_days) >= 2:
            last_day = open_days[-2]
        else:
            return None, 0.0, False

    if last_day not in open_days:
        return None, 0.0, False

    idx = open_days.index(last_day)
    if idx - 4 < 0:
        five_days = open_days[0: idx + 1]
    else:
        five_days = open_days[idx - 4: idx + 1]

    frames = []
    for d in five_days:
        df_hm = pro.hm_detail(
            ts_code=stock_code,
            trade_date=d,
            fields=["trade_date", "buy_amount", "sell_amount", "net_amount", "hm_name"]
        )
        if not df_hm.empty:
            # 转为万
            for col in ["buy_amount", "sell_amount", "net_amount"]:
                df_hm[col] = pd.to_numeric(df_hm[col], errors='coerce').fillna(0) / 10000.0
            df_hm["trade_date"] = df_hm["trade_date"].astype(str)
            frames.append(df_hm)
        time.sleep(0.2)

    if not frames:
        return None, 0.0, False

    df_merged = pd.concat(frames, ignore_index=True)
    yz_5d_sum = df_merged["net_amount"].sum()
    return df_merged, yz_5d_sum, True


def get_recent_kpl_concept_cons(trade_cal_df, max_tries=10):
    """获取最近 max_tries 个交易日内可用的 kpl_concept_cons 数据（题材名称、人气值等）"""
    open_days = trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()
    if not open_days:
        return pd.DataFrame(columns=['name', 'con_code', 'hot_num', 'desc'])

    recent_days = open_days[-max_tries:]
    for trade_date in reversed(recent_days):
        try:
            df_kpl = pro.kpl_concept_cons(
                trade_date=trade_date,
                fields=['name', 'con_code', 'hot_num', 'desc']  # 修改字段名称为 'con_code'
            )
            if not df_kpl.empty:
                print(f"成功获取到 {trade_date} 的 kpl_concept_cons 数据，共 {len(df_kpl)} 条。")
                df_kpl['trade_date'] = trade_date
                return df_kpl
            else:
                print(f"{trade_date} 的kpl_concept_cons 数据为空，回退前一交易日...")
        except Exception as e:
            logging.error(f"获取 kpl_concept_cons 出错, 交易日 {trade_date}: {e}")
            print(f"获取 kpl_concept_cons 出错, 交易日 {trade_date}，请查看 error.log。")

        time.sleep(0.2)
    return pd.DataFrame(columns=['name', 'con_code', 'hot_num', 'desc'])


def aggregate_concept_info(df_kpl):
    """对 kpl_concept_cons 数据按股票代码进行聚合，得到 total_hot_num、combined_name、combined_desc"""
    if df_kpl.empty:
        return pd.DataFrame(columns=['cons_code', 'total_hot_num', 'combined_name', 'combined_desc'])

    df_kpl['hot_num'] = pd.to_numeric(df_kpl['hot_num'], errors='coerce').fillna(0)

    # 注意这里改为按 'con_code' 分组，然后重命名为 'cons_code'
    grouped = df_kpl.groupby('con_code').agg({
        'name': lambda x: ';'.join(sorted(set(x.dropna()))),
        'desc': lambda x: ';'.join(sorted(set(x.dropna()))),
        'hot_num': 'sum'
    }).reset_index()

    grouped.rename(columns={
        'name': 'combined_name',
        'desc': 'combined_desc',
        'hot_num': 'total_hot_num',
        'con_code': 'cons_code'  # 重命名字段，便于后续使用
    }, inplace=True)

    return grouped


def zscore(series: pd.Series) -> pd.Series:
    mean_val = series.mean()
    std_val = series.std()
    if std_val == 0:
        return pd.Series([0] * len(series), index=series.index)
    return (series - mean_val) / std_val


def print_5days_hm_detail(stock_code, stock_name, df_5days):
    """
    手动控制列宽，打印近5日游资明细:
      trade_date, buy_amount(万), sell_amount(万), net_amount(万)
    """
    print(f"\n===== {stock_code} ({stock_name}) 近5日 游资交易明细 =====")
    if df_5days is None or df_5days.empty:
        print("没有游资交易数据。")
        return

    col_widths = {
        'trade_date': 14,
        'buy_amount(万)': 16,
        'sell_amount(万)': 16,
        'net_amount(万)': 16,
        'hm_name': 20
    }

    header_str = (
        f"{'trade_date':<{col_widths['trade_date']}} "
        f"{'buy_amount(万)':<{col_widths['buy_amount(万)']}} "
        f"{'sell_amount(万)':<{col_widths['sell_amount(万)']}} "
        f"{'net_amount(万)':<{col_widths['net_amount(万)']}} "
        f"{'hm_name':<{col_widths['hm_name']}}"
    )
    print(header_str)
    print("-" * (sum(col_widths.values()) + 4))

    for idx, row in df_5days.iterrows():
        td_str = str(row['trade_date'])
        buy_str = f"{row['buy_amount']:.2f}"
        sell_str = f"{row['sell_amount']:.2f}"
        net_str = f"{row['net_amount']:.2f}"
        hm_name_str = str(row['hm_name'])
        print(
            f"{td_str:<{col_widths['trade_date']}} "
            f"{buy_str:<{col_widths['buy_amount(万)']}} "
            f"{sell_str:<{col_widths['sell_amount(万)']}} "
            f"{net_str:<{col_widths['net_amount(万)']}} "
            f"{hm_name_str:<{col_widths['hm_name']}}"
        )


def fetch_margin_6d_ratio(stock_code, circ_mv, trade_cal_df):
    """
    计算近5日的融资融券净流入占比(%)。

    净流入 = (当前融资余额 − 上期融资余额) − (当前融券余额 − 上期融券余额)
    近5日的净流入总和 / 流通市值
    """
    try:
        df = pro.margin_detail(ts_code=stock_code, limit=6, fields=["rzye", "rqye", "trade_date"])
        if df.empty or len(df) < 6:
            return 0.0

        df['rzye'] = pd.to_numeric(df['rzye'], errors='coerce').fillna(0.0) / 10000.0
        df['rqye'] = pd.to_numeric(df['rqye'], errors='coerce').fillna(0.0) / 10000.0

        # 升序排序（最早的在前）
        df = df.sort_values(by='trade_date').reset_index(drop=True)

        df['net_inflow'] = (df['rzye'].diff()) - (df['rqye'].diff())
        net_inflow_5d = df['net_inflow'].iloc[1:6].sum()

        if circ_mv == 0:
            return 0.0

        margin_ratio = (net_inflow_5d / circ_mv) * 100.0
        return margin_ratio
    except Exception as e:
        logging.error(f"{stock_code} 获取近6日融资融券数据出错: {e}")
        print(f"获取 {stock_code} 的融资融券数据时出错，请查看 error.log。")
        return 0.0


def main():
    # 1) 获取股票基本信息
    stock_basic_mapping = fetch_stock_basic()
    if not stock_basic_mapping:
        print("无法获取股票基本信息，程序终止。")
        return

    # 2) 用户输入文件路径（可为空）
    print(
        "\n/Users/apple/Desktop/成分股.txt /Users/apple/Desktop/涨停板.txt /Users/apple/Desktop/RSI选股.txt /Users/apple/Desktop/机构调研.txt /Users/apple/Desktop/游资.txt")
    extra_pools_input = input("请输入股票池文件路径（多个文件用空格分隔，可直接按Enter跳过）：").strip()

    selected_stocks = set()
    if extra_pools_input:
        extra_file_paths = extra_pools_input.split()
        print("\n加载股票池文件并计算并集...")
        selected_stocks = get_union_stock_pools(extra_file_paths)
    else:
        print("未输入任何股票池文件，跳过文件股票池合并。")

    # 3) 题材代码（可选），如果用户也不输入题材代码且前面也没任何股票则结束。
    concept_date = None
    concept_codes_input = input("\n请输入题材代码（多个代码用空格分隔，可直接按Enter跳过）：").strip()
    if not concept_codes_input and not selected_stocks:
        print("既没有输入文件，也没有输入题材代码，程序结束。")
        return

    # 如果用户输入了题材代码，则获取成分股并合并/交集
    if concept_codes_input:
        concept_codes = [code.strip() for code in concept_codes_input.split() if code.strip()]
        if concept_codes:
            trade_cal_df = get_trade_calendar()
            if trade_cal_df.empty:
                print("交易日历获取失败，程序终止。")
                return

            recent_trade_days = get_latest_trade_days(trade_cal_df, max_tries=10)
            if not recent_trade_days:
                print("未找到有效的交易日，程序终止。")
                return

            all_concept_stocks = set()
            valid_concept_codes = []
            for trade_date in reversed(recent_trade_days):
                temp_concept_stocks = set()
                temp_valid_concept_codes = []
                for concept_code in concept_codes:
                    stocks = get_component_stocks(concept_code, trade_date)
                    if stocks:
                        temp_concept_stocks.update(stocks)
                        temp_valid_concept_codes.append(concept_code)

                if temp_concept_stocks:
                    all_concept_stocks = temp_concept_stocks
                    valid_concept_codes = temp_valid_concept_codes
                    concept_date = trade_date
                    print(f"成功获取到 {concept_date} 的成分股数据。")
                    break
                else:
                    print(f"{trade_date} 的成分股数据为空，尝试回退到上一个交易日。")
                time.sleep(0.1)

            if all_concept_stocks:
                # 可根据需求决定合并方式：并集 or 交集
                # 这里以并集为示例，如果想要求“同时在文件池和题材池”里，则用 & 交集
                if selected_stocks:
                    selected_stocks = selected_stocks & all_concept_stocks
                else:
                    selected_stocks = all_concept_stocks

                print(f"结合题材代码后，股票池总数: {len(selected_stocks)}")
        else:
            print("未检测到有效的题材代码输入。")
    else:
        print("未输入题材代码，仅使用文件池股票进行后续处理。")

    # 若仍为空，则退出
    if not selected_stocks:
        print("最终股票池为空，程序终止。")
        return

    # ========== 第一步：先通过游资数据进行筛选 ==========
    print("\n第一步：根据游资数据筛选股票...")
    trade_cal_df = get_trade_calendar()
    if trade_cal_df.empty:
        print("交易日历获取失败，程序终止。")
        return

    filtered_stocks = []
    hm_detail_map = {}  # 在此阶段就把游资明细保存起来，以免二次获取
    for stock_code in tqdm(selected_stocks, desc="游资筛选"):
        df_5days, yz_5d_sum, has_data_5d = fetch_hm_detail_5days(stock_code, trade_cal_df)
        # 如果近5日游资数据不为空 (has_data_5d=True)，则保留；否则剔除
        if has_data_5d:
            filtered_stocks.append(stock_code)
            # 把游资明细暂存起来，后面直接用
            hm_detail_map[stock_code] = (df_5days, yz_5d_sum)
        else:
            pass  # 剔除该股票

    if not filtered_stocks:
        print("没有任何股票通过游资数据筛选，程序结束。")
        return

    # ========== 第二步：对筛选后的股票进行其他数据获取及评分 ==========
    final_data = []
    print("\n第二步：对通过游资筛选的股票，获取资金流、北向、流通市值、融资融券等数据，并进行AI评分...")

    # 获取 kpl_concept_cons 数据
    df_kpl_final = get_recent_kpl_concept_cons(trade_cal_df, max_tries=10)
    if df_kpl_final.empty:
        print("在最近的交易日范围内，kpl_concept_cons 数据均为空。")
        df_kpl_agg = pd.DataFrame()
    else:
        df_kpl_agg = aggregate_concept_info(df_kpl_final)

    concept_info_dict = {}
    if not df_kpl_agg.empty:
        for idx, row in df_kpl_agg.iterrows():
            code = row['cons_code']
            concept_info_dict[code] = {
                'hot_num': row['total_hot_num'],
                'concept_names': row['combined_name'],
                'desc': row['combined_desc']
            }

    # 收集评分所需数据
    for stock_code in tqdm(filtered_stocks, desc="数据采集"):
        stock_name = stock_basic_mapping.get(stock_code, '未知')

        # (1) 直接拿前面保存的游资明细
        df_5days, yz_5d_sum = hm_detail_map[stock_code]

        # (2) 流通市值 & 量比
        circ_mv, volume_ratio = fetch_circ_mv_and_volume_ratio(stock_code)
        if circ_mv <= 0:
            continue

        # (3) 资金流
        net_d5_amount, net_amount, buy_lg_amount_rate = fetch_moneyflow_data(stock_code)
        net_d5_amount = net_d5_amount if net_d5_amount else 0.0
        net_amount = net_amount if net_amount else 0.0
        buy_lg_amount_rate = buy_lg_amount_rate if buy_lg_amount_rate else 0.0

        ratio_today = (net_amount / circ_mv) * 100
        ratio_5day = (net_d5_amount / circ_mv) * 100

        # (4) 机构 & 北向
        hold_ratio = fetch_institution_data(stock_code)
        northbound_ratio = fetch_northbound_ratio(stock_code)

        # (5) 游资净额占比
        yz_5d_ratio = (yz_5d_sum / circ_mv) * 100

        # (6) 人气值、题材名称、描述
        hot_num_val = 0
        concept_name_val = "无"
        desc_val = "无"
        if stock_code in concept_info_dict:
            hot_num_val = concept_info_dict[stock_code]['hot_num']
            concept_name_val = concept_info_dict[stock_code]['concept_names']
            desc_val = concept_info_dict[stock_code]['desc']

        # (7) 概念标签
        concepts = get_stock_concepts(stock_code)
        if len(concepts) > 30:
            concepts = concepts[:27] + '...'

        # (8) 近6日融资融券净流入占比
        margin_ratio = fetch_margin_6d_ratio(stock_code, circ_mv, trade_cal_df)

        # 整理到 final_data
        final_data.append({
            '股票代码': stock_code,
            '股票名称': stock_name,
            '当日净值占比(%)': float(ratio_today),
            '5日主力净值占比(%)': float(ratio_5day),
            '游资净额占比(%)': float(yz_5d_ratio),
            '大单占比(%)': float(buy_lg_amount_rate),
            '机构占比(%)': float(hold_ratio),
            '北向占比(%)': float(northbound_ratio),
            '量比': float(volume_ratio),
            '融资融券净流入占比(%)': float(margin_ratio),
            '人气值': float(hot_num_val),
            '题材名称': concept_name_val,
            '概念标签': concepts,
            '描述': desc_val
        })

    if not final_data:
        print("游资筛选后的股票，在后续数据采集时均不符合要求（可能流通市值=0等），程序结束。")
        return

    # ========== AI评分 ==========
    final_df = pd.DataFrame(final_data)
    numeric_cols = [
        '当日净值占比(%)', '5日主力净值占比(%)', '游资净额占比(%)',
        '大单占比(%)', '机构占比(%)', '北向占比(%)', '量比', '人气值',
        '融资融券净流入占比(%)'
    ]
    for col in numeric_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)

    # 计算 z-score
    final_df['z_ratio_today'] = zscore(final_df['当日净值占比(%)'])
    final_df['z_ratio_5day'] = zscore(final_df['5日主力净值占比(%)'])
    final_df['z_yz_5d_ratio'] = zscore(final_df['游资净额占比(%)'])
    final_df['z_dd_ratio'] = zscore(final_df['大单占比(%)'])
    final_df['z_jg_ratio'] = zscore(final_df['机构占比(%)'])
    final_df['z_bx_ratio'] = zscore(final_df['北向占比(%)'])
    final_df['z_vol_ratio'] = zscore(final_df['量比'])
    final_df['z_hot_num'] = zscore(final_df['人气值'])
    final_df['z_margin_ratio'] = zscore(final_df['融资融券净流入占比(%)'])

    # AI评分
    final_df['AI评分'] = (
            1.0 * final_df['z_ratio_today'] +
            1.2 * final_df['z_ratio_5day'] +
            1.5 * final_df['z_yz_5d_ratio'] +
            1.0 * final_df['z_dd_ratio'] +
            0.8 * final_df['z_jg_ratio'] +
            0.8 * final_df['z_bx_ratio'] +
            0.6 * final_df['z_vol_ratio'] +
            0.5 * final_df['z_hot_num'] +
            1.0 * final_df['z_margin_ratio']
    )

    # 调整列顺序，将“AI评分”插在“人气值”后面
    cols = list(final_df.columns)
    idx_renqi = cols.index('人气值')
    new_position = idx_renqi + 1
    cols.remove('AI评分')
    cols.insert(new_position, 'AI评分')
    final_df = final_df[cols]

    # 删除临时 z_score 列
    drop_cols = [
        'z_ratio_today', 'z_ratio_5day', 'z_yz_5d_ratio', 'z_dd_ratio',
        'z_jg_ratio', 'z_bx_ratio', 'z_vol_ratio', 'z_hot_num', 'z_margin_ratio'
    ]
    for dc in drop_cols:
        if dc in final_df.columns:
            final_df.drop(columns=dc, inplace=True)

    # 排序
    final_df = final_df.sort_values(by='AI评分', ascending=False).reset_index(drop=True)
    data_date = concept_date if concept_date else dt.datetime.today().strftime('%Y%m%d')
    print(f"\n数据日期: {data_date}\n")

    # ========== 打印评分统计表 ==========
    header_widths = {
        '股票代码': 8,
        '股票名称': 8,
        '当日净值占比(%)': 10,
        '5日主力净值占比(%)': 12,
        '游资净额占比(%)': 10,
        '大单占比(%)': 8,
        '机构占比(%)': 8,
        '北向占比(%)': 8,
        '量比': 5,
        '融资融券净流入占比(%)': 12,
        '人气值': 5,
        'AI评分': 8,
        '题材名称': 30,
        '概念标签': 30,
        '描述': 25
    }

    data_widths = {
        '股票代码': 12,
        '股票名称': 12,
        '当日净值占比(%)': 14,
        '5日主力净值占比(%)': 14,
        '游资净额占比(%)': 14,
        '大单占比(%)': 10,
        '机构占比(%)': 10,
        '北向占比(%)': 8,
        '量比': 14,
        '融资融券净流入占比(%)': 10,
        '人气值': 8,
        'AI评分': 6,
        '题材名称': 30,
        '概念标签': 30,
        '描述': 160
    }

    header_list = list(final_df.columns)
    header_str = ""
    for col in header_list:
        hw = header_widths.get(col, 10)
        header_str += f"{col:<{hw}} "
    print(header_str)
    print("-" * len(header_str))

    def truncate_str(val, limit):
        s = str(val)
        return s[:(limit - 3)] + "..." if len(s) > limit else s

    for idx, row in final_df.iterrows():
        row_str = ""
        for col in header_list:
            val = row[col]
            dw = data_widths.get(col, 10)
            if col == '人气值':
                val_str = str(int(val))
                row_str += f"{truncate_str(val_str, dw):<{dw}} "
            elif isinstance(val, float):
                val_str = f"{val:.2f}"
                row_str += f"{truncate_str(val_str, dw):<{dw}} "
            else:
                val_str = truncate_str(str(val), dw)
                row_str += f"{val_str:<{dw}} "
        print(row_str)

    # ========== 打印游资明细 ==========
    for idx, row in final_df.iterrows():
        stock_code = row['股票代码']
        stock_name = row['股票名称']
        df_5d, yz_5d_sum = hm_detail_map.get(stock_code, (None, 0.0))
        print_5days_hm_detail(stock_code, stock_name, df_5d)

    # 保存结果
    desktop_path = os.path.expanduser("~/Desktop")
    final_save_path = os.path.join(desktop_path, "评分股票.txt")
    save_selected_stocks(final_df['股票代码'].tolist(), final_save_path)
    print(f"\n强势题材股票总数: {len(final_df)}")


if __name__ == "__main__":
    main()
