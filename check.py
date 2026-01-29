import akshare as ak
import pandas as pd
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import time

warnings.filterwarnings('ignore')

CONFIG = {
    "pulse_threshold": 0.015,    # 正股脉冲阈值：1.5%，超过此涨幅才检测转债
    "stop_profit": 0.008,        # 止盈阈值：0.8%
    "stop_loss": -0.005,         # 止损阈值：-0.5%
    "check_interval": 30,         # 检测间隔：30秒
    "max_workers": 10,           # 最大并发线程数：10个线程同时检测
    "min_bond_change": 0.005,    # 转债最小涨幅：0.5%，低于此涨幅认为是滞后
    "data_file": "1.json",        # 数据文件：包含转债-正股映射关系
    "log_file": "monitor.log",    # 日志文件：记录监控过程
    "retry_times": 3,             # 重试次数：网络请求失败时重试3次
    "retry_delay": 2,             # 重试延迟：每次重试间隔2秒
}

class BondStockMonitor:
    """
    可转债-正股联动监控类
    
    功能：
    1. 加载转债-正股映射数据
    2. 实时监控转债和正股的价格变化
    3. 发现正股脉冲但转债滞后的套利机会
    4. 管理持仓的止盈止损
    """
    
    def __init__(self):
        """
        初始化监控器
        """
        self.bond_stock_map = {}      # 转债-正股映射字典：{bond_code: bond_info}
        self.hold_list = {}          # 持仓列表：{bond_code: entry_price}
        self.log_count = 0           # 日志计数器
        self.all_bonds_data = None    # 缓存所有转债对比数据（DataFrame）
        self.stock_price_cache = {}    # 正股价格缓存：{stock_code: previous_price}

    def log(self, message):
        """
        记录日志到控制台和文件
        
        参数：
            message: 日志消息内容
        
        功能：
            1. 在控制台打印带时间戳的日志
            2. 将日志写入monitor.log文件
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        print(log_message)
        
        try:
            with open(CONFIG["log_file"], 'a', encoding='utf-8') as f:
                f.write(log_message + '\n')
        except Exception as e:
            print(f"写入日志失败: {e}")

    def load_data_from_json(self, filename):
        """
        从JSON文件加载转债-正股映射数据
        
        参数：
            filename: JSON文件路径
        
        返回：
            data: 转债-正股映射列表，失败返回空列表
        
        功能：
            读取1.json文件，包含转债代码、名称、正股代码、名称等信息
        """
        try:
            self.log(f"从 {filename} 加载可转债数据...")
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.log(f"成功加载 {len(data)} 个可转债-正股对")
            return data
        except FileNotFoundError:
            self.log(f"文件 {filename} 不存在，请先运行 get.py 生成数据")
            return []
        except Exception as e:
            self.log(f"加载文件失败: {e}")
            return []

    def build_mapping(self):
        """
        构建转债-正股映射字典
        
        返回：
            mapping: 转债-正股映射字典 {bond_code: bond_info}
        
        功能：
            将JSON数据转换为字典格式，方便快速查找
        """
        data = self.load_data_from_json(CONFIG["data_file"])
        if not data:
            return {}
        
        mapping = {}
        for item in data:
            bond_code = item.get('bond_code', '')
            if bond_code:
                mapping[bond_code] = {
                    'bond_name': item.get('bond_name', ''),      # 转债名称
                    'stock_code': item.get('stock_code', ''),      # 正股代码
                    'stock_name': item.get('stock_name', ''),      # 正股名称
                    'bond_price': item.get('bond_price', 0),      # 转债价格
                    'premium_rate': item.get('premium_rate', 0),   # 溢价率
                    'bond_amount': item.get('amount', 0),        # 成交额（万元）
                }
        
        return mapping

    def get_all_bonds_comparison(self):
        """
        一次性获取所有转债和正股的对比数据（使用稳定接口）
        
        返回：
            df: 包含所有转债对比数据的DataFrame，失败返回None
        
        功能：
            结合两个稳定接口获取数据：
            1. 使用ak.bond_zh_cov获取转债基本信息和正股对应关系
            2. 使用ak.bond_zh_hs_cov_spot获取实时行情数据
            3. 通过转债代码合并两个数据集
            
        数据字段：
            - 债券代码、债券简称、最新价、涨跌幅、涨跌额、成交量、成交额
            - 正股代码、正股简称、正股价、转股价、转股价值、债现价、转股溢价率
        """
        try:
            self.log(f"正在获取所有转债-正股对比数据...")
            
            # 1. 获取转债基本信息和正股对应关系
            self.log(f"  [1/2] 获取转债基本信息...")
            df_basic = ak.bond_zh_cov()
            
            if df_basic is None or len(df_basic) == 0:
                self.log(f"  获取转债基本信息失败")
                return None
            
            self.log(f"  ✅ 成功获取 {len(df_basic)} 条基本信息")
            
            # 选择需要的列
            df_basic = df_basic[['债券代码', '债券简称', '正股代码', '正股简称', '正股价', '转股价', '转股价值', '债现价', '转股溢价率']]
            
            # 2. 获取实时行情数据
            self.log(f"  [2/2] 获取实时行情数据...")
            df_spot = ak.bond_zh_hs_cov_spot()
            
            if df_spot is None or len(df_spot) == 0:
                self.log(f"  获取实时行情数据失败")
                return None
            
            self.log(f"  ✅ 成功获取 {len(df_spot)} 条实时数据")
            
            # 选择需要的列
            df_spot = df_spot[['code', 'trade', 'pricechange', 'changepercent', 'volume', 'amount']]
            
            # 3. 合并数据
            self.log(f"  合并数据...")
            
            # 统一转债代码格式（去掉前缀）
            df_basic['债券代码_纯数字'] = df_basic['债券代码'].astype(str)
            df_spot['code'] = df_spot['code'].astype(str)
            
            # 合并数据
            df_merged = pd.merge(df_basic, df_spot, left_on='债券代码_纯数字', right_on='code', how='left')
            
            # 删除临时列
            df_merged = df_merged.drop('债券代码_纯数字', axis=1)
            
            # 重命名列以匹配原有格式
            df_merged = df_merged.rename(columns={
                '债券代码': '转债代码',
                '债券简称': '转债名称',
                'trade': '转债最新价',
                'pricechange': '转债涨跌额',
                'changepercent': '转债涨跌幅',
                'volume': '成交量',
                'amount': '成交额',
                '正股代码': '正股代码',
                '正股简称': '正股名称',
                '正股价': '正股最新价',
            })
            
            # 转换数据类型
            if '转债涨跌幅' in df_merged.columns:
                df_merged['转债涨跌幅'] = pd.to_numeric(df_merged['转债涨跌幅'], errors='coerce') / 100
            
            # 过滤掉没有实时数据的转债
            df_merged = df_merged[df_merged['转债最新价'].notna()]
            
            self.log(f"  ✅ 合并完成，剩余 {len(df_merged)} 条有效数据")
            
            return df_merged
            
        except Exception as e:
            self.log(f"获取转债对比数据异常: {e}")
            return None

    def get_stock_min_data(self, symbol):
        """
        获取单只股票的1分钟K线数据（带重试机制）
        
        参数：
            symbol: 股票代码（如"688059"）
        
        返回：
            df: 包含最后2条1分钟数据的DataFrame，失败返回None
        
        功能：
            1. 调用ak.stock_zh_a_hist_min_em获取1分钟K线数据
            2. 计算最新价格和涨幅
            3. 失败时自动重试3次，每次间隔2秒
            4. 识别网络连接错误，继续重试
            5. 其他错误直接返回None
        """
        for attempt in range(CONFIG["retry_times"]):
            try:
                self.log(f"  获取正股 {symbol} 的1分钟数据 (第{attempt+1}次尝试)...")
                df = ak.stock_zh_a_hist_min_em(symbol=symbol, period='1', adjust="")
                
                if df is not None and len(df) >= 2:
                    latest = df.tail(2)  # 获取最后2条数据
                    price = latest['收盘'].values[-1]      # 最新收盘价
                    prev_price = latest['收盘'].values[-2]  # 前一分钟收盘价
                    change = (price - prev_price) / prev_price  # 计算涨幅
                    
                    self.log(f"    正股 {symbol}: 价格={price:.2f}, 前期={prev_price:.2f}, 涨幅={change:.2%}")
                    return df.tail(2)
                
                if attempt < CONFIG["retry_times"] - 1:
                    self.log(f"  获取失败，{CONFIG['retry_delay']}秒后重试...")
                    time.sleep(CONFIG["retry_delay"])
                
            except Exception as e:
                error_msg = str(e)
                # 识别网络连接错误，继续重试
                if "Connection aborted" in error_msg or "RemoteDisconnected" in error_msg:
                    self.log(f"  网络连接错误: {e}")
                    if attempt < CONFIG["retry_times"] - 1:
                        self.log(f"  {CONFIG['retry_delay']}秒后重试...")
                        time.sleep(CONFIG["retry_delay"])
                    continue
                else:
                    # 其他错误直接返回
                    self.log(f"  获取异常: {e}")
                    return None
        
        self.log(f"  正股 {symbol}: 重试{CONFIG['retry_times']}次后仍失败")
        return None

    def check_single_pair(self, bond_code, bond_info):
        """
        检测单个转债-正股对的套利机会
        
        参数：
            bond_code: 转债代码
            bond_info: 转债信息字典（从1.json加载）
        
        返回：
            result: 套利机会字典，无机会返回None
        
        检测逻辑：
            1. 从对比数据中获取转债和正股的实时信息
            2. 如果正股涨幅≥1.5%，获取1分钟数据验证
            3. 如果转债涨幅<0.5%，发现套利机会
        """
        try:
            self.log(f"\n--- 开始检测: {bond_info['bond_name']}({bond_code}) - {bond_info['stock_name']}({bond_info['stock_code']}) ---")
            
            # 1. 从对比数据中获取转债和正股的实时信息
            if self.all_bonds_data is None:
                self.log(f"  跳过: 未获取转债对比数据")
                return None
            
            # 查找转债在对比数据中的信息
            bond_row = self.all_bonds_data[self.all_bonds_data['转债代码'] == bond_code]
            if bond_row.empty:
                self.log(f"  跳过: 未找到转债 {bond_code} 的对比数据")
                return None
            
            bond_info_data = bond_row.iloc[0]
            
            # 获取转债价格和涨幅（确保数据类型正确）
            bond_price = pd.to_numeric(bond_info_data['转债最新价'], errors='coerce')
            bond_change = pd.to_numeric(bond_info_data['转债涨跌幅'], errors='coerce')  # 已经转换为小数
            
            self.log(f"  【比对步骤1】转债信息")
            self.log(f"    转债代码: {bond_code}")
            self.log(f"    转债名称: {bond_info_data['转债名称']}")
            self.log(f"    转债价格: {bond_price:.2f}")
            self.log(f"    转债涨幅: {bond_change:.2%}")
            self.log(f"    溢价率: {bond_info_data.get('转股溢价率', 0):.2f}%")
            
            # 获取正股价格（确保数据类型正确）
            stock_price_from_comparison = pd.to_numeric(bond_info_data['正股最新价'], errors='coerce')
            
            # 计算正股涨幅（使用缓存机制）
            stock_code = bond_info['stock_code']
            if stock_code in self.stock_price_cache:
                previous_price = self.stock_price_cache[stock_code]
                stock_change = (stock_price_from_comparison - previous_price) / previous_price
                self.log(f"  【比对步骤2】正股信息（使用缓存计算涨幅）")
            else:
                # 第一次获取，无法计算涨幅
                stock_change = 0
                self.log(f"  【比对步骤2】正股信息（首次获取，缓存价格）")
            
            # 更新缓存
            self.stock_price_cache[stock_code] = stock_price_from_comparison
            
            self.log(f"    正股代码: {stock_code}")
            self.log(f"    正股名称: {bond_info_data['正股名称']}")
            self.log(f"    正股价格: {stock_price_from_comparison:.2f}")
            if stock_code in self.stock_price_cache:
                self.log(f"    正股涨幅: {stock_change:.2%}")
            
            # 判断正股涨幅是否达标
            self.log(f"  【比对步骤3】判断正股涨幅是否达标")
            self.log(f"    正股涨幅: {stock_change:.2%} vs 阈值: {CONFIG['pulse_threshold']:.2%}")
            
            if stock_change < CONFIG["pulse_threshold"]:
                self.log(f"    ❌ 正股涨幅未达标，跳过")
                return None
            
            self.log(f"    ✅ 正股涨幅验证通过")
            
            # 3. 判断转债涨幅是否滞后
            self.log(f"  【比对步骤4】判断转债涨幅是否滞后")
            self.log(f"    转债涨幅: {bond_change:.2%} vs 阈值: {CONFIG['min_bond_change']:.2%}")
            
            if bond_change < CONFIG["min_bond_change"]:
                result = {
                    'bond_code': bond_code,
                    'bond_name': bond_info['bond_name'],
                    'stock_code': bond_info['stock_code'],
                    'stock_name': bond_info['stock_name'],
                    'stock_change': stock_change,
                    'bond_change': bond_change,
                    'bond_price': bond_price,
                    'premium_rate': bond_info_data.get('转股溢价率', 0),
                    'bond_amount': bond_info.get('bond_amount', 0),
                }
                
                # 明显的套利机会提示
                self.log(f"\n{'=' * 100}")
                self.log(f"🎯🎯🎯 发现套利机会！🎯🎯🎯")
                self.log(f"{'=' * 100}")
                self.log(f"📊 【转债信息】")
                self.log(f"    转债代码: {bond_code}")
                self.log(f"    转债名称: {bond_info['bond_name']}")
                self.log(f"    转债价格: {bond_price:.2f} 元")
                self.log(f"    转债涨幅: {bond_change:.2%} (滞后)")
                self.log(f"    溢价率: {bond_info_data.get('转股溢价率', 0):.2f}%")
                self.log(f"    成交额: {bond_info.get('bond_amount', 0):.0f} 万元")
                self.log(f"\n📈 【正股信息】")
                self.log(f"    正股代码: {bond_info['stock_code']}")
                self.log(f"    正股名称: {bond_info['stock_name']}")
                self.log(f"    正股涨幅: {stock_change:.2%} (脉冲)")
                self.log(f"\n💰 【套利空间】")
                self.log(f"    正股脉冲幅度: {stock_change:.2%}")
                self.log(f"    转债滞后幅度: {bond_change:.2%}")
                self.log(f"    套利空间: {stock_change - bond_change:.2%}")
                self.log(f"{'=' * 100}\n")
                
                return result
            
            self.log(f"    ❌ 转债涨幅已达标，无套利机会")
            return None
        except Exception as e:
            self.log(f"  检测异常: {e}")
            return None

    def scan_market(self):
        """
        全市场扫描套利机会
        
        返回：
            opportunities: 套利机会列表
        
        功能：
            1. 调用bond_cov_comparison获取所有转债对比数据
            2. 并发检测每个转债-正股对
            3. 汇总所有套利机会
        """
        # 1. 先获取所有转债的对比数据
        self.all_bonds_data = self.get_all_bonds_comparison()
        if self.all_bonds_data is None:
            self.log(f"无法获取转债对比数据，跳过本次扫描")
            return []
        
        self.log(f"\n{'=' * 100}")
        self.log(f"开始全市场扫描...")
        self.log(f"监控标的数量: {len(self.bond_stock_map)}")
        
        opportunities = []
        
        # 2. 并发检测每个转债-正股对
        # 使用线程池提高效率，最多10个线程同时检测
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            future_to_bond = {
                executor.submit(self.check_single_pair, bond_code, bond_info): bond_code
                for bond_code, bond_info in self.bond_stock_map.items()
            }
            
            # 等待所有检测任务完成
            for future in as_completed(future_to_bond):
                result = future.result()
                if result:
                    opportunities.append(result)
        
        # 3. 输出套利机会
        if opportunities:
            self.log(f"\n🎯 发现 {len(opportunities)} 个套利机会:")
            self.log("=" * 100)
            for opp in opportunities:
                bond_amount_wan = opp['bond_amount']
                message = f"🚀 {opp['stock_name']}({opp['stock_code']}) 涨幅: {opp['stock_change']:.2%} | 转债 {opp['bond_name']}({opp['bond_code']}) 涨幅: {opp['bond_change']:.2%} | 转债价格: {opp['bond_price']:.2f} | 溢价率: {opp['premium_rate']:.2%} | 转债成交额: {bond_amount_wan:.0f}万元"
                print(message)
                self.log(message)
            self.log("=" * 100)
        else:
            self.log(f"未发现套利机会")

    def check_holdings(self):
        """
        检查持仓的止盈止损
        
        功能：
            1. 遍历所有持仓
            2. 获取转债当前价格
            3. 计算收益率
            4. 触发止盈或止损时平仓
        """
        if not self.hold_list:
            return
        
        self.log(f"\n检查持仓...")
        
        for bond_code, entry_price in list(self.hold_list.items()):
            bond_data = self.get_bond_min_data(bond_code)
            if bond_data is None:
                continue
            
            current_price = bond_data['收盘'].values[-1]
            pnl = (current_price - entry_price) / entry_price
            
            # 触发止盈
            if pnl >= CONFIG["stop_profit"]:
                message = f"✅ 止盈: 转债 {bond_code} | 收益率: {pnl:.2%} | 价格: {current_price:.2f}"
                print(message)
                self.log(message)
                del self.hold_list[bond_code]
            # 触发止损
            elif pnl <= CONFIG["stop_loss"]:
                message = f"❌ 止损: 转债 {bond_code} | 收益率: {pnl:.2%} | 价格: {current_price:.2f}"
                print(message)
                self.log(message)
                del self.hold_list[bond_code]

    def run(self):
        """
        主运行函数
        
        功能：
            1. 打印启动信息
            2. 加载转债-正股映射
            3. 进入监控循环
            4. 交易时间内扫描市场
            5. 非交易时间等待
            6. 支持Ctrl+C中断
        """
        print("=" * 100)
        print("=== 全市场可转债联动监控系统 ===")
        print("=" * 100)
        print(f"启动时间: {datetime.now()}")
        print(f"交易时间: 9:30-11:30, 13:00-15:00")
        print(f"配置参数: {CONFIG}")
        print("=" * 100)
        
        self.log("=" * 100)
        self.log("=== 全市场可转债联动监控系统启动 ===")
        self.log(f"启动时间: {datetime.now()}")
        self.log(f"配置参数: {CONFIG}")
        self.log("=" * 100)
        
        # 加载转债-正股映射
        self.bond_stock_map = self.build_mapping()
        
        if not self.bond_stock_map:
            self.log("无法加载转债数据，请先运行 get.py 生成数据文件")
            print("无法加载转债数据，请先运行 get.py 生成数据文件")
            return
        
        self.log(f"\n开始监控，按 Ctrl+C 停止程序")
        print(f"\n开始监控，按 Ctrl+C 停止程序")
        print(f"日志文件: {CONFIG['log_file']}\n")
        
        try:
            while True:
                now = datetime.now()
                
                # 判断是否为交易时间
                # 上午：9:30-11:30
                # 下午：13:00-15:00
                is_trading_time = (now.hour == 9 and now.minute >= 30) or (now.hour == 10) or \
                                  (now.hour == 11 and now.minute <= 30) or (now.hour >= 13 and now.hour < 20)
                
                if is_trading_time:
                    # 交易时间内：检查持仓 + 扫描市场
                    self.check_holdings()
                    self.scan_market()
                else:
                    # 非交易时间：等待
                    self.log(f"非交易时间，等待中...")
                    print(f"[{datetime.now()}] 非交易时间，等待中...")
                
                # 等待指定间隔后继续
                time.sleep(CONFIG["check_interval"])
                
        except KeyboardInterrupt:
            # 用户中断程序
            self.log("\n" + "=" * 100)
            self.log("=== 程序已停止 ===")
            print(f"\n\n[{datetime.now()}] 程序已停止")
            print(f"当前持仓: {len(self.hold_list)} 只转债")
            print(f"日志已保存到: {CONFIG['log_file']}")

if __name__ == "__main__":
    monitor = BondStockMonitor()
    monitor.run()
