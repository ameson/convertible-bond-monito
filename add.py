import akshare as ak
import json
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

def get_bond_info_from_zh_cov(bond_code):
    try:
        df = ak.bond_zh_cov()
        if df is None or df.empty:
            return None
        
        bond_row = df[df['债券代码'] == bond_code]
        if bond_row.empty:
            return None
        
        bond_row = bond_row.iloc[0]
        return {
            'bond_name': bond_row.get('债券简称', ''),
            'stock_code': bond_row.get('正股代码', ''),
            'stock_name': bond_row.get('正股简称', ''),
            'bond_price': bond_row.get('债现价', 0),
            'premium_rate': bond_row.get('转股溢价率', 0),
            'amount': bond_row.get('发行规模', 0),
        }
    except Exception as e:
        print(f"从 bond_zh_cov 获取转债信息失败: {e}")
        return None

def add_bond():
    """
    添加可转债到1.json文件
    
    功能：
        1. 支持单个转债代码输入
        2. 支持批量转债代码输入（逗号分隔）
        3. 从 bond_zh_cov 获取转债信息
        4. 保存到1.json文件
    """
    print("=" * 100)
    print("=== 添加可转债 ===")
    print("=" * 100)
    
    filename = "1.json"
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = []
    except Exception as e:
        print(f"读取文件失败: {e}")
        return
    
    print(f"当前已有 {len(data)} 个可转债\n")
    
    while True:
        print("\n请选择添加方式：")
        print("1. 单个添加")
        print("2. 批量添加")
        print("q. 退出")
        
        choice = input("\n请输入选项 (1/2/q): ").strip().lower()
        
        if choice == 'q':
            break
        
        if choice == '1':
            # 单个添加
            print("\n--- 单个添加模式 ---")
            bond_code = input("转债代码（6位数字，如 123000）：").strip()
            
            if not bond_code or len(bond_code) != 6 or not bond_code.isdigit():
                print("❌ 转债代码必须是6位数字")
                continue
            
            print(f"\n正在获取 {bond_code} 的信息...")
            
            bond_info = get_bond_info_from_zh_cov(bond_code)
            if not bond_info:
                print(f"❌ 未找到转债代码 {bond_code}")
                continue
            
            bond_name = bond_info['bond_name']
            stock_code = bond_info['stock_code']
            stock_name = bond_info['stock_name']
            bond_price = bond_info['bond_price']
            premium_rate = bond_info['premium_rate']
            amount = bond_info['amount']
            
            print(f"✅ 转债名称: {bond_name}")
            print(f"✅ 正股代码: {stock_code}")
            print(f"✅ 正股名称: {stock_name}")
            print(f"✅ 转债价格: {bond_price}")
            print(f"✅ 溢价率: {premium_rate:.2f}%")
            print(f"✅ 成交额: {amount:.0f}万元")
            
            new_bond = {
                "bond_code": bond_code,
                "bond_name": bond_name,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "bond_price": bond_price,
                "premium_rate": premium_rate,
                "amount": amount,
            }
            
            data.append(new_bond)
            print(f"\n✅ 已添加：{bond_name}({bond_code}) - {stock_name}({stock_code})")
            
            save = input("\n是否继续添加？(y/n): ").strip().lower()
            if save != 'y':
                break
        
        elif choice == '2':
            # 批量添加
            print("\n--- 批量添加模式 ---")
            print("请输入转债代码（逗号分隔，如：110095,123241,123180）")
            print("或者直接按回车使用预设列表")
            
            bond_codes_input = input("转债代码列表: ").strip()
            
            if bond_codes_input:
                # 使用用户输入的代码
                bond_codes = [code.strip() for code in bond_codes_input.split(',')]
            else:
                # 使用预设列表
                bond_codes = [
                    "110095", "123241", "123180", "123129", "118062", "113691", "123131", "127070", "118057", "118052",
                    "123210", "123128", "113039", "127075", "113687", "127092", "111023", "118004", "127094", "118048",
                    "123195", "123182", "113089", "123189", "113695", "123252", "123176", "113052", "113677", "113615",
                    "111012", "113623", "113066", "118049", "118058", "110075", "118042", "113589", "123263", "110067",
                    "118064", "127082", "127050", "127038", "118016", "113076", "127067", "118015", "118042", "113071"
                ]
                print(f"\n使用预设列表，共 {len(bond_codes)} 个转债代码")
            
            # 过滤无效代码
            valid_bond_codes = [code for code in bond_codes if code and len(code) == 6 and code.isdigit()]
            if len(valid_bond_codes) != len(bond_codes):
                print(f"⚠️  过滤掉 {len(bond_codes) - len(valid_bond_codes)} 个无效代码")
            
            print(f"\n开始批量获取 {len(valid_bond_codes)} 个转债信息...\n")
            
            # 一次性获取所有转债数据
            print(f"[1/2] 获取所有转债数据...")
            try:
                df_all = ak.bond_zh_cov()
                if df_all is None or df_all.empty:
                    print(f"❌ 获取转债数据失败")
                    return
                print(f"✅ 成功获取 {len(df_all)} 条转债数据")
            except Exception as e:
                print(f"❌ 获取转债数据失败: {e}")
                return
            
            success_count = 0
            fail_count = 0
            duplicate_count = 0
            
            for i, bond_code in enumerate(valid_bond_codes, 1):
                print(f"[{i}/{len(valid_bond_codes)}] 处理 {bond_code}...")
                
                # 检查是否已存在
                existing = [b for b in data if b['bond_code'] == bond_code]
                if existing:
                    print(f"  ⚠️  转债 {bond_code} 已存在，跳过")
                    duplicate_count += 1
                    continue
                
                # 从所有数据中查找转债
                bond_row = df_all[df_all['债券代码'] == bond_code]
                if bond_row.empty:
                    print(f"  ❌ 未找到转债代码 {bond_code}")
                    fail_count += 1
                    continue
                
                bond = bond_row.iloc[0]
                
                # 添加到数据
                new_bond = {
                    "bond_code": bond_code,
                    "bond_name": bond.get('债券简称', ''),
                    "stock_code": bond.get('正股代码', ''),
                    "stock_name": bond.get('正股简称', ''),
                    "bond_price": bond.get('债现价', 0),
                    "premium_rate": bond.get('转股溢价率', 0),
                    "amount": bond.get('发行规模', 0),
                }
                
                data.append(new_bond)
                print(f"  ✅ 已添加：{new_bond['bond_name']}({bond_code}) - {new_bond['stock_name']}({new_bond['stock_code']})")
                success_count += 1
            
            # 输出统计信息
            print("\n" + "=" * 100)
            print("批量添加完成！")
            print(f"成功: {success_count} 个")
            print(f"失败: {fail_count} 个")
            print(f"重复: {duplicate_count} 个")
            print(f"总计: {len(data)} 个转债")
            print("=" * 100)
            
            break
        else:
            print("❌ 无效选项")
    
    if data:
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"\n[{datetime.now()}] 数据已保存到 {filename}")
            print(f"总共 {len(data)} 个可转债")
        except Exception as e:
            print(f"保存文件失败: {e}")
    
    print("\n程序执行完成！")

if __name__ == "__main__":
    add_bond()
