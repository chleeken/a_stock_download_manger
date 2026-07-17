import sys
sys.path.insert(0, r"D:\stock_folder\a_stock_download_manger_all")

from minute60_stock_data_download import StockDownloader, config
from datetime import datetime

with StockDownloader() as d:
    codes = d.read_file()
    print(f"\n从table.txt读取 {len(codes)} 只股票\n")

    # ====== 第一步：数据库已有数据统计 ======
    print("=" * 80)
    print("第一步：查询数据库已有数据概况")
    print("=" * 80)
    
    stats = d.con.execute("""
        SELECT code, COUNT(*) as cnt, MIN(date_time) as min_dt, MAX(date_time) as max_dt
        FROM minute60_data 
        GROUP BY code
        ORDER BY code
    """).fetchall()
    
    in_db = {}
    for row in stats:
        in_db[row[0]] = {'count': row[1], 'min_date': str(row[2])[:10], 'max_date': str(row[3])[:10]}
    
    print(f"数据库中有 {len(in_db)} 只股票的数据")
    print(f"总记录数: {sum(v['count'] for v in in_db.values())}")
    
    # 分类
    need_update = []
    up_to_date = []
    no_data = []
    
    for code in codes:
        c = d.norm(code)
        if c in in_db:
            info = in_db[c]
            days_gap = None
            if info['max_date']:
                try:
                    last = datetime.strptime(info['max_date'], '%Y-%m-%d')
                    now = datetime.now()
                    days_gap = (now - last).days
                except:
                    pass
            
            if days_gap is not None and days_gap > 1:
                need_update.append((c, info))
            elif days_gap is not None and days_gap <= 1:
                up_to_date.append(c)
            else:
                up_to_date.append(c)
        else:
            no_data.append(c)
    
    print(f"\n分类统计:")
    print(f"  需更新 (>1天): {len(need_update)} 只")
    print(f"  已更新 (<=1天): {len(up_to_date)} 只")
    print(f"  无数据: {len(no_data)} 只")
    print()

    # ====== 第二步：批量下载 ======
    print("=" * 80)
    print("第二步：批量下载股票数据")
    print("=" * 80)
    
    to_download = need_update + [(c, {}) for c in no_data]
    total = len(to_download)
    success = 0
    skip = 0
    failed = 0
    
    for i, item in enumerate(to_download):
        code = item[0]
        try:
            ok, msg = d.download_single_stock(code, force=False)
            if ok:
                success += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
        
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{total}] 成功:{success} 跳过:{skip} 失败:{failed}")
    
    print(f"\n下载完成:")
    print(f"  本次处理: {total} 只")
    print(f"  成功: {success}")
    print(f"  跳过: {skip}")
    print(f"  失败: {failed}")
    print(f"  总计: {len(codes)} 只 (其中{len(up_to_date)}只已更新无需处理)")
