import os
import tkinter as tk
from tkinter import messagebox

def convert_xls_to_csv():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    xls_path = os.path.join(script_dir, 'Table.xls')
    csv_path = os.path.join(script_dir, 'table.csv')
    
    if not os.path.exists(xls_path):
        root = tk.Tk()
        root.withdraw()
        messagebox.showwarning("警告", "找不到 Table.xls 文件")
        root.destroy()
        return
    
    try:
        # 读取文件
        for encoding in ['gbk', 'utf-8', 'gb2312']:
            try:
                with open(xls_path, 'r', encoding=encoding) as f:
                    content = f.read()
                break
            except:
                continue
        else:
            raise Exception("无法识别文件编码")
        
        # 替换制表符为逗号
        content = content.replace('\t', ',')
        
        # 保存为 CSV
        with open(csv_path, 'w', encoding='utf-8-sig') as f:
            f.write(content)
        
        # 成功提示
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo("成功", f"转换完成！\nTable.xls → table.csv")
        root.destroy()
        
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("错误", f"转换失败：{str(e)}")
        root.destroy()

if __name__ == "__main__":
    convert_xls_to_csv()