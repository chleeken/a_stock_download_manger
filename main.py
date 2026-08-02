"""
股票数据下载器 - 主程序入口
运行方式：python main.py
"""
from stock_downloader_gui import StockDownloaderGUI


def main():
    try:
        app = StockDownloaderGUI()
        app.run()
    except Exception as e:
        print(f"程序启动失败: {e}")
        input("按回车键退出...")


if __name__ == "__main__":
    main()
