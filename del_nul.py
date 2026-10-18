import os
import sys
import time
import threading
import ctypes
import ctypes.wintypes as wintypes

user32 = ctypes.windll.user32
WM_CLOSE = 0x0010


def _show_msgbox(title: str, msg: str, delay: int = 1):
    MB_ICON_INFO = 0x40
    MB_OK = 0x0000

    def _run():
        user32.MessageBoxW(None, msg, title, MB_ICON_INFO | MB_OK)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(delay)
    hwnd = user32.FindWindowW(None, title)
    if hwnd:
        user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)


def _nul_path_upper(dir_path: str) -> str:
    d = os.path.normpath(dir_path).replace('/', '\\').upper()
    return '\\\\?\\' + d + '\\nul'


def delete_nul(dir_path: str) -> bool:
    ext = _nul_path_upper(dir_path)
    try:
        os.remove(ext)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def get_target_dir():
    if len(sys.argv) > 1:
        return os.path.normpath(sys.argv[1])
    return os.path.dirname(os.path.abspath(sys.argv[0]))


if __name__ == '__main__':
    silent = len(sys.argv) > 1
    target = get_target_dir()
    if os.path.isdir(target):
        ok = delete_nul(target)
        if not silent:
            if ok:
                _show_msgbox('删除 nul 文件', '已成功删除 nul 文件')
            else:
                _show_msgbox('删除 nul 文件', '目录中未找到 nul 文件')
    else:
        if not silent:
            _show_msgbox('删除 nul 文件', f'目录不存在:\n{target}')
