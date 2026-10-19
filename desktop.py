# -*- coding: utf-8 -*-
"""桌面运维工作台 · 桌面壳（pywebview）

v2.0 — 对齐 AIPM 工具箱工程化经验：
  · 端口冲突提前规划：启动时探测 8000-8020 空闲端口，写入 runtime.json
  · 一键健康检查：后端就绪后先请求 /api/health，结果写入部署日志
  · 部署日志：启动 / 端口 / 健康分 / 异常统一落盘 logs/deploy.log
  · 失败降级：后端未就绪 → 弹窗说明原因，不黑屏退出
"""
import os
import sys
import json
import socket
import time
import platform
import threading
import pathlib
import urllib.request

if getattr(sys, "frozen", False):
    APP_DIR = pathlib.Path(sys.executable).parent
else:
    APP_DIR = pathlib.Path(__file__).resolve().parent

try:
    os.chdir(APP_DIR)
except Exception:
    pass

sys.path.insert(0, str(APP_DIR))

import uvicorn
import webview

import server

HOST = "127.0.0.1"
TITLE = "桌面运维工作台"
PORT_RANGE = range(8000, 8021)
VERSION = getattr(server, "VERSION", "4.0.0")

DATA_DIR = pathlib.Path.home() / ".ops_workbench"
LOGS_DIR = DATA_DIR / "logs"
RUNTIME_JSON = DATA_DIR / "runtime.json"


def log_deploy(event, **fields):
    """写部署日志（JSONL），与 server 的部署日志同目录。"""
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        rec = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "event": event,
            **fields,
        }
        with open(LOGS_DIR / "deploy.log", "a", encoding="utf-8") as fp:
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def port_in_use(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def find_free_port():
    for p in PORT_RANGE:
        if not port_in_use(HOST, p):
            return p
    return None


def wait_port(host, port, timeout=25):
    start = time.time()
    while time.time() - start < timeout:
        if port_in_use(host, port):
            return True
        time.sleep(0.15)
    return False


def http_get_json(url, timeout=8):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return None


def check_webview2():
    if platform.system() != "Windows":
        return True
    candidates = [
        pathlib.Path(os.environ.get("ProgramFiles(x86)",
                                    r"C:\Program Files (x86)"))
            / "Microsoft" / "EdgeWebView" / "Application",
        pathlib.Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
            / "Microsoft" / "EdgeWebView" / "Application",
    ]
    return any(p.exists() for p in candidates)


def fatal(msg):
    print(msg, file=sys.stderr)
    html = (
        '<html><body style="font-family:\'Microsoft YaHei\',sans-serif;'
        'padding:20px;background:#12161f;color:#e6ebf5;line-height:1.8">'
        '<h3 style="margin:0 0 12px;color:#ff8080">启动失败</h3>'
        '<p style="white-space:pre-wrap;font-size:13px">' + msg + '</p>'
        '</body></html>'
    )
    try:
        webview.create_window("启动失败", html=html, width=560, height=320)
        webview.start()
    except Exception:
        pass
    sys.exit(1)


def run_server(port):
    config = uvicorn.Config(
        server.app,
        host=HOST,
        port=port,
        log_level="warning",
        access_log=False,
    )
    srv = uvicorn.Server(config)
    try:
        srv.run()
    except Exception as e:
        log_deploy("server_crash", error=str(e))
        print(f"[server] 后端异常退出：{e}", file=sys.stderr)


def main():
    log_deploy("desktop_start", version=VERSION, py=sys.executable)

    if not check_webview2():
        fatal(
            "未检测到 Microsoft Edge WebView2 Runtime。\n"
            "本程序依赖它来显示界面，请先安装后重新运行。\n"
            "下载地址：https://developer.microsoft.com/microsoft-edge/webview2/"
        )

    # 若已有实例在运行（任意端口被本程序占用），直接复用其页面
    try:
        if RUNTIME_JSON.exists():
            old = json.loads(RUNTIME_JSON.read_text(encoding="utf-8"))
            old_port = int(old.get("port", 0))
            if old_port and port_in_use(HOST, old_port):
                log_deploy("reuse_instance", port=old_port)
                webview.create_window(
                    TITLE, f"http://{HOST}:{old_port}",
                    width=1440, height=900, min_size=(1024, 640),
                    text_select=True,
                )
                webview.start()
                os._exit(0)
    except Exception:
        pass

    port = find_free_port()
    if port is None:
        fatal("端口 8000-8020 全部被占用。\n请关闭占用端口的程序后重试。")
    log_deploy("port_selected", port=port)

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        RUNTIME_JSON.write_text(
            json.dumps({
                "port": port, "pid": os.getpid(),
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "version": VERSION,
            }, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        log_deploy("runtime_json_error", error=str(e))

    t = threading.Thread(target=run_server, args=(port,), daemon=True)
    t.start()

    url = f"http://{HOST}:{port}"
    if not wait_port(HOST, port):
        fatal(
            "后端启动超时（端口 " + str(port) + " 未就绪）。\n\n"
            "可能原因：\n"
            "  · server.py 导入失败（依赖缺失）\n"
            "  · 端口被防火墙拦截\n"
            "  · 系统缺少必要运行库"
        )

    # 启动自检：请求 /api/health 一次，记录结果（失败不阻断，页面会显示）
    health = http_get_json(url + "/api/health")
    if health is None:
        log_deploy("health_probe_failed", port=port)
    else:
        log_deploy("health_ok", port=port,
                   score=health.get("score"),
                   status=health.get("status"),
                   checks=len(health.get("checks") or []))

    webview.create_window(
        TITLE, url,
        width=1440, height=900,
        min_size=(1024, 640),
        text_select=True,
    )
    webview.start()
    log_deploy("desktop_exit", port=port)
    os._exit(0)


if __name__ == "__main__":
    main()
