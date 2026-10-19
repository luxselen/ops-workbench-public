# -*- coding: utf-8 -*-
# 桌面运维工作台 · 后端服务（v4.0.0）
# 架构：FastAPI + SSE + 本地规则诊断引擎 + 可选外部 LLM（默认关闭，数据不出本机）
# 分层约定：
#   - 配置/密钥/日志/数据 统一落在用户目录 ~/.ops_workbench（不污染系统环境）
#   - 运行数据（记录/备份/报告）落在用户指定的 record_dir / backup_dir
#   - 每个动作独立超时、独立留痕；单任务失败不影响整体
#   - AI 仅做辅助诊断，所有建议需人工复核后才允许执行，绝不自动执行业务操作
import asyncio
import json
import os
import sys
import platform
import shutil
import datetime
import pathlib
import uuid
import subprocess
import time
import collections

from fastapi import FastAPI, Query, Body, Request
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
import psutil

try:
    import winreg  # noqa: F401
except ImportError:
    winreg = None

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    HAS_SCHED = True
except ImportError:
    HAS_SCHED = False

VERSION = "4.0.0"
DIAG_RULES_VERSION = "1.2.0"
START_TIME = time.time()

# ---------------- 路径 ----------------
if getattr(sys, "frozen", False):
    RES_DIR = pathlib.Path(sys._MEIPASS)
    APP_DIR = pathlib.Path(sys.executable).parent
else:
    RES_DIR = pathlib.Path(__file__).resolve().parent
    APP_DIR = RES_DIR

try:
    os.chdir(APP_DIR)
except Exception:
    pass

IS_WIN = platform.system() == "Windows"
IS_MAC = platform.system() == "Darwin"

# ---------------- 配置 / 密钥 / 日志 目录（统一隔离到用户目录） ----------------
CONFIG_DIR = pathlib.Path.home() / ".ops_workbench"
CONFIG_FILE = CONFIG_DIR / "config.json"
SECRETS_FILE = CONFIG_DIR / "secrets.json"
LOGS_DIR = CONFIG_DIR / "logs"
CONFIG_BACKUP_DIR = CONFIG_DIR / "config_backups"


def ensure_dir(p):
    try:
        p.mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        return False


def _touch_writable(p):
    try:
        probe = p / f".probe_{uuid.uuid4().hex[:6]}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except Exception:
        return False


for _d in (CONFIG_DIR, LOGS_DIR, CONFIG_BACKUP_DIR):
    ensure_dir(_d)

DEFAULT_CONFIG = {
    "configured": False,
    "backup_dir": "",
    "record_dir": "",
    "theme": "dark",
    "log_mode": "simple",
    "favorites": [],
    "schedules": [],
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    try:
        if CONFIG_FILE.exists():
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update(data)
    except Exception:
        pass
    return cfg


def _backup_config_file():
    """保存前先把旧配置归档到 config_backups（保留最近 10 份），实现配置级备份。"""
    try:
        if not CONFIG_FILE.exists():
            return
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = CONFIG_BACKUP_DIR / f"config_{ts}.json"
        shutil.copy2(CONFIG_FILE, dest)
        backups = sorted(CONFIG_BACKUP_DIR.glob("config_*.json"), reverse=True)
        for old in backups[10:]:
            try:
                old.unlink()
            except Exception:
                pass
    except Exception:
        pass


def save_config(cfg):
    try:
        _backup_config_file()
        CONFIG_FILE.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


CONFIG = load_config()


def get_backup_dir():
    p = CONFIG.get("backup_dir") or ""
    return pathlib.Path(p).expanduser() if p else APP_DIR / "backups"


def get_record_dir():
    p = CONFIG.get("record_dir") or ""
    return pathlib.Path(p).expanduser() if p else APP_DIR / "records"


HOME = pathlib.Path.home()
RECOMMENDED_ROOT = HOME / "Documents" / "桌面运维工作台"

app = FastAPI(title="桌面运维工作台", version=VERSION)

# ================= 结构化日志（LLMOps：独立日志、统一管理） =================
# 每个 scope 一个 JSONL 文件：app / action / error / deploy / diag
LOG_SCOPES = {}


def log_json(scope, level, event, **fields):
    """结构化落盘日志，一行一条 JSON。失败静默，不影响主流程。"""
    try:
        record = {
            "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "level": level,
            "event": event,
            **fields,
        }
        f = LOGS_DIR / f"{scope}.log"
        with open(f, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def log_deploy(event, **fields):
    log_json("deploy", "info", event, **fields)


def log_error(scope, event, **fields):
    log_json("error", "error", event, scope=scope, **fields)


# ================= 日志广播（SSE） =================
log_queues = set()
log_history = []


def now_ts():
    return datetime.datetime.now().strftime("%H:%M:%S")


async def broadcast(level, text, channel="tech"):
    item = {"ts": now_ts(), "level": level,
            "text": str(text), "channel": channel}
    log_history.append(item)
    if len(log_history) > 800:
        log_history.pop(0)
    dead = []
    for q in log_queues:
        try:
            q.put_nowait(item)
        except Exception:
            dead.append(q)
    for q in dead:
        log_queues.discard(q)


async def broadcast_user(text, level="info"):
    await broadcast(level, text, channel="user")


@app.get("/api/logs")
async def logs(request: Request):
    q = asyncio.Queue()
    log_queues.add(q)

    async def gen():
        try:
            for item in log_history[-150:]:
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            log_queues.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ================= 命令执行（带超时与流式输出） =================
def _decode_cmd_bytes(b):
    """兼容 PowerShell 管道输出的 UTF-16/UTF-8/GBK 混合编码。"""
    if len(b) >= 2 and b[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return b.decode("utf-16", errors="replace").lstrip("\ufeff")
    if len(b) >= 4 and b[1] == 0 and b[3] == 0:
        return b.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    try:
        return b.decode("utf-8", errors="strict")
    except Exception:
        pass
    try:
        return b.decode("gbk", errors="strict")
    except Exception:
        return b.decode("utf-8", errors="replace")


async def _drain_proc(proc, quiet):
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        txt = _decode_cmd_bytes(line).rstrip()
        if txt and not quiet:
            await broadcast("info", txt, channel="tech")


async def run_command(cmd, cwd=None, quiet=False, show_cmd=True, timeout=None):
    if show_cmd:
        await broadcast("cmd", f"$ {cmd}", channel="tech")
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(cwd or APP_DIR),
        )
        try:
            if timeout:
                await asyncio.wait_for(_drain_proc(proc, quiet), timeout)
                await proc.wait()
            else:
                await _drain_proc(proc, quiet)
                await proc.wait()
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            if not quiet:
                await broadcast("warn",
                                f"命令执行超时（超过 {timeout}s），已终止",
                                channel="tech")
            log_json("action", "warn", "cmd_timeout", cmd=cmd[:200],
                     timeout=timeout)
            return 124
        if not quiet:
            await broadcast("ok" if proc.returncode == 0 else "warn",
                            f"完成，退出码 {proc.returncode}", channel="tech")
        return proc.returncode
    except Exception as e:
        await broadcast("error", str(e), channel="tech")
        log_error("cmd", "run_failed", cmd=cmd[:200], error=str(e))
        return -1


async def run_ps(script, timeout=60):
    if not IS_WIN:
        return "", "非 Windows 平台"
    header = ("[Console]::OutputEncoding = [System.Text.Encoding]::UTF8\n"
              "$OutputEncoding = [System.Text.Encoding]::UTF8\n"
              "$ErrorActionPreference = 'SilentlyContinue'\n")
    tmp = APP_DIR / f"_tmp_{uuid.uuid4().hex}.ps1"
    try:
        tmp.write_text("\ufeff" + header + script, encoding="utf-8")
        proc = await asyncio.create_subprocess_shell(
            f'powershell -NoProfile -ExecutionPolicy Bypass -File "{tmp}"',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (out.decode("utf-8", errors="replace"),
                err.decode("utf-8", errors="replace"))
    except asyncio.TimeoutError:
        return "", "命令超时"
    except Exception as e:
        return "", str(e)
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


def parse_ps_json(out):
    if not out or not out.strip():
        return []
    try:
        data = json.loads(out)
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []
# ================= 操作记录 =================
def write_record(action_id, name, detail, status):
    try:
        d = datetime.datetime.now()
        rec = {
            "time": d.strftime("%Y-%m-%d %H:%M:%S"),
            "action": action_id,
            "name": name,
            "detail": str(detail)[:500],
            "status": status,
        }
        rd = get_record_dir()
        if not ensure_dir(rd):
            return
        f = rd / f"{d.strftime('%Y-%m-%d')}.jsonl"
        with open(f, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
        log_json("action", status, "action_record", action=action_id,
                 name=name, status=status)
    except Exception as e:
        log_error("action", "record_failed", error=str(e))


@app.get("/api/records")
async def get_records(limit: int = 200):
    rd = get_record_dir()
    if not rd.exists():
        return []
    files = sorted(rd.glob("*.jsonl"), reverse=True)
    out = []
    for f in files:
        try:
            lines = f.read_text(encoding="utf-8").strip().splitlines()
        except Exception:
            continue
        for line in reversed(lines):
            try:
                out.append(json.loads(line))
            except Exception:
                pass
            if len(out) >= limit:
                break
        if len(out) >= limit:
            break
    return out


@app.get("/api/backup/list")
async def backup_list():
    bd = get_backup_dir()
    items = []
    if bd.exists():
        for d in sorted(bd.iterdir(), reverse=True):
            try:
                items.append({
                    "name": d.name,
                    "time": datetime.datetime.fromtimestamp(
                        d.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
            except Exception:
                pass
    return items[:50]


@app.get("/api/config_backups/list")
async def config_backups_list():
    items = []
    try:
        for f in sorted(CONFIG_BACKUP_DIR.glob("config_*.json"), reverse=True):
            try:
                items.append({
                    "name": f.name,
                    "time": datetime.datetime.fromtimestamp(
                        f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
            except Exception:
                pass
    except Exception:
        pass
    return items[:10]


# ================= 设置 =================
@app.get("/api/settings")
async def get_settings():
    return {
        "configured": bool(CONFIG.get("configured")),
        "backup_dir": CONFIG.get("backup_dir") or "",
        "record_dir": CONFIG.get("record_dir") or "",
        "theme": CONFIG.get("theme") or "dark",
        "log_mode": CONFIG.get("log_mode") or "simple",
        "version": VERSION,
        "config_file": str(CONFIG_FILE),
        "logs_dir": str(LOGS_DIR),
        "default_backup_dir": str(APP_DIR / "backups"),
        "default_record_dir": str(APP_DIR / "records"),
        "recommended_backup_dir": str(RECOMMENDED_ROOT / "backups"),
        "recommended_record_dir": str(RECOMMENDED_ROOT / "records"),
        "home": str(HOME),
        "desktop": str(HOME / "Desktop"),
        "documents": str(HOME / "Documents"),
        "platform": platform.system(),
    }


@app.post("/api/settings")
async def update_settings(payload: dict = Body(...)):
    backup_dir = (payload.get("backup_dir") or "").strip()
    record_dir = (payload.get("record_dir") or "").strip()
    theme = payload.get("theme")
    log_mode = payload.get("log_mode")

    if backup_dir:
        p = pathlib.Path(backup_dir).expanduser()
        if not ensure_dir(p):
            return JSONResponse({"detail": f"备份目录不可用：{p}"}, status_code=400)
        CONFIG["backup_dir"] = str(p)
    if record_dir:
        p = pathlib.Path(record_dir).expanduser()
        if not ensure_dir(p):
            return JSONResponse({"detail": f"记录目录不可用：{p}"}, status_code=400)
        CONFIG["record_dir"] = str(p)
    if theme in ("dark", "light"):
        CONFIG["theme"] = theme
    if log_mode in ("simple", "detail"):
        CONFIG["log_mode"] = log_mode

    CONFIG["configured"] = True
    if not save_config(CONFIG):
        return JSONResponse({"detail": "配置写入失败"}, status_code=500)
    await broadcast_user("[完成] 设置已保存", "ok")
    return {"ok": True, "settings": await get_settings()}


@app.post("/api/settings/reset")
async def reset_settings():
    CONFIG.clear()
    CONFIG.update(DEFAULT_CONFIG)
    save_config(CONFIG)
    return {"ok": True}


@app.post("/api/open_folder")
async def open_folder(payload: dict = Body(...)):
    target = (payload.get("path") or "").strip()
    if not target:
        return JSONResponse({"detail": "未指定路径"}, status_code=400)
    p = pathlib.Path(target).expanduser()
    if not p.exists():
        return JSONResponse({"detail": f"目录不存在：{p}"}, status_code=404)
    try:
        if IS_WIN:
            os.startfile(str(p))
        elif IS_MAC:
            subprocess.Popen(["open", str(p)])
        else:
            subprocess.Popen(["xdg-open", str(p)])
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)


# ================= 密钥管理（集中存储，掩码返回） =================
DEFAULT_SECRETS = {"llm_api_key": "", "llm_base_url": "", "llm_model": ""}


def load_secrets():
    sec = dict(DEFAULT_SECRETS)
    try:
        if SECRETS_FILE.exists():
            data = json.loads(SECRETS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                sec.update(data)
    except Exception:
        pass
    return sec


def save_secrets(sec):
    try:
        SECRETS_FILE.write_text(
            json.dumps(sec, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


SECRETS = load_secrets()


def _mask_key(key):
    if not key:
        return ""
    if len(key) <= 8:
        return "*" * len(key)
    return key[:4] + "****" + key[-4:]


@app.get("/api/secrets")
async def get_secrets():
    return {
        "llm_configured": bool(SECRETS.get("llm_api_key")),
        "llm_api_key_masked": _mask_key(SECRETS.get("llm_api_key") or ""),
        "llm_base_url": SECRETS.get("llm_base_url") or "",
        "llm_model": SECRETS.get("llm_model") or "",
        "storage": str(SECRETS_FILE),
    }


@app.post("/api/secrets")
async def save_secret(payload: dict = Body(...)):
    """保存密钥。仅接受 llm 相关配置；空值表示清除。"""
    base = (payload.get("llm_base_url") or "").strip()
    model = (payload.get("llm_model") or "").strip()
    key = (payload.get("llm_api_key") or "").strip()
    if key == "" and payload.get("keep_key"):
        key = SECRETS.get("llm_api_key") or ""
    SECRETS["llm_base_url"] = base
    SECRETS["llm_model"] = model
    SECRETS["llm_api_key"] = key
    if not save_secrets(SECRETS):
        return JSONResponse({"detail": "密钥写入失败"}, status_code=500)
    log_json("app", "info", "secrets_updated",
             llm_configured=bool(key), base_url=base[:60])
    return {"ok": True, "secrets": await get_secrets()}


@app.post("/api/secrets/remove")
async def remove_secret(payload: dict = Body(...)):
    target = (payload.get("target") or "llm_api_key").strip()
    if target in SECRETS:
        SECRETS[target] = ""
    save_secrets(SECRETS)
    return {"ok": True, "secrets": await get_secrets()}


# ================= 收藏 =================
@app.get("/api/favorites")
async def get_favorites():
    return CONFIG.get("favorites", [])


@app.post("/api/favorites/toggle")
async def toggle_favorite(payload: dict = Body(...)):
    aid = payload.get("id")
    if not aid:
        return JSONResponse({"detail": "缺少 id"}, status_code=400)
    favs = list(CONFIG.get("favorites", []))
    if aid in favs:
        favs.remove(aid)
    else:
        favs.append(aid)
    CONFIG["favorites"] = favs
    save_config(CONFIG)
    return {"ok": True, "favorites": favs}


# ================= 目录大小 & 报告式清理 =================
def dir_size(path):
    total = 0
    try:
        for r, dirs, files in os.walk(path):
            for f in files:
                try:
                    total += (pathlib.Path(r) / f).stat().st_size
                except Exception:
                    pass
    except Exception:
        pass
    return total


async def clean_dir_with_report(target_path, label):
    p = pathlib.Path(target_path)
    if not p.exists():
        await broadcast_user(f"[注意] {label}不存在，跳过", "warn")
        return 0
    await broadcast_user(f"[开始] 正在扫描 {label}，请稍候…")
    before = dir_size(p)
    await broadcast_user(f"扫描完成，当前占用 {before/1024/1024:.1f} MB，开始清理…")
    failed = 0
    try:
        for item in p.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=False)
                else:
                    item.unlink()
            except Exception:
                failed += 1
    except Exception as e:
        await broadcast_user(f"[错误] {label}清理失败：{e}", "error")
        return 0
    after = dir_size(p)
    released = max(0, before - after)
    await broadcast_user(
        f"[完成] {label}清理完成，释放约 {released/1024/1024:.1f} MB"
        f"（跳过 {failed} 个占用中的文件）", "ok")
    return released# ================= Python 型任务 =================
async def task_scan_temp():
    paths = [("用户临时目录", pathlib.Path(os.environ.get("TEMP", "/tmp")))]
    if IS_WIN:
        paths.append(("系统临时目录",
                      pathlib.Path(os.environ.get("windir", r"C:\Windows")) / "Temp"))
    else:
        paths.append(("系统临时目录", pathlib.Path("/tmp")))
    await broadcast_user("[开始] 正在扫描临时文件占用…")
    for label, p in paths:
        if not p.exists():
            await broadcast_user(f"[注意] {label}不存在", "warn")
            continue
        total, cnt = 0, 0
        try:
            for root, dirs, files in os.walk(p):
                for f in files:
                    try:
                        total += (pathlib.Path(root) / f).stat().st_size
                        cnt += 1
                    except Exception:
                        pass
        except Exception:
            pass
        await broadcast_user(
            f"[目录] {label}：{cnt} 个文件，共 {total/1024/1024:.1f} MB")
    await broadcast_user("[完成] 扫描完成", "ok")
    return "scan done"


async def task_clean_user_temp():
    return await clean_dir_with_report(os.environ.get("TEMP", "/tmp"), "用户临时文件")


async def task_clean_sys_temp():
    if IS_WIN:
        target = pathlib.Path(os.environ.get("windir", r"C:\Windows")) / "Temp"
    else:
        target = pathlib.Path("/tmp")
    return await clean_dir_with_report(target, "系统临时文件")


async def task_battery():
    rd = get_record_dir()
    ensure_dir(rd)
    out_file = rd / "battery-report.html"
    await broadcast_user("[开始] 正在生成电池报告…")
    if IS_WIN:
        await run_command(f'powercfg /batteryreport /output "{out_file}"',
                          show_cmd=False)
    await broadcast_user(f"[完成] 电池报告已生成：{out_file}", "ok")
    return str(out_file)


async def task_startup():
    items = await _list_startup()
    await broadcast_user(f"[开始] 共找到 {len(items)} 个启动项")
    for it in items:
        await broadcast_user(f"· {it['name']}")
    return "done"


async def task_services():
    items = await _list_services()
    auto = [x for x in items if x.get("start_type") == "Automatic"]
    await broadcast_user(f"[开始] 共 {len(auto)} 个自动启动服务")
    for x in auto[:30]:
        tag = "系统关键" if x.get("protected") else "可禁用"
        await broadcast_user(f"· {x['name']}（{tag}）")
    return "done"


async def task_tasks():
    items = await _list_tasks()
    await broadcast_user(f"[开始] 共找到 {len(items)} 个计划任务")
    for x in items[:30]:
        tag = "系统任务" if x.get("protected") else "可禁用"
        await broadcast_user(f"· {x['name']}（{tag}）")
    return "done"


async def task_defender():
    script = ("Get-MpComputerStatus | "
              "Select-Object AMServiceEnabled,AntivirusEnabled,"
              "RealTimeProtectionEnabled,AntivirusSignatureLastUpdated | "
              "ConvertTo-Json")
    out, err = await run_ps(script)
    await broadcast_user("[开始] 检查 Defender 状态…")
    for line in out.splitlines():
        if line.strip():
            await broadcast_user(line)
    return "done"


async def task_firewall():
    script = "Get-NetFirewallProfile | Select-Object Name,Enabled | ConvertTo-Json"
    out, err = await run_ps(script)
    await broadcast_user("[开始] 检查防火墙状态…")
    for line in out.splitlines():
        if line.strip():
            await broadcast_user(line)
    return "done"


async def task_smart():
    script = ("Get-PhysicalDisk | "
              "Select-Object FriendlyName,MediaType,HealthStatus,"
              "OperationalStatus,Size | ConvertTo-Json")
    out, err = await run_ps(script)
    await broadcast_user("[开始] 正在读取磁盘健康状态…")
    for x in parse_ps_json(out):
        size_gb = round((x.get("Size") or 0) / 1024 ** 3, 1)
        await broadcast_user(
            f"[磁盘] {x.get('FriendlyName')}  {x.get('MediaType')}  "
            f"{size_gb}GB  健康:{x.get('HealthStatus')}")
    return "done"


async def task_asset():
    script = r"""
$os   = Get-CimInstance Win32_OperatingSystem
$cs   = Get-CimInstance Win32_ComputerSystem
$bios = Get-CimInstance Win32_BIOS
$net  = Get-CimInstance Win32_NetworkAdapterConfiguration | Where-Object {$_.MACAddress}
[PSCustomObject]@{
  Hostname     = $env:COMPUTERNAME
  User         = $env:USERNAME
  OS           = $os.Caption
  Version      = $os.Version
  Serial       = $bios.SerialNumber
  Manufacturer = $cs.Manufacturer
  Model        = $cs.Model
  MAC          = ($net.MACAddress -join ', ')
} | ConvertTo-Json
"""
    out, err = await run_ps(script)
    await broadcast_user("[开始] 采集资产信息…")
    try:
        d = json.loads(out)
        for k, v in d.items():
            await broadcast_user(f"{str(k).ljust(14)}: {v}")
    except Exception:
        for line in out.splitlines():
            if line.strip():
                await broadcast_user(line)
    return "done"


async def task_backup():
    src = APP_DIR
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    bd = get_backup_dir()
    if not ensure_dir(bd):
        await broadcast_user(f"[错误] 备份目录不可用：{bd}", "error")
        return "backup dir unavailable"
    dest = bd / f"backup_{ts}"
    await broadcast_user(f"[开始] 正在备份工作目录到 {dest}…")
    try:
        shutil.copytree(
            src, dest,
            ignore=shutil.ignore_patterns(
                "backups", "__pycache__", "records",
                "*.pyc", "_tmp_*", ".ops_workbench", "build", "dist"))
        await broadcast_user(f"[完成] 备份完成：{dest}", "ok")
        return str(dest)
    except Exception as e:
        await broadcast_user(f"[错误] 备份失败：{e}", "error")
        return str(e)


PY_TASKS = {
    "scan_temp": task_scan_temp,
    "clean_user_temp": task_clean_user_temp,
    "clean_sys_temp": task_clean_sys_temp,
    "battery": task_battery,
    "startup": task_startup,
    "services": task_services,
    "tasks": task_tasks,
    "defender": task_defender,
    "firewall": task_firewall,
    "smart": task_smart,
    "asset": task_asset,
    "backup": task_backup,
}

# ================= 动作表（schema：name/tip/risk/detail/kind + cmd|py） =================
ACTIONS = {
    "scan_temp": {
        "name": "扫描临时文件占用", "risk": "low",
        "tip": "只统计不删除",
        "begin_text": "[开始] 正在扫描临时文件占用…",
        "done_text": "[完成] 临时文件扫描完成",
        "detail": "【做什么】只统计不删除，看看临时文件夹里堆了多少垃圾。\n\n【什么时候用】C 盘空间紧张想先摸清情况时，或定期体检时先跑一遍。\n\n【要注意】只是看一眼，不会动任何文件，随便跑。",
        "kind": "py", "py": "scan_temp",
    },
    "clean_user_temp": {
        "name": "清理用户临时文件", "risk": "medium",
        "tip": "删除 %TEMP% 内容，占用中的文件会跳过",
        "begin_text": "[开始] 正在清理用户临时文件…",
        "done_text": "[完成] 用户临时文件清理完成",
        "detail": "【做什么】删掉你账号下 %TEMP% 里的临时文件，腾出 C 盘空间。\n\n【什么时候用】C 盘空间不够、系统用久了变卡、装软件前想清一清。\n\n【要注意】正在被程序占用的文件会自动跳过，不会强删。建议先关掉不用的软件再执行，效果更好。",
        "kind": "py", "py": "clean_user_temp",
    },
    "clean_sys_temp": {
        "name": "清理系统临时文件", "risk": "medium",
        "tip": "清理 C:\\Windows\\Temp，建议管理员运行",
        "begin_text": "[开始] 正在清理系统临时文件…",
        "done_text": "[完成] 系统临时文件清理完成",
        "detail": "【做什么】删掉 C:\\Windows\\Temp 里的临时文件。\n\n【什么时候用】C 盘空间紧张，且用户临时文件夹已经清过之后。\n\n【要注意】部分文件需要管理员权限才能删，普通权限下会跳过一些。建议右键以管理员身份运行本程序再执行。",
        "kind": "py", "py": "clean_sys_temp",
    },
    "clean_recycle": {
        "name": "清空回收站", "risk": "high",
        "tip": "不可恢复，请确认后再执行",
        "begin_text": "[开始] 正在清空回收站…",
        "done_text": "[完成] 回收站已清空",
        "detail": "【做什么】把回收站里的东西彻底删掉，释放磁盘空间。\n\n【什么时候用】确认回收站里没有还需要找回的文件时。\n\n【要注意】删了就找不回来了。执行前先打开回收站翻一遍，确认没有误删的重要文件。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; '
                'Clear-RecycleBin -Force -ErrorAction SilentlyContinue; '
                'Write-Output \'回收站已清空\'"'),
    },
    "clean_dns": {
        "name": "刷新 DNS 缓存", "risk": "low",
        "tip": "清除本机 DNS 解析缓存",
        "begin_text": "[开始] 正在刷新 DNS 缓存…",
        "done_text": "[完成] DNS 缓存已刷新",
        "detail": "【做什么】清掉本机记的网址解析缓存，让网络重新查一遍。\n\n【什么时候用】网页打不开、某个网站突然访问不了、改了 hosts 文件不生效。\n\n【要注意】完全安全，随便跑。清完后前几个网页可能稍微慢一点点，属于正常。",
        "kind": "cmd", "cmd": "ipconfig /flushdns",
    },
    "clean_thumb": {
        "name": "清理缩略图缓存", "risk": "low",
        "tip": "删除缩略图数据库，下次浏览会重建",
        "begin_text": "[开始] 正在清理缩略图缓存…",
        "done_text": "[完成] 缩略图缓存已清理",
        "detail": "【做什么】删掉文件资源管理器里图片、视频的小缩略图数据库。\n\n【什么时候用】缩略图显示错乱、图标显示成白色方块、文件夹打开很卡。\n\n【要注意】完全安全。下次打开文件夹会重新生成缩略图，头一次会稍微慢几秒。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; '
                'Remove-Item -Path '
                '$env:LOCALAPPDATA\\Microsoft\\Windows\\Explorer\\thumbcache_*.db '
                '-Force -ErrorAction SilentlyContinue; '
                'Write-Output \'缩略图缓存已清理\'"'),
    },
    "quick_clean": {
        "name": "一键清理（低风险）", "risk": "medium",
        "tip": "依次清理临时文件、回收站、缩略图缓存，不删除个人文档",
        "begin_text": "[开始] 一键清理启动…",
        "done_text": "[完成] 一键清理完成",
        "detail": "【做什么】把最常见的三类垃圾一次清干净：你的临时文件、回收站、缩略图缓存。\n\n【什么时候用】C 盘快满了、电脑用久了变慢，或者懒得一个个清理的时候。\n\n【要注意】只做低风险清理，不会碰你的文档、照片和程序。正在使用的文件会自动跳过。可设为定时任务每天自动跑。",
        "kind": "py", "py": "quick_clean",
    },
    "ssd_trim": {
        "name": "SSD 优化（TRIM）", "risk": "low",
        "tip": "对系统盘执行 Windows 优化驱动器",
        "begin_text": "[开始] 正在执行 SSD TRIM 优化…",
        "done_text": "[完成] SSD 优化完成",
        "detail": "【做什么】对系统盘做一次 SSD 专用的 TRIM 优化，让它保持速度。\n\n【什么时候用】用 SSD 的电脑，建议每月跑一次。\n\n【要注意】只对 SSD 有效，机械硬盘别跑。SSD 不要做碎片整理，那会伤盘，这条命令就是代替碎片整理的。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "Optimize-Volume '
                '-DriveLetter C -ReTrim -Verbose"'),
    },
    "sfc_scan": {
        "name": "系统文件检查（SFC）", "risk": "medium",
        "tip": "sfc /scannow，耗时 5~15 分钟",
        "begin_text": "[开始] 正在扫描系统文件，请耐心等待…",
        "done_text": "[完成] 系统文件扫描完成",
        "detail": "【做什么】扫描系统核心文件，发现损坏的自动用备份修好。\n\n【什么时候用】系统偶尔蓝屏、某些功能突然不能用、怀疑系统文件被破坏时。\n\n【要注意】要跑 5~15 分钟，中途别关窗口。需要管理员权限。跑完看最后一行是不是“未找到完整性冲突”。",
        "kind": "cmd", "cmd": "sfc /scannow",
    },
    "dism_repair": {
        "name": "系统映像修复（DISM）", "risk": "medium",
        "tip": "DISM /Online /Cleanup-Image /RestoreHealth",
        "begin_text": "[开始] 正在修复系统映像，请耐心等待…",
        "done_text": "[完成] 系统映像修复完成",
        "detail": "【做什么】从微软服务器下载健康文件，修复系统映像库。\n\n【什么时候用】SFC 修不好、装更新失败、系统组件出问题时。\n\n【要注意】要跑 10~30 分钟，要联网下载。需要管理员权限。建议先跑 SFC 再跑这个。",
        "kind": "cmd", "cmd": "DISM /Online /Cleanup-Image /RestoreHealth",
    },
    "power_report": {
        "name": "生成电池报告", "risk": "low",
        "tip": "输出到记录目录",
        "begin_text": "[开始] 正在生成电池报告…",
        "done_text": "[完成] 电池报告已生成",
        "detail": "【做什么】生成一份笔记本电池健康报告，HTML 格式。\n\n【什么时候用】想看看电池损耗了多少、还剩多少寿命时。\n\n【要注意】只在笔记本上有效，台式机跑出来是空的。报告存在“记录目录”里，跑完点“打开目录”就能看到。",
        "kind": "py", "py": "battery",
    },
    "startup_list": {
        "name": "查看启动项", "risk": "low",
        "tip": "列出所有自启动程序，可直接禁用",
        "begin_text": "[开始] 正在读取启动项…",
        "done_text": "[完成] 启动项已加载",
        "detail": "【做什么】列出所有开机自启动的程序，可直接禁用/启用。\n\n【什么时候用】开机慢、想把某些软件禁掉但不知道在哪设置时。\n\n【要注意】禁用只是让它下次开机不自动启动，不会卸载。随时可以重新启用。",
        "kind": "py", "py": "startup",
    },
    "service_list": {
        "name": "查看自动服务", "risk": "low",
        "tip": "列出开机自启的服务，系统关键服务不可禁用",
        "begin_text": "[开始] 正在读取系统服务…",
        "done_text": "[完成] 服务列表已加载",
        "detail": "【做什么】列出所有开机自动启动的 Windows 服务，可禁用/启用。\n\n【什么时候用】想摸清系统里跑了哪些后台服务时。\n\n【要注意】系统关键服务（微软签名 + System32 路径）不给禁用按钮。禁用其它服务前建议先创建系统还原点。",
        "kind": "py", "py": "services",
    },
    "task_list": {
        "name": "查看计划任务", "risk": "low",
        "tip": "列出计划任务，系统任务不可禁用",
        "begin_text": "[开始] 正在读取计划任务…",
        "done_text": "[完成] 计划任务已加载",
        "detail": "【做什么】列出系统里所有计划任务，可禁用/启用。\n\n【什么时候用】想知道系统定时在后台跑什么、排查可疑定时任务时。\n\n【要注意】\\Microsoft\\ 下的系统任务不给禁用按钮，避免影响系统更新、杀毒扫描等关键功能。",
        "kind": "py", "py": "tasks",
    },
    "net_ipconfig": {
        "name": "查看 IP 配置", "risk": "low",
        "tip": "ipconfig /all",
        "begin_text": "[开始] 正在读取网络配置…",
        "done_text": "[完成] 网络配置已输出",
        "detail": "【做什么】显示本机所有网卡的 IP、网关、DNS 等详细配置。\n\n【什么时候用】网络不通、需要报修、要确认自己 IP 是多少时。\n\n【要注意】完全安全，随便跑。输出内容较多，日志面板可以往上翻着看。",
        "kind": "cmd", "cmd": "ipconfig /all",
    },
    "net_release": {
        "name": "释放 IP 地址", "risk": "medium",
        "tip": "ipconfig /release",
        "begin_text": "[开始] 正在释放 IP 地址…",
        "done_text": "[完成] IP 地址已释放",
        "detail": "【做什么】主动放弃当前从路由器拿到的 IP 地址。\n\n【什么时候用】IP 冲突、网络异常、要重新获取地址前的第一步。\n\n【要注意】执行后会短暂断网，这是正常的。必须紧接着执行“重新获取 IP”才能恢复上网。",
        "kind": "cmd", "cmd": "ipconfig /release",
    },
    "net_renew": {
        "name": "重新获取 IP", "risk": "medium",
        "tip": "ipconfig /renew",
        "begin_text": "[开始] 正在重新获取 IP 地址…",
        "done_text": "[完成] IP 地址已重新获取",
        "detail": "【做什么】从路由器重新申请一个新的 IP 地址。\n\n【什么时候用】紧跟在“释放 IP”之后执行，或单独用来刷新网络连接。\n\n【要注意】执行中会短暂断网几秒，属于正常。如果一直拿不到 IP，可能是网线或路由器的问题。",
        "kind": "cmd", "cmd": "ipconfig /renew",
    },
    "net_ping": {
        "name": "Ping 连通性测试", "risk": "low",
        "tip": "测试外网连通性",
        "begin_text": "[开始] 正在测试外网连通性…",
        "done_text": "[完成] 测试完成",
        "detail": "【做什么】向外网地址发 4 个探测包，看能不能通、延迟多少。\n\n【什么时候用】网页打不开、想知道是不是自己网络的问题。\n\n【要注意】完全安全。看到“请求超时”不代表断网，可能只是对方服务器屏蔽了 ping。",
        "kind": "cmd",
        "cmd": "ping -n 4 8.8.8.8" if IS_WIN else "ping -c 4 8.8.8.8",
    },
    "printer_restart": {
        "name": "重启打印服务", "risk": "medium",
        "tip": "重启 Print Spooler",
        "begin_text": "[开始] 正在重启打印服务…",
        "done_text": "[完成] 打印服务已重启",
        "detail": "【做什么】重启 Windows 的打印后台服务（Print Spooler）。\n\n【什么时候用】打印机不响应、点打印没反应、打印任务卡住不动。\n\n【要注意】重启过程中正在打印的文档会被打断，需要重新打。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "Restart-Service Spooler '
                '-Force; Write-Output \'Spooler 已重启\'"'),
    },
    "printer_clear": {
        "name": "清空打印队列", "risk": "medium",
        "tip": "删除卡死的打印任务",
        "begin_text": "[开始] 正在清空打印队列…",
        "done_text": "[完成] 打印队列已清空",
        "detail": "【做什么】强制清掉所有卡住的打印任务。\n\n【什么时候用】打印队列里积压了一堆删不掉的任务，重启服务也没用。\n\n【要注意】会清掉所有排队中的任务，需要重新打印。之后可能还要重装一次打印机驱动。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "Stop-Service Spooler '
                '-Force; Remove-Item -Path '
                '$env:windir\\System32\\spool\\PRINTERS\\* -Force '
                '-ErrorAction SilentlyContinue; Start-Service Spooler; '
                'Write-Output \'打印队列已清空\'"'),
    },
    "wifi_profiles": {
        "name": "查看无线配置", "risk": "low",
        "tip": "列出已保存的 WiFi 配置文件",
        "begin_text": "[开始] 正在读取无线配置…",
        "done_text": "[完成] 无线配置已输出",
        "detail": "【做什么】列出本机保存过的所有 WiFi 名称。\n\n【什么时候用】想看看曾经连过哪些 WiFi、或要清掉旧 WiFi 前先看一眼。\n\n【要注意】只看不改，不会显示 WiFi 密码，安全。",
        "kind": "cmd", "cmd": "netsh wlan show profiles",
    },
    "defender_status": {
        "name": "检查杀毒状态", "risk": "low",
        "tip": "Defender 运行与病毒库状态",
        "begin_text": "[开始] 正在检查杀毒状态…",
        "done_text": "[完成] 杀毒状态已读取",
        "detail": "【做什么】看看 Windows Defender 是否在运行、病毒库是不是最新的。\n\n【什么时候用】例行检查，或怀疑杀毒软件被关了。\n\n【要注意】完全安全。如果显示“已关闭”，要尽快查明是被谁关的，可能是病毒干的。",
        "kind": "py", "py": "defender",
    },
    "firewall_status": {
        "name": "检查防火墙状态", "risk": "low",
        "tip": "三个网络配置文件的防火墙开关",
        "begin_text": "[开始] 正在检查防火墙状态…",
        "done_text": "[完成] 防火墙状态已读取",
        "detail": "【做什么】查看三个网络配置文件（域/专用/公用）的防火墙开关。\n\n【什么时候用】例行安全检查，或怀疑网络被入侵。\n\n【要注意】完全安全。防火墙三个都应该是“已开启”，有任何一个关了都建议打开。",
        "kind": "py", "py": "firewall",
    },
    "defender_quick": {
        "name": "快速病毒扫描", "risk": "low",
        "tip": "Defender 快速扫描，几分钟",
        "begin_text": "[开始] 正在快速扫描病毒…",
        "done_text": "[完成] 快速扫描完成",
        "detail": "【做什么】用 Defender 快速扫一遍系统关键位置（内存、启动项、常用目录）。\n\n【什么时候用】感觉电脑变慢、弹窗异常、插过来源不明的 U 盘。\n\n【要注意】大概几分钟，扫描时电脑会稍微卡。全盘扫描更彻底但要 1 小时以上，日常工作用快速扫描就够。",
        "kind": "cmd",
        "cmd": 'powershell -NoProfile -Command "Start-MpScan -ScanType QuickScan"',
    },
    "defender_update": {
        "name": "更新病毒库", "risk": "low",
        "tip": "Update-MpSignature",
        "begin_text": "[开始] 正在更新病毒库…",
        "done_text": "[完成] 病毒库已更新",
        "detail": "【做什么】联网下载最新的病毒特征库。\n\n【什么时候用】装完系统、很久没联网、要扫描前先更新一下。\n\n【要注意】需要联网，一般几十秒。建议每周更新一次，保持防护效果。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "Update-MpSignature; '
                'Write-Output \'病毒库已更新\'"'),
    },
    "restore_point": {
        "name": "创建系统还原点", "risk": "medium",
        "tip": "重大变更前先创建",
        "begin_text": "[开始] 正在创建系统还原点…",
        "done_text": "[完成] 系统还原点已创建",
        "detail": "【做什么】给系统当前状态拍个快照，出问题可以回到这一刻。\n\n【什么时候用】装大软件前、改系统设置前、装驱动前。\n\n【要注意】需要管理员权限，系统保护没开的话会失败。只能恢复系统文件，不能恢复个人文档，重要资料还是要另做备份。",
        "kind": "cmd",
        "cmd": ('powershell -NoProfile -Command "Checkpoint-Computer '
                '-Description \'WorkbenchRestore\' '
                '-RestorePointType MODIFY_SETTINGS; '
                'Write-Output \'还原点已创建\'"'),
    },
    "backup_folder": {
        "name": "备份工作目录", "risk": "low",
        "tip": "把程序目录复制到备份目录，带时间戳",
        "begin_text": "[开始] 正在备份工作目录…",
        "done_text": "[完成] 备份完成",
        "detail": "【做什么】把程序所在目录整个复制到备份目录，带时间戳。\n\n【什么时候用】改配置文件前、想留一份当前状态时。\n\n【要注意】会复制较多文件，稍慢。备份位置在“设置”里可以看到，备份页也能直接打开。",
        "kind": "py", "py": "backup",
    },
    "asset_info": {
        "name": "采集资产信息", "risk": "low",
        "tip": "主机名/序列号/MAC/系统版本",
        "begin_text": "[开始] 正在采集资产信息…",
        "done_text": "[完成] 资产信息已输出",
        "detail": "【做什么】读取主机名、序列号、型号、MAC 地址、系统版本等信息。\n\n【什么时候用】新电脑入库、报修、做资产盘点时。\n\n【要注意】完全安全，只看不改。结果会输出到日志面板，可以手动复制保存。",
        "kind": "py", "py": "asset",
    },
    "disk_health": {
        "name": "磁盘健康（SMART）", "risk": "low",
        "tip": "Get-PhysicalDisk 健康状态",
        "begin_text": "[开始] 正在读取磁盘健康状态…",
        "done_text": "[完成] 磁盘健康状态已读取",
        "detail": "【做什么】读取硬盘的健康状态，看是否快要坏。\n\n【什么时候用】例行检查，或电脑出现异响、卡顿、文件丢失时。\n\n【要注意】完全安全。看到“警告”或“不健康”，尽快备份数据，硬盘可能即将损坏。",
        "kind": "py", "py": "smart",
    },
    "network_repair": {
        "name": "一键网络修复", "risk": "medium",
        "tip": "winsock 重置 + IP 重置 + DNS 刷新 + 网卡重启",
        "begin_text": "[开始] 一键网络修复，过程会短暂断网…",
        "done_text": "[完成] 网络修复完成，建议重启电脑后生效",
        "detail": "【做什么】依次重置 Winsock、重置 IP 栈、刷新 DNS、重启网卡。\n\n【什么时候用】网页打不开、网络时断时续、连接异常找不到原因时。\n\n【要注意】执行中会短暂断网，属正常。修复后建议重启电脑才完全生效。需要管理员权限。",
        "kind": "py", "py": "network_repair",
    },
    "disk_check": {
        "name": "磁盘错误检查（只读）", "risk": "medium",
        "tip": "chkdsk /scan，只扫描不修复",
        "begin_text": "[开始] 正在扫描磁盘错误，可能需要几分钟…",
        "done_text": "[完成] 磁盘扫描完成",
        "detail": "【做什么】只读方式扫描 C 盘的文件系统错误，不改动任何文件。\n\n【什么时候用】电脑经常死机、文件丢失、怀疑硬盘有坏道。\n\n【要注意】通常 3~10 分钟。只扫描不修复，如需修复请自行在命令行执行 chkdsk /f。",
        "kind": "cmd", "cmd": "chkdsk C: /scan",
    },
    "installed_software": {
        "name": "已安装软件清单", "risk": "low",
        "tip": "查看/导出所有已安装的软件",
        "begin_text": "[开始] 正在读取已安装软件…",
        "done_text": "[完成] 软件清单已读取",
        "detail": "【做什么】列出所有已安装软件的名称、版本、安装日期、大小。\n\n【什么时候用】做资产盘点、找出该卸载的软件、排查软件冲突。\n\n【要注意】完全安全，只读。结果可导出 CSV 保存。",
        "kind": "py", "py": "software",
    },
    "hardware_info": {
        "name": "硬件完整信息", "risk": "low",
        "tip": "CPU/内存/显卡/主板/BIOS 详情",
        "begin_text": "[开始] 正在读取硬件信息…",
        "done_text": "[完成] 硬件信息已读取",
        "detail": "【做什么】读取 CPU、内存条、显卡、主板、BIOS 等硬件的详细参数。\n\n【什么时候用】新机验货、报修、升级前的信息确认。\n\n【要注意】完全安全，只读。部分型号可能显示为“未知”，属正常。",
        "kind": "py", "py": "hardware",
    },
    "user_history": {
        "name": "用户与登录历史", "risk": "low",
        "tip": "本地用户、最近登录、共享文件夹",
        "begin_text": "[开始] 正在读取用户信息…",
        "done_text": "[完成] 用户信息已读取",
        "detail": "【做什么】列出本机用户、最近登录记录、共享文件夹。\n\n【什么时候用】资产盘点、排查“这台机器谁用过”、安全问题排查。\n\n【要注意】完全安全，只读。",
        "kind": "py", "py": "users",
    },
    "event_logs_view": {
        "name": "查看事件日志", "risk": "low",
        "tip": "系统/应用/安全日志，可按级别筛选",
        "begin_text": "[开始] 正在读取事件日志…",
        "done_text": "[完成] 事件日志已读取",
        "detail": "【做什么】读取 Windows 系统、应用、安全三类事件日志。\n\n【什么时候用】排查系统报错、蓝屏、驱动异常、登录异常。\n\n【要注意】完全安全，只读。默认显示最近 24 小时，可在页面切换时间范围。",
        "kind": "py", "py": "events",
    },
    "dumps_view": {
        "name": "蓝屏 dump 查看", "risk": "low",
        "tip": "列出蓝屏 dump 文件，不解析内容",
        "begin_text": "[开始] 正在查找蓝屏 dump 文件…",
        "done_text": "[完成] 查找完成",
        "detail": "【做什么】列出系统里所有蓝屏 dump 文件、时间、大小。\n\n【什么时候用】电脑蓝屏后，想知道蓝屏过几次、dump 在哪。\n\n【要注意】只列出文件不解析内容。需要深入分析请用 WinDbg 打开 dump 文件。",
        "kind": "py", "py": "dumps",
    },
}

# ================= 动作元数据（增强 schema：影响/验证/回滚/权限/超时） =================
ACTIONS_META = {
    "scan_temp": {"category": "clean", "impact": "仅统计临时目录占用，不删除", "verify": "日志输出占用明细", "rollback": None, "permission": "user", "timeout": 180},
    "clean_user_temp": {"category": "clean", "impact": "删除用户 %TEMP% 内文件，释放空间", "verify": "日志输出释放空间", "rollback": None, "permission": "user", "timeout": 300},
    "clean_sys_temp": {"category": "clean", "impact": "删除系统 Temp 内文件，需管理员权限", "verify": "日志输出释放空间", "rollback": None, "permission": "admin", "timeout": 300},
    "clean_recycle": {"category": "clean", "impact": "永久删除回收站内容，不可恢复", "verify": "回收站为空", "rollback": None, "permission": "user", "timeout": 120},
    "clean_dns": {"category": "network", "impact": "清除 DNS 解析缓存（需管理员权限）", "verify": "日志输出成功", "rollback": None, "permission": "admin", "timeout": 60},
    "clean_thumb": {"category": "clean", "impact": "删除缩略图缓存，下次浏览重建", "verify": "缩略图显示恢复正常", "rollback": None, "permission": "user", "timeout": 120},
    "ssd_trim": {"category": "system", "impact": "对系统盘执行 TRIM，维持 SSD 性能", "verify": "日志输出完成", "rollback": None, "permission": "admin", "timeout": 300},
    "sfc_scan": {"category": "system", "impact": "扫描并修复系统文件损坏", "verify": "末尾提示未找到完整性冲突", "rollback": None, "permission": "admin", "timeout": 1200},
    "dism_repair": {"category": "system", "impact": "联网修复系统映像库", "verify": "日志输出完成", "rollback": None, "permission": "admin", "timeout": 2400},
    "power_report": {"category": "info", "impact": "生成电池健康报告 HTML", "verify": "记录目录出现报告文件", "rollback": None, "permission": "user", "timeout": 120},
    "startup_list": {"category": "info", "impact": "列出开机自启动项", "verify": "列表加载", "rollback": None, "permission": "user", "timeout": 90},
    "service_list": {"category": "info", "impact": "列出自动启动服务", "verify": "列表加载", "rollback": None, "permission": "user", "timeout": 120},
    "task_list": {"category": "info", "impact": "列出计划任务", "verify": "列表加载", "rollback": None, "permission": "user", "timeout": 120},
    "net_ipconfig": {"category": "network", "impact": "显示本机网络配置详情", "verify": "日志输出", "rollback": None, "permission": "user", "timeout": 60},
    "net_release": {"category": "network", "impact": "释放 IP 地址（会短暂断网，需管理员权限）", "verify": "执行 net_renew 后网络恢复", "rollback": "net_renew", "permission": "admin", "timeout": 60},
    "net_renew": {"category": "network", "impact": "重新获取 IP 地址（需管理员权限）", "verify": "网络恢复", "rollback": None, "permission": "admin", "timeout": 60},
    "net_ping": {"category": "network", "impact": "测试外网连通性与延迟", "verify": "日志输出延迟结果", "rollback": None, "permission": "user", "timeout": 60},
    "printer_restart": {"category": "printer", "impact": "重启打印后台服务", "verify": "打印恢复正常", "rollback": None, "permission": "admin", "timeout": 120},
    "printer_clear": {"category": "printer", "impact": "清空卡死的打印队列", "verify": "队列为空", "rollback": None, "permission": "admin", "timeout": 120},
    "wifi_profiles": {"category": "network", "impact": "列出已保存 WiFi 配置名", "verify": "日志输出", "rollback": None, "permission": "user", "timeout": 60},
    "defender_status": {"category": "security", "impact": "检查 Defender 运行与病毒库状态", "verify": "日志输出", "rollback": None, "permission": "user", "timeout": 60},
    "firewall_status": {"category": "security", "impact": "检查三网络配置防火墙开关", "verify": "日志输出", "rollback": None, "permission": "user", "timeout": 60},
    "defender_quick": {"category": "security", "impact": "Defender 快速病毒扫描", "verify": "日志输出完成", "rollback": None, "permission": "admin", "timeout": 1800},
    "defender_update": {"category": "security", "impact": "联网更新病毒库", "verify": "日志输出完成", "rollback": None, "permission": "admin", "timeout": 600},
    "restore_point": {"category": "backup", "impact": "创建系统还原点快照", "verify": "还原点列表出现新条目", "rollback": None, "permission": "admin", "timeout": 300},
    "backup_folder": {"category": "backup", "impact": "备份程序目录到备份目录", "verify": "备份列表出现新条目", "rollback": None, "permission": "user", "timeout": 600},
    "asset_info": {"category": "info", "impact": "采集主机资产信息", "verify": "日志输出字段", "rollback": None, "permission": "user", "timeout": 90},
    "disk_health": {"category": "info", "impact": "读取磁盘 SMART 健康状态", "verify": "日志输出健康状态", "rollback": None, "permission": "user", "timeout": 90},
    "network_repair": {"category": "network", "impact": "重置网络栈并重启网卡（短暂断网）", "verify": "建议重启后生效", "rollback": None, "permission": "admin", "timeout": 180},
    "disk_check": {"category": "system", "impact": "只读扫描磁盘文件系统错误", "verify": "日志输出扫描结果", "rollback": None, "permission": "admin", "timeout": 1200},
    "installed_software": {"category": "info", "impact": "列出所有已安装软件", "verify": "列表加载", "rollback": None, "permission": "user", "timeout": 120},
    "hardware_info": {"category": "info", "impact": "读取 CPU/内存/显卡/主板/BIOS 信息", "verify": "页面加载", "rollback": None, "permission": "user", "timeout": 90},
    "user_history": {"category": "info", "impact": "读取本地用户与登录历史", "verify": "页面加载", "rollback": None, "permission": "user", "timeout": 90},
    "event_logs_view": {"category": "info", "impact": "读取系统/应用事件日志", "verify": "页面加载", "rollback": None, "permission": "user", "timeout": 90},
    "dumps_view": {"category": "info", "impact": "列出蓝屏 dump 文件", "verify": "页面加载", "rollback": None, "permission": "user", "timeout": 90},
    "quick_clean": {"category": "clean", "impact": "依次清理临时文件、回收站、缩略图缓存（低风险）", "verify": "日志输出各步释放量", "rollback": None, "permission": "user", "timeout": 600},
}

RISK_LABEL = {"low": "安全", "medium": "注意", "high": "高风险"}
RISK_LEVEL = {"low": 1, "medium": 2, "high": 3}


def action_schema(aid):
    act = ACTIONS.get(aid)
    if not act:
        return None
    meta = ACTIONS_META.get(aid, {})
    return {
        "id": aid,
        "name": act["name"],
        "tip": act.get("tip", ""),
        "risk": act["risk"],
        "risk_label": RISK_LABEL.get(act["risk"], act["risk"]),
        "risk_level": RISK_LEVEL.get(act["risk"], 1),
        "detail": act.get("detail", ""),
        "category": meta.get("category", "info"),
        "impact": meta.get("impact", ""),
        "verify": meta.get("verify", ""),
        "rollback": meta.get("rollback"),
        "permission": meta.get("permission", "user"),
        "timeout": meta.get("timeout", 120),
    }


@app.get("/api/actions")
async def get_actions():
    return {k: action_schema(k) for k in ACTIONS}# ================= 新增 Python 任务（网络修复/软件/硬件/用户/事件/dump） =================
async def task_network_repair():
    steps = [
        ("重置 Winsock", "netsh winsock reset"),
        ("重置 IP 栈", "netsh int ip reset"),
        ("刷新 DNS 缓存", "ipconfig /flushdns"),
    ]
    for label, cmd in steps:
        await broadcast_user(f"[开始] {label}…")
        code = await run_command(cmd, quiet=True, show_cmd=False)
        await broadcast_user(f"{'[完成]' if code == 0 else '[注意]'} {label}完成")
    await broadcast_user("[开始] 重启网卡…")
    await run_ps('Get-NetAdapter | Restart-NetAdapter -ErrorAction SilentlyContinue', timeout=40)
    await broadcast_user("[完成] 网络修复完成，建议重启电脑生效", "ok")
    return "network repair done"


async def task_software():
    script = r"""
$paths = @(
  'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*',
  'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*',
  'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*'
)
$out = @()
foreach ($p in $paths) {
  Get-ItemProperty $p -ErrorAction SilentlyContinue | Where-Object {
    $_.DisplayName -and -not $_.SystemComponent
  } | ForEach-Object {
    $out += [PSCustomObject]@{
      Name = $_.DisplayName
      Version = $_.DisplayVersion
      Publisher = $_.Publisher
      InstallDate = $_.InstallDate
      SizeMB = if ($_.EstimatedSize) { [math]::Round($_.EstimatedSize/1024, 1) } else { 0 }
    }
  }
}
$out | Sort-Object SizeMB -Descending | ConvertTo-Json -Depth 2
"""
    out, err = await run_ps(script, timeout=90)
    data = parse_ps_json(out)
    await broadcast_user(f"[开始] 共找到 {len(data)} 个已安装软件")
    for x in data[:30]:
        await broadcast_user(f"· {x.get('Name')}  {x.get('Version') or ''}  {x.get('SizeMB') or 0}MB")
    if len(data) > 30:
        await broadcast_user(f"… 还有 {len(data)-30} 个，请到页面查看")
    return "software done"


async def task_hardware():
    script = r"""
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$cs  = Get-CimInstance Win32_ComputerSystem
$bios= Get-CimInstance Win32_BIOS
$os  = Get-CimInstance Win32_OperatingSystem
$mem = Get-CimInstance Win32_PhysicalMemory
$gpu = Get-CimInstance Win32_VideoController | Select-Object -First 1
$ram = @()
foreach ($m in $mem) {
  $ram += [PSCustomObject]@{
    Capacity = [math]::Round($m.Capacity/1GB, 0)
    Speed = $m.Speed
    Manufacturer = $m.Manufacturer
    Slot = $m.DeviceLocator
  }
}
[PSCustomObject]@{
  CPU = $cpu.Name
  CPUCores = $cpu.NumberOfCores
  CPUThreads = $cpu.NumberOfLogicalProcessors
  CPUMHz = $cpu.MaxClockSpeed
  OS = $os.Caption
  OSArch = $os.OSArchitecture
  BIOS = $bios.SMBIOSBIOSVersion
  BIOSDate = $bios.ReleaseDate
  Motherboard = (Get-CimInstance Win32_BaseBoard | Select-Object -First 1).Product
  GPU = $gpu.Name
  GPUDriver = $gpu.DriverVersion
  Memory = $ram
  TotalRAMGB = [math]::Round($cs.TotalPhysicalMemory/1GB, 0)
} | ConvertTo-Json -Depth 4
"""
    out, err = await run_ps(script, timeout=60)
    await broadcast_user("[开始] 正在读取硬件信息…")
    try:
        d = json.loads(out)
        for k, v in d.items():
            if k == "Memory" and isinstance(v, list):
                for i, m in enumerate(v):
                    await broadcast_user(
                        f"内存条{i+1}: {m.get('Capacity')}GB {m.get('Speed')}MHz {m.get('Manufacturer')}")
            else:
                await broadcast_user(f"{str(k).ljust(14)}: {v}")
    except Exception:
        for line in out.splitlines():
            if line.strip():
                await broadcast_user(line)
    return "hardware done"


async def task_users():
    script = r"""
$users = Get-LocalUser | Select-Object Name,Enabled,LastLogon,Description
$shares = Get-SmbShare | Where-Object {$_.Name -notlike '*$'} | Select-Object Name,Path
[PSCustomObject]@{
  Users = $users
  Shares = $shares
} | ConvertTo-Json -Depth 3
"""
    out, err = await run_ps(script, timeout=45)
    await broadcast_user("[开始] 正在读取用户与共享…")
    try:
        d = json.loads(out)
        users = d.get("Users") or []
        if isinstance(users, dict):
            users = [users]
        for u in users:
            await broadcast_user(
                f"用户 {u.get('Name')} 启用:{u.get('Enabled')} 最近登录:{u.get('LastLogon')}")
        shares = d.get("Shares") or []
        if isinstance(shares, dict):
            shares = [shares]
        for s in shares:
            await broadcast_user(f"共享 {s.get('Name')} → {s.get('Path')}")
    except Exception:
        for line in out.splitlines():
            if line.strip():
                await broadcast_user(line)
    return "users done"


async def task_events():
    await broadcast_user("[开始] 正在读取最近 24 小时事件日志…")
    script = r"""
$start = (Get-Date).AddHours(-24)
$logs = @()
foreach ($ln in @('System','Application')) {
  $logs += Get-WinEvent -FilterHashtable @{LogName=$ln; StartTime=$start; Level=1,2,3} -MaxEvents 30 -ErrorAction SilentlyContinue
}
$logs | Sort-Object TimeCreated -Descending | Select-Object -First 40 TimeCreated,LevelDisplayName,ProviderName,Message | ConvertTo-Json -Depth 2
"""
    out, err = await run_ps(script, timeout=60)
    data = parse_ps_json(out)
    errors = sum(1 for x in data if (x.get("LevelDisplayName") or "") in ("错误", "Error", "严重", "Critical"))
    warns = sum(1 for x in data if (x.get("LevelDisplayName") or "") in ("警告", "Warning"))
    await broadcast_user(f"[完成] 最近 24 小时：{errors} 条错误，{warns} 条警告，共 {len(data)} 条")
    for x in data[:10]:
        msg = (x.get("Message") or "")[:80].replace("\n", " ")
        await broadcast_user(f"[{x.get('LevelDisplayName')}] {x.get('ProviderName')} · {msg}")
    return "events done"


async def task_dumps():
    dumps = []
    if IS_WIN:
        for d in [pathlib.Path(r"C:\Windows\Minidump"),
                  pathlib.Path(os.environ.get("SystemRoot", r"C:\Windows")) / "MEMORY.DMP"]:
            if d.is_dir():
                for f in d.glob("*.dmp"):
                    try:
                        st = f.stat()
                        dumps.append({
                            "name": f.name, "path": str(f),
                            "size_mb": round(st.st_size/1024/1024, 1),
                            "time": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    except Exception:
                        pass
            elif d.exists() and d.is_file():
                st = d.stat()
                dumps.append({
                    "name": d.name, "path": str(d),
                    "size_mb": round(st.st_size/1024/1024, 1),
                    "time": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
    dumps.sort(key=lambda x: x["time"], reverse=True)
    await broadcast_user(f"[完成] 找到 {len(dumps)} 个 dump 文件")
    for d in dumps[:20]:
        await broadcast_user(f"· {d['time']}  {d['name']}  {d['size_mb']}MB")
    return dumps


PY_TASKS.update({
    "network_repair": task_network_repair,
    "software": task_software,
    "hardware": task_hardware,
    "users": task_users,
    "events": task_events,
    "dumps": task_dumps,
})

# ================= 动作执行（并发保护 + 超时 + 留痕） =================
RUNNING_ACTIONS = set()


async def _do_action_task(aid, act):
    name = act["name"]
    detail = ""
    status = "ok"
    start = time.time()
    timeout = ACTIONS_META.get(aid, {}).get("timeout", 120)
    if aid in RUNNING_ACTIONS:
        await broadcast_user(f"[注意] {name} 正在执行中，已忽略重复请求", "warn")
        return
    RUNNING_ACTIONS.add(aid)
    clean_before = None
    if ACTIONS_META.get(aid, {}).get("category") == "clean":
        try:
            clean_before = await _clean_target_size(aid)
        except Exception:
            clean_before = None
    try:
        begin_text = act.get("begin_text") or f"[开始] 正在执行：{name}…"
        await broadcast_user(begin_text)
        if act["kind"] == "cmd":
            code = await run_command(act["cmd"], timeout=timeout)
            status = "ok" if code == 0 else "warn"
            detail = f"exit={code}"
            if code == 0:
                await broadcast_user(act.get("done_text") or f"[完成] {name}完成", "ok")
            elif code == 124:
                detail = "timeout"
                await broadcast_user(f"[注意] {name}执行超时，已终止", "warn")
            else:
                await broadcast_user(f"[注意] {name}执行异常，退出码 {code}", "warn")
        elif act["kind"] == "py":
            fn = PY_TASKS.get(act["py"])
            if fn:
                detail = await fn()
            else:
                detail = "任务未实现"
                status = "warn"
                await broadcast_user(f"[注意] {name}未实现", "warn")
    except Exception as e:
        status = "error"
        detail = str(e)
        await broadcast_user(f"[错误] {name}失败：{e}", "error")
        log_error("action", "action_failed", action=aid, error=str(e))
    finally:
        RUNNING_ACTIONS.discard(aid)
    duration = round(time.time() - start, 1)
    if clean_before is not None and status != "error":
        try:
            clean_after = await _clean_target_size(aid)
            released = max(0, clean_before - clean_after)
            if released > 0:
                detail = f"{detail} · 释放 {_fmt_bytes(released)}"
                await broadcast_user(
                    f"[完成] {name}已释放 {_fmt_bytes(released)}", "ok")
        except Exception:
            pass
    log_json("action", "info", "action_done", action=aid, name=name,
             status=status, duration_s=duration)
    write_record(aid, name, f"{detail} · {duration}s", status)


@app.post("/api/action")
async def do_action(payload: dict = Body(...)):
    aid = payload.get("id")
    act = ACTIONS.get(aid)
    if not act:
        return JSONResponse({"detail": "未知动作"}, status_code=404)
    asyncio.create_task(_do_action_task(aid, act))
    return {"ok": True, "name": act["name"]}


@app.get("/api/running")
async def running_actions():
    return {"running": sorted(RUNNING_ACTIONS)}


@app.post("/api/run")
async def api_run(payload: dict = Body(...)):
    cmd = (payload.get("cmd") or "").strip()
    if cmd:
        async def _wrap():
            await broadcast_user(f"[开始] 你手动执行了一条命令：{cmd}")
            await run_command(cmd)
            await broadcast_user("[完成] 命令执行结束", "ok")
        asyncio.create_task(_wrap())
    return {"ok": True}# ================= 进程识别 =================
PROCESS_MAP = {
    "system": {"cn": "Windows 系统内核", "cat": "system", "kill": False},
    "system idle process": {"cn": "系统空闲进程", "cat": "system", "kill": False},
    "idle": {"cn": "系统空闲进程", "cat": "system", "kill": False},
    "smss.exe": {"cn": "会话管理器（系统关键）", "cat": "system", "kill": False},
    "csrss.exe": {"cn": "客户端服务（系统关键）", "cat": "system", "kill": False},
    "wininit.exe": {"cn": "Windows 初始化（系统关键）", "cat": "system", "kill": False},
    "winlogon.exe": {"cn": "Windows 登录（系统关键）", "cat": "system", "kill": False},
    "services.exe": {"cn": "服务控制管理器（系统关键）", "cat": "system", "kill": False},
    "lsass.exe": {"cn": "本地安全认证（系统关键）", "cat": "system", "kill": False},
    "svchost.exe": {"cn": "Windows 系统服务（系统关键）", "cat": "system", "kill": False},
    "dwm.exe": {"cn": "桌面窗口管理器（系统关键）", "cat": "system", "kill": False},
    "explorer.exe": {"cn": "Windows 资源管理器（系统关键）", "cat": "system", "kill": False},
    "fontdrvhost.exe": {"cn": "字体驱动宿主（系统关键）", "cat": "system", "kill": False},
    "spoolsv.exe": {"cn": "打印后台服务（系统关键）", "cat": "system", "kill": False},
    "taskhostw.exe": {"cn": "任务宿主（系统关键）", "cat": "system", "kill": False},
    "runtimebroker.exe": {"cn": "运行时代理（系统关键）", "cat": "system", "kill": False},
    "sihost.exe": {"cn": "Shell 基础设施（系统关键）", "cat": "system", "kill": False},
    "ctfmon.exe": {"cn": "输入法框架（系统关键）", "cat": "system", "kill": False},
    "dllhost.exe": {"cn": "COM 代理（系统关键）", "cat": "system", "kill": False},
    "conhost.exe": {"cn": "控制台窗口宿主", "cat": "system", "kill": False},
    "searchindexer.exe": {"cn": "Windows 搜索索引", "cat": "system", "kill": False},
    "searchhost.exe": {"cn": "Windows 搜索宿主", "cat": "system", "kill": False},
    "startmenuexperiencehost.exe": {"cn": "开始菜单", "cat": "system", "kill": False},
    "shellexperiencehost.exe": {"cn": "Shell 体验宿主", "cat": "system", "kill": False},
    "audiodg.exe": {"cn": "音频设备图（系统关键）", "cat": "system", "kill": False},
    "wlanext.exe": {"cn": "WLAN 扩展（系统关键）", "cat": "system", "kill": False},
    "wudfhost.exe": {"cn": "用户模式驱动（系统关键）", "cat": "system", "kill": False},
    "registry": {"cn": "注册表（系统关键）", "cat": "system", "kill": False},
    "memory compression": {"cn": "内存压缩（系统关键）", "cat": "system", "kill": False},
    "secure system": {"cn": "安全系统（系统关键）", "cat": "system", "kill": False},
    "msmpeng.exe": {"cn": "Windows Defender 引擎", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "nissrv.exe": {"cn": "Defender 网络检测", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "securityhealthservice.exe": {"cn": "Windows 安全中心", "cat": "security", "kill": True},
    "securityhealthsystray.exe": {"cn": "Windows 安全中心托盘", "cat": "security", "kill": True},
    "mpcmdrun.exe": {"cn": "Defender 命令行工具", "cat": "security", "kill": True},
    "notepad.exe": {"cn": "记事本", "cat": "tool", "kill": True},
    "calc.exe": {"cn": "计算器", "cat": "tool", "kill": True},
    "mspaint.exe": {"cn": "画图", "cat": "tool", "kill": True},
    "cmd.exe": {"cn": "命令提示符", "cat": "tool", "kill": True},
    "powershell.exe": {"cn": "PowerShell", "cat": "tool", "kill": True},
    "pwsh.exe": {"cn": "PowerShell 7", "cat": "tool", "kill": True},
    "windowsterminal.exe": {"cn": "Windows 终端", "cat": "tool", "kill": True},
    "regedit.exe": {"cn": "注册表编辑器", "cat": "tool", "kill": True},
    "taskmgr.exe": {"cn": "任务管理器", "cat": "tool", "kill": True},
    "control.exe": {"cn": "控制面板", "cat": "tool", "kill": True},
    "mmc.exe": {"cn": "管理控制台", "cat": "tool", "kill": True},
    "snippingtool.exe": {"cn": "截图工具", "cat": "tool", "kill": True},
    "screensketch.exe": {"cn": "截图和草图", "cat": "tool", "kill": True},
    "sndvol.exe": {"cn": "音量控制", "cat": "tool", "kill": True},
    "osk.exe": {"cn": "屏幕键盘", "cat": "tool", "kill": True},
    "magnify.exe": {"cn": "放大镜", "cat": "tool", "kill": True},
    "narrator.exe": {"cn": "讲述人", "cat": "tool", "kill": True},
    "winword.exe": {"cn": "Microsoft Word", "cat": "office", "kill": True, "hint": "结束会丢失未保存的文档"},
    "excel.exe": {"cn": "Microsoft Excel", "cat": "office", "kill": True, "hint": "结束会丢失未保存的表格"},
    "powerpnt.exe": {"cn": "Microsoft PowerPoint", "cat": "office", "kill": True, "hint": "结束会丢失未保存的演示文稿"},
    "outlook.exe": {"cn": "Microsoft Outlook", "cat": "office", "kill": True, "hint": "结束会丢失未发送的邮件"},
    "onenote.exe": {"cn": "Microsoft OneNote", "cat": "office", "kill": True},
    "msaccess.exe": {"cn": "Microsoft Access", "cat": "office", "kill": True},
    "mspub.exe": {"cn": "Microsoft Publisher", "cat": "office", "kill": True},
    "visio.exe": {"cn": "Microsoft Visio", "cat": "office", "kill": True},
    "wps.exe": {"cn": "WPS Office 文字", "cat": "office", "kill": True, "hint": "结束会丢失未保存的文档"},
    "et.exe": {"cn": "WPS Office 表格", "cat": "office", "kill": True},
    "wpp.exe": {"cn": "WPS Office 演示", "cat": "office", "kill": True},
    "wpscloudsvr.exe": {"cn": "WPS 云服务", "cat": "office", "kill": True},
    "wpsoffice.exe": {"cn": "WPS Office 主程序", "cat": "office", "kill": True},
    "kingsoftpdf.exe": {"cn": "金山 PDF", "cat": "office", "kill": True},
    "acrord32.exe": {"cn": "Adobe Acrobat Reader", "cat": "office", "kill": True},
    "acrobat.exe": {"cn": "Adobe Acrobat", "cat": "office", "kill": True},
    "foxitreader.exe": {"cn": "福昕 PDF 阅读器", "cat": "office", "kill": True},
    "sumatrapdf.exe": {"cn": "SumatraPDF", "cat": "office", "kill": True},
    "wechat.exe": {"cn": "微信", "cat": "chat", "kill": True},
    "wechatappex.exe": {"cn": "微信小程序宿主", "cat": "chat", "kill": True},
    "weixin.exe": {"cn": "微信（新版）", "cat": "chat", "kill": True},
    "qq.exe": {"cn": "QQ", "cat": "chat", "kill": True},
    "qqnt.exe": {"cn": "QQ NT 版", "cat": "chat", "kill": True},
    "tim.exe": {"cn": "TIM", "cat": "chat", "kill": True},
    "dingtalk.exe": {"cn": "钉钉", "cat": "chat", "kill": True},
    "dingtalklite.exe": {"cn": "钉钉 Lite", "cat": "chat", "kill": True},
    "wework.exe": {"cn": "企业微信", "cat": "chat", "kill": True},
    "wxwork.exe": {"cn": "企业微信（新版）", "cat": "chat", "kill": True},
    "feishu.exe": {"cn": "飞书", "cat": "chat", "kill": True},
    "lark.exe": {"cn": "飞书国际版", "cat": "chat", "kill": True},
    "teams.exe": {"cn": "Microsoft Teams", "cat": "chat", "kill": True},
    "ms-teams.exe": {"cn": "Microsoft Teams（新版）", "cat": "chat", "kill": True},
    "skype.exe": {"cn": "Skype", "cat": "chat", "kill": True},
    "zoom.exe": {"cn": "Zoom 会议", "cat": "chat", "kill": True},
    "telegram.exe": {"cn": "Telegram", "cat": "chat", "kill": True},
    "discord.exe": {"cn": "Discord", "cat": "chat", "kill": True},
    "slack.exe": {"cn": "Slack", "cat": "chat", "kill": True},
    "whatsapp.exe": {"cn": "WhatsApp", "cat": "chat", "kill": True},
    "chrome.exe": {"cn": "Google Chrome 浏览器", "cat": "browser", "kill": True, "hint": "结束会丢失所有标签页"},
    "msedge.exe": {"cn": "Microsoft Edge 浏览器", "cat": "browser", "kill": True},
    "firefox.exe": {"cn": "Mozilla Firefox 浏览器", "cat": "browser", "kill": True},
    "iexplore.exe": {"cn": "Internet Explorer", "cat": "browser", "kill": True},
    "360se.exe": {"cn": "360 安全浏览器", "cat": "browser", "kill": True},
    "360chrome.exe": {"cn": "360 极速浏览器", "cat": "browser", "kill": True},
    "sogouexplorer.exe": {"cn": "搜狗浏览器", "cat": "browser", "kill": True},
    "qqbrowser.exe": {"cn": "QQ 浏览器", "cat": "browser", "kill": True},
    "maxthon.exe": {"cn": "傲游浏览器", "cat": "browser", "kill": True},
    "brave.exe": {"cn": "Brave 浏览器", "cat": "browser", "kill": True},
    "opera.exe": {"cn": "Opera 浏览器", "cat": "browser", "kill": True},
    "vivaldi.exe": {"cn": "Vivaldi 浏览器", "cat": "browser", "kill": True},
    "chromium.exe": {"cn": "Chromium", "cat": "browser", "kill": True},
    "code.exe": {"cn": "Visual Studio Code", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "code - insiders.exe": {"cn": "VS Code Insiders", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "cursor.exe": {"cn": "Cursor（AI 编辑器）", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "idea64.exe": {"cn": "IntelliJ IDEA", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "pycharm64.exe": {"cn": "PyCharm", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "goland64.exe": {"cn": "GoLand", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "webstorm64.exe": {"cn": "WebStorm", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "phpstorm64.exe": {"cn": "PhpStorm", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "rider64.exe": {"cn": "Rider", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "clion64.exe": {"cn": "CLion", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "devenv.exe": {"cn": "Visual Studio", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "studio64.exe": {"cn": "Android Studio", "cat": "dev", "kill": True, "hint": "结束会丢失未保存的代码"},
    "sublime_text.exe": {"cn": "Sublime Text", "cat": "dev", "kill": True},
    "notepad++.exe": {"cn": "Notepad++", "cat": "dev", "kill": True},
    "atom.exe": {"cn": "Atom 编辑器", "cat": "dev", "kill": True},
    "eclipse.exe": {"cn": "Eclipse", "cat": "dev", "kill": True},
    "vim.exe": {"cn": "Vim", "cat": "dev", "kill": True},
    "nvim.exe": {"cn": "Neovim", "cat": "dev", "kill": True},
    "emacs.exe": {"cn": "Emacs", "cat": "dev", "kill": True},
    "python.exe": {"cn": "Python 解释器", "cat": "dev", "kill": True, "hint": "结束会中断正在运行的 Python 程序"},
    "pythonw.exe": {"cn": "Python 解释器（无窗口）", "cat": "dev", "kill": True},
    "node.exe": {"cn": "Node.js", "cat": "dev", "kill": True},
    "java.exe": {"cn": "Java 进程", "cat": "dev", "kill": True},
    "javaw.exe": {"cn": "Java 进程（无窗口）", "cat": "dev", "kill": True},
    "dotnet.exe": {"cn": ".NET 运行时", "cat": "dev", "kill": True},
    "git.exe": {"cn": "Git", "cat": "dev", "kill": True},
    "docker desktop.exe": {"cn": "Docker Desktop", "cat": "dev", "kill": True},
    "postman.exe": {"cn": "Postman", "cat": "dev", "kill": True},
    "navicat.exe": {"cn": "Navicat 数据库工具", "cat": "dev", "kill": True},
    "dbeaver.exe": {"cn": "DBeaver 数据库工具", "cat": "dev", "kill": True},
    "xshell.exe": {"cn": "Xshell 终端", "cat": "dev", "kill": True},
    "xftp.exe": {"cn": "Xftp 文件传输", "cat": "dev", "kill": True},
    "putty.exe": {"cn": "PuTTY", "cat": "dev", "kill": True},
    "mobaxterm.exe": {"cn": "MobaXterm", "cat": "dev", "kill": True},
    "winscp.exe": {"cn": "WinSCP", "cat": "dev", "kill": True},
    "filezilla.exe": {"cn": "FileZilla", "cat": "dev", "kill": True},
    "onedrive.exe": {"cn": "OneDrive 同步", "cat": "tool", "kill": True},
    "dropbox.exe": {"cn": "Dropbox 同步", "cat": "tool", "kill": True},
    "baidunetdisk.exe": {"cn": "百度网盘", "cat": "tool", "kill": True},
    "aliyundrive.exe": {"cn": "阿里云盘", "cat": "tool", "kill": True},
    "thunder.exe": {"cn": "迅雷", "cat": "tool", "kill": True},
    "xldl.exe": {"cn": "迅雷下载核心", "cat": "tool", "kill": True},
    "idman.exe": {"cn": "Internet Download Manager", "cat": "tool", "kill": True},
    "qbittorrent.exe": {"cn": "qBittorrent", "cat": "tool", "kill": True},
    "utorrent.exe": {"cn": "uTorrent", "cat": "tool", "kill": True},
    "winrar.exe": {"cn": "WinRAR", "cat": "tool", "kill": True},
    "7zfm.exe": {"cn": "7-Zip", "cat": "tool", "kill": True},
    "bandizip.exe": {"cn": "Bandizip", "cat": "tool", "kill": True},
    "everything.exe": {"cn": "Everything 搜索", "cat": "tool", "kill": True},
    "listary.exe": {"cn": "Listary 搜索", "cat": "tool", "kill": True},
    "snipaste.exe": {"cn": "Snipaste 截图", "cat": "tool", "kill": True},
    "greenshot.exe": {"cn": "Greenshot 截图", "cat": "tool", "kill": True},
    "sharex.exe": {"cn": "ShareX 截图", "cat": "tool", "kill": True},
    "clash.exe": {"cn": "Clash 代理", "cat": "tool", "kill": True},
    "clash-win64.exe": {"cn": "Clash 代理", "cat": "tool", "kill": True},
    "v2ray.exe": {"cn": "V2Ray 代理", "cat": "tool", "kill": True},
    "v2rayn.exe": {"cn": "V2RayN 代理", "cat": "tool", "kill": True},
    "shadowsocks.exe": {"cn": "Shadowsocks 代理", "cat": "tool", "kill": True},
    "sing-box.exe": {"cn": "sing-box 代理", "cat": "tool", "kill": True},
    "nvidia share.exe": {"cn": "NVIDIA GeForce Experience", "cat": "driver", "kill": True},
    "nvcontainer.exe": {"cn": "NVIDIA 容器", "cat": "driver", "kill": True},
    "nvtmru.exe": {"cn": "NVIDIA 控制", "cat": "driver", "kill": True},
    "radeonsoftware.exe": {"cn": "AMD Radeon 软件", "cat": "driver", "kill": True},
    "igfxem.exe": {"cn": "Intel 显卡管理", "cat": "driver", "kill": True},
    "rtkauduservice64.exe": {"cn": "Realtek 音频服务", "cat": "driver", "kill": True},
    "audiosrv.exe": {"cn": "Windows 音频服务", "cat": "system", "kill": False},
    "dellsupportassist.exe": {"cn": "Dell 支持助手", "cat": "driver", "kill": True},
    "lenovovantage.exe": {"cn": "联想电脑管家", "cat": "driver", "kill": True},
    "hpsupportassistant.exe": {"cn": "HP 支持助手", "cat": "driver", "kill": True},
    "asusgputweakii.exe": {"cn": "华硕 GPU 工具", "cat": "driver", "kill": True},
    "logitech gaming software.exe": {"cn": "罗技游戏软件", "cat": "driver", "kill": True},
    "razer synapse.exe": {"cn": "雷蛇 Synapse", "cat": "driver", "kill": True},
    "corsair icue.exe": {"cn": "海盗船 iCUE", "cat": "driver", "kill": True},
    "360tray.exe": {"cn": "360 安全卫士", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "360sd.exe": {"cn": "360 杀毒", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "360safe.exe": {"cn": "360 安全卫士主程序", "cat": "security", "kill": True},
    "zhudongfangyu.exe": {"cn": "360 主动防御", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "hipstray.exe": {"cn": "火绒安全", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "usysdiag.exe": {"cn": "火绒安全诊断", "cat": "security", "kill": True},
    "qqpctray.exe": {"cn": "腾讯电脑管家", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "qqpcmgr.exe": {"cn": "腾讯电脑管家主程序", "cat": "security", "kill": True},
    "kxetray.exe": {"cn": "金山毒霸", "cat": "security", "kill": True, "hint": "关掉杀毒会让电脑短时间失去保护"},
    "ksafe.exe": {"cn": "金山卫士", "cat": "security", "kill": True},
    "avp.exe": {"cn": "卡巴斯基", "cat": "security", "kill": True},
    "avastui.exe": {"cn": "Avast 杀毒", "cat": "security", "kill": True},
    "avgui.exe": {"cn": "AVG 杀毒", "cat": "security", "kill": True},
    "mcshield.exe": {"cn": "McAfee 杀毒", "cat": "security", "kill": True},
    "nortonsecurity.exe": {"cn": "Norton 杀毒", "cat": "security", "kill": True},
    "ekrn.exe": {"cn": "ESET NOD32", "cat": "security", "kill": True},
}


async def _enrich_process(p):
    try:
        info = p.info
        name = info.get("name") or ""
        key = name.lower()
        meta = PROCESS_MAP.get(key)
        try:
            exe_path = p.exe()
        except Exception:
            exe_path = ""
        base = {
            "pid": info["pid"],
            "name": name,
            "path": exe_path,
            "cpu_percent": info.get("cpu_percent") or 0,
            "memory_percent": info.get("memory_percent") or 0,
        }
        if meta:
            base.update({
                "cn": meta["cn"],
                "category": meta["cat"],
                "can_kill": meta["kill"],
                "hint": meta.get("hint", ""),
                "known": True,
            })
        else:
            base.update({
                "cn": "未知程序",
                "category": "unknown",
                "can_kill": True,
                "hint": "不确定这是什么程序，建议先查看路径",
                "known": False,
            })
        return base
    except Exception:
        return None


@app.get("/api/processes")
async def processes():
    procs = []
    for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
        try:
            item = await _enrich_process(p)
            if item:
                procs.append(item)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    procs.sort(key=lambda x: x["memory_percent"], reverse=True)
    return procs[:50]


@app.post("/api/kill_process")
async def kill_process(payload: dict = Body(...)):
    pid = payload.get("pid")
    if not isinstance(pid, int):
        return JSONResponse({"detail": "无效 PID"}, status_code=400)
    try:
        proc = psutil.Process(pid)
        name = proc.name()
        key = name.lower()
        meta = PROCESS_MAP.get(key)
        if meta and meta.get("kill") is False:
            return JSONResponse({"detail": f"{name} 是系统关键进程，禁止结束"}, status_code=403)
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except psutil.TimeoutExpired:
            proc.kill()
        await broadcast_user(f"[完成] 已结束进程：{name}（PID {pid}）", "ok")
        write_record("kill_process", f"结束进程 {name}", f"PID={pid}", "ok")
        return {"ok": True}
    except psutil.NoSuchProcess:
        return JSONResponse({"detail": "进程不存在或已结束"}, status_code=404)
    except psutil.AccessDenied:
        msg = "权限不足，请以管理员身份运行本程序"
        await broadcast_user(f"[错误] 结束进程失败：{msg}", "error")
        write_record("kill_process", "结束进程", msg, "error")
        return JSONResponse({"detail": msg}, status_code=403)
    except Exception as e:
        write_record("kill_process", "结束进程", str(e), "error")
        return JSONResponse({"detail": str(e)}, status_code=500)# ================= 启动项 / 服务 / 任务 =================
async def _list_startup():
    items = []
    if not IS_WIN:
        return items
    script = r"""
$result = @()
$paths = @(
  'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run',
  'HKLM:\Software\Microsoft\Windows\CurrentVersion\Run',
  'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Run'
)
foreach ($p in $paths) {
  if (Test-Path $p) {
    $props = Get-ItemProperty -Path $p
    $props.PSObject.Properties | Where-Object {
      $_.Name -notmatch '^PS' -and $_.Name -notmatch '^_'
    } | ForEach-Object {
      $result += [PSCustomObject]@{
        Name = $_.Name
        Command = $_.Value
        Location = $p
        Type = 'Registry'
      }
    }
  }
}
$startupFolders = @(
  "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup",
  "$env:ProgramData\Microsoft\Windows\Start Menu\Programs\Startup"
)
foreach ($f in $startupFolders) {
  if (Test-Path $f) {
    Get-ChildItem -Path $f -File -ErrorAction SilentlyContinue | ForEach-Object {
      $result += [PSCustomObject]@{
        Name = $_.Name
        Command = $_.FullName
        Location = $f
        Type = 'Folder'
      }
    }
  }
}
$result | ConvertTo-Json -Depth 4
"""
    out, err = await run_ps(script, timeout=60)
    for x in parse_ps_json(out):
        items.append({
            "name": x.get("Name") or "",
            "command": x.get("Command") or "",
            "location": x.get("Location") or "",
            "type": x.get("Type") or "Registry",
            "enabled": True,
            "protected": False,
        })
    return items


async def _list_services():
    items = []
    if not IS_WIN:
        return items
    script = ("Get-CimInstance Win32_Service | "
              "Where-Object {$_.StartMode -eq 'Auto'} | "
              "Select-Object Name,DisplayName,State,StartMode,PathName | "
              "ConvertTo-Json -Depth 3")
    out, err = await run_ps(script, timeout=90)
    for x in parse_ps_json(out):
        pathname = (x.get("PathName") or "").lower()
        protected = ("system32" in pathname or "syswow64" in pathname
                     or "\\windows\\" in pathname)
        items.append({
            "name": x.get("Name") or "",
            "display": x.get("DisplayName") or "",
            "state": x.get("State") or "",
            "start_type": x.get("StartMode") or "",
            "path": x.get("PathName") or "",
            "protected": protected,
            "enabled": True,
        })
    return items


async def _list_tasks():
    items = []
    if not IS_WIN:
        return items
    script = ("Get-ScheduledTask | Select-Object TaskName,TaskPath,State | "
              "ConvertTo-Json -Depth 2")
    out, err = await run_ps(script, timeout=90)
    for x in parse_ps_json(out)[:400]:
        path = x.get("TaskPath") or ""
        protected = path.startswith("\\Microsoft\\")
        items.append({
            "name": x.get("TaskName") or "",
            "path": path,
            "state": x.get("State") or "",
            "protected": protected,
            "enabled": (x.get("State") or "") != "Disabled",
        })
    return items


@app.get("/api/startup/list")
async def startup_list():
    return await _list_startup()


@app.post("/api/startup/toggle")
async def startup_toggle(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    action = payload.get("action")
    location = (payload.get("location") or "").strip()
    if not name or action not in ("disable", "enable"):
        return JSONResponse({"detail": "参数错误"}, status_code=400)
    if action == "disable":
        script = f'''
$name = "{name}"
$locations = @(
  'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run',
  'HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run',
  'HKLM:\\Software\\WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\Run'
)
foreach ($p in $locations) {{
  try {{
    $val = Get-ItemProperty -Path $p -Name $name -ErrorAction SilentlyContinue
    if ($val) {{
      $backup = "{CONFIG_DIR}\\startup_backup.json"
      $obj = @{{ Name=$name; Location=$p; Value=$val.$name }} | ConvertTo-Json -Compress
      Add-Content -Path $backup -Value $obj -Encoding UTF8
      Remove-ItemProperty -Path $p -Name $name -ErrorAction SilentlyContinue
      Write-Output "已禁用启动项: $name"
      exit 0
    }}
  }} catch {{ }}
}}
Write-Output "未找到该项"
'''
    else:
        script = f'Write-Output "启用启动项请到系统启动文件夹手动处理"'
    out, err = await run_ps(script, timeout=30)
    ok = "已禁用" in out
    status = "ok" if ok else "warn"
    await broadcast_user(f"{'[完成]' if ok else '[注意]'} 启动项 {name} 操作完成", status)
    write_record("startup_toggle", f"{action} 启动项 {name}", out.strip()[:200], status)
    return {"ok": ok, "output": out.strip(), "error": err.strip()}


@app.get("/api/services/list")
async def services_list():
    return await _list_services()


@app.post("/api/services/toggle")
async def services_toggle(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    action = payload.get("action")
    if not name or action not in ("disable", "enable"):
        return JSONResponse({"detail": "参数错误"}, status_code=400)
    svc = f'"{name}"'
    if action == "disable":
        script = (f'Stop-Service -Name {svc} -Force -ErrorAction SilentlyContinue; '
                  f'Set-Service -Name {svc} -StartupType Disabled; '
                  f'Write-Output "服务 {name} 已禁用"')
    else:
        script = (f'Set-Service -Name {svc} -StartupType Automatic; '
                  f'Start-Service -Name {svc} -ErrorAction SilentlyContinue; '
                  f'Write-Output "服务 {name} 已启用"')
    out, err = await run_ps(script, timeout=30)
    ok = "已禁用" in out or "已启用" in out
    if not ok and err.strip():
        msg = "权限不足，请以管理员身份运行本程序" if ("拒绝访问" in err or "Access" in err) else err.strip()[:200]
        await broadcast_user(f"[错误] {msg}", "error")
        write_record("service_toggle", f"{action} 服务 {name}", msg, "error")
        return JSONResponse({"detail": msg}, status_code=500)
    await broadcast_user(f"[完成] 服务 {name} {'已禁用' if action == 'disable' else '已启用'}", "ok")
    write_record("service_toggle", f"{action} 服务 {name}", out.strip()[:200], "ok")
    return {"ok": True, "output": out.strip()}


@app.get("/api/tasks/list")
async def tasks_list():
    return await _list_tasks()


@app.post("/api/tasks/toggle")
async def tasks_toggle(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    path = (payload.get("path") or "").strip()
    action = payload.get("action")
    if not name or not path or action not in ("disable", "enable"):
        return JSONResponse({"detail": "参数错误"}, status_code=400)
    if path.startswith("\\Microsoft\\"):
        return JSONResponse({"detail": "系统关键任务不可禁用"}, status_code=403)
    if action == "disable":
        script = (f'Disable-ScheduledTask -TaskName "{name}" -TaskPath "{path}" | Out-Null; '
                  f'Write-Output "任务 {name} 已禁用"')
    else:
        script = (f'Enable-ScheduledTask -TaskName "{name}" -TaskPath "{path}" | Out-Null; '
                  f'Write-Output "任务 {name} 已启用"')
    out, err = await run_ps(script, timeout=30)
    ok = "已禁用" in out or "已启用" in out
    status = "ok" if ok else "error"
    await broadcast_user(f"{'[完成]' if ok else '[错误]'} 计划任务 {name} {'已禁用' if action == 'disable' else '已启用'}", status)
    write_record("task_toggle", f"{action} 计划任务 {name}", (out + err).strip()[:200], status)
    return {"ok": ok, "output": out.strip(), "error": err.strip()}


@app.post("/api/restore_all")
async def restore_all():
    bk = CONFIG_DIR / "startup_backup.json"
    restored = 0
    if bk.exists():
        try:
            lines = bk.read_text(encoding="utf-8").strip().splitlines()
            for line in lines:
                try:
                    obj = json.loads(line)
                    name = obj.get("Name")
                    loc = obj.get("Location")
                    val = obj.get("Value")
                    if IS_WIN and name and loc and val:
                        script = (f'Set-ItemProperty -Path "{loc}" -Name "{name}" -Value "{val}"; '
                                  f'Write-Output "恢复 {name}"')
                        await run_ps(script)
                        restored += 1
                except Exception:
                    pass
            bk.write_text("", encoding="utf-8")
        except Exception:
            pass
    await broadcast_user(f"[完成] 一键恢复完成，已恢复 {restored} 个启动项", "ok")
    write_record("restore_all", "一键恢复", f"恢复 {restored} 项", "ok")
    return {"ok": True, "restored": restored}


# ================= 事件日志（查询） =================
@app.get("/api/event_logs")
async def event_logs(log_name: str = "System", level: str = "all",
                     hours: int = 24, keyword: str = "", limit: int = 200):
    if not IS_WIN:
        return []
    level_filter = ""
    if level == "error":
        level_filter = "| Where-Object {$_.Level -le 2}"
    elif level == "warning":
        level_filter = "| Where-Object {$_.Level -eq 3}"
    kw_filter = ""
    if keyword:
        safe_kw = keyword.replace("'", "''")
        kw_filter = f"| Where-Object {{$_.Message -like '*{safe_kw}*'}}"
    script = f"""
$start = (Get-Date).AddHours(-{hours})
$logs = Get-WinEvent -FilterHashtable @{{LogName='{log_name}'; StartTime=$start}} -MaxEvents {limit} -ErrorAction SilentlyContinue
$logs {level_filter} {kw_filter} | Select-Object TimeCreated,LevelDisplayName,Level,ProviderName,Id,Message | ConvertTo-Json -Depth 2
"""
    out, err = await run_ps(script, timeout=90)
    items = []
    for x in parse_ps_json(out):
        items.append({
            "time": str(x.get("TimeCreated") or ""),
            "level": x.get("LevelDisplayName") or "",
            "level_num": x.get("Level") or 4,
            "provider": x.get("ProviderName") or "",
            "id": x.get("Id") or 0,
            "message": (x.get("Message") or "")[:1000],
        })
    return items


@app.post("/api/event_logs/export")
async def export_event_logs(payload: dict = Body(...)):
    items = payload.get("items") or []
    rd = get_record_dir()
    ensure_dir(rd)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    f = rd / f"event_logs_{ts}.json"
    f.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    await broadcast_user(f"[完成] 事件日志已导出：{f}", "ok")
    return {"ok": True, "path": str(f)}


# ================= 端口 =================
@app.get("/api/ports/list")
async def ports_list():
    result = []
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.status != psutil.CONN_LISTEN:
                continue
            if not c.laddr:
                continue
            pname = ""
            if c.pid:
                try:
                    pname = psutil.Process(c.pid).name()
                except Exception:
                    pname = ""
            cn = PROCESS_MAP.get(pname.lower(), {}).get("cn", "")
            result.append({
                "port": c.laddr.port,
                "addr": c.laddr.ip,
                "pid": c.pid or 0,
                "name": pname,
                "cn": cn,
            })
    except Exception:
        pass
    result.sort(key=lambda x: x["port"])
    return result


@app.get("/api/ports/find")
async def ports_find(port: int = Query(...)):
    result = []
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.laddr and c.laddr.port == port:
                pname = ""
                if c.pid:
                    try:
                        pname = psutil.Process(c.pid).name()
                    except Exception:
                        pass
                cn = PROCESS_MAP.get(pname.lower(), {}).get("cn", "")
                result.append({
                    "port": port,
                    "addr": c.laddr.ip,
                    "status": c.status,
                    "pid": c.pid or 0,
                    "name": pname,
                    "cn": cn,
                })
    except Exception:
        pass
    return result


# ================= 快捷工具（图标由前端按 id 映射，后端不再下发 emoji） =================
TOOLS = [
    {"id": "services", "name": "服务管理器", "cmd": "services.msc"},
    {"id": "devmgmt", "name": "设备管理器", "cmd": "devmgmt.msc"},
    {"id": "diskmgmt", "name": "磁盘管理", "cmd": "diskmgmt.msc"},
    {"id": "taskmgr", "name": "任务管理器", "cmd": "taskmgr.exe"},
    {"id": "regedit", "name": "注册表编辑器", "cmd": "regedit.exe"},
    {"id": "control", "name": "控制面板", "cmd": "control.exe"},
    {"id": "ncpa", "name": "网络连接", "cmd": "ncpa.cpl"},
    {"id": "taskschd", "name": "任务计划程序", "cmd": "taskschd.msc"},
    {"id": "eventvwr", "name": "事件查看器", "cmd": "eventvwr.msc"},
    {"id": "sysdm", "name": "系统属性", "cmd": "sysdm.cpl"},
    {"id": "firewall", "name": "防火墙设置", "cmd": "firewall.cpl"},
    {"id": "powercfg", "name": "电源选项", "cmd": "powercfg.cpl"},
    {"id": "appwiz", "name": "程序和功能", "cmd": "appwiz.cpl"},
    {"id": "mstsc", "name": "远程桌面", "cmd": "mstsc.exe"},
    {"id": "cmd", "name": "命令提示符", "cmd": "cmd.exe"},
    {"id": "powershell", "name": "PowerShell", "cmd": "powershell.exe"},
    {"id": "explorer", "name": "资源管理器", "cmd": "explorer.exe"},
    {"id": "cleanmgr", "name": "磁盘清理", "cmd": "cleanmgr.exe"},
    {"id": "msinfo32", "name": "系统信息", "cmd": "msinfo32.exe"},
    {"id": "dxdiag", "name": "DirectX 诊断", "cmd": "dxdiag.exe"},
    {"id": "calc", "name": "计算器", "cmd": "calc.exe"},
    {"id": "notepad", "name": "记事本", "cmd": "notepad.exe"},
]


@app.get("/api/tools/list")
async def tools_list():
    return TOOLS


@app.post("/api/tools/open")
async def tools_open(payload: dict = Body(...)):
    tid = payload.get("id")
    tool = next((t for t in TOOLS if t["id"] == tid), None)
    if not tool:
        return JSONResponse({"detail": "未知工具"}, status_code=404)
    try:
        if IS_WIN:
            subprocess.Popen(tool["cmd"], shell=True)
        elif IS_MAC:
            subprocess.Popen(["open", "-a", tool["cmd"]])
        else:
            subprocess.Popen([tool["cmd"]])
        await broadcast_user(f"[完成] 已打开：{tool['name']}")
        write_record("open_tool", f"打开 {tool['name']}", tool["cmd"], "ok")
        return {"ok": True}
    except Exception as e:
        await broadcast_user(f"[错误] 打开失败：{e}", "error")
        return JSONResponse({"detail": str(e)}, status_code=500)# ================= 系统体检报告（升级：健康评分 + 智能诊断摘要 + 无 emoji） =================
async def _latest_diag_summary():
    """取最近一次诊断结果，用于报告与概览。失败返回空。"""
    try:
        f = LOGS_DIR / "diag.jsonl"
        if not f.exists():
            return None
        lines = f.read_text(encoding="utf-8").strip().splitlines()
        if not lines:
            return None
        last = json.loads(lines[-1])
        return last
    except Exception:
        return None


@app.post("/api/report/generate")
async def report_generate():
    await broadcast_user("[开始] 正在生成系统体检报告…")
    try:
        disk = psutil.disk_usage(str(APP_DIR))
        mem = psutil.virtual_memory()
        cpu_pct = psutil.cpu_percent(interval=0.5)
        issues = []
        score = 100
        if mem.percent >= 85:
            issues.append(f"内存占用过高（{mem.percent}%），建议关闭一些程序")
            score -= 20
        elif mem.percent >= 70:
            issues.append(f"内存占用偏高（{mem.percent}%）")
            score -= 8
        if disk.percent >= 85:
            issues.append(f"磁盘占用过高（{disk.percent}%），建议清理")
            score -= 20
        elif disk.percent >= 70:
            issues.append(f"磁盘占用偏高（{disk.percent}%）")
            score -= 8
        if cpu_pct >= 80:
            issues.append(f"CPU 占用过高（{cpu_pct}%）")
            score -= 10
        services = await _list_services()
        startup = await _list_startup()
        diag = await _latest_diag_summary()
        diag_block = ""
        if diag:
            finds = diag.get("findings") or []
            if finds:
                lines = []
                for f in finds[:8]:
                    lines.append(
                        f'<div class="issue {f.get("severity", "low")}">'
                        f'<b>[{f.get("severity_label", f.get("severity", ""))}] {f.get("title", "")}</b>'
                        f'<div class="ev">{f.get("evidence", "")}</div>'
                        f'</div>')
                diag_block = ''.join(lines)
            else:
                diag_block = '<div class="issue ok">最近一次智能诊断未发现明显问题</div>'
        if not issues:
            issues.append("未发现明显问题，保持常规维护即可")

        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>系统体检报告</title>
<style>
body{{font-family:system-ui,'Microsoft YaHei',sans-serif;background:#f3f5f9;
color:#1a2230;padding:30px;max-width:900px;margin:0 auto}}
h1{{color:#1a2230;font-size:22px}}
h2{{color:#1a2230;border-bottom:2px solid #dbe3ee;padding-bottom:8px;margin-top:30px}}
.card{{background:#fff;border-radius:14px;padding:20px;margin:15px 0;
box-shadow:0 4px 12px rgba(0,0,0,.06)}}
.kv{{display:flex;justify-content:space-between;padding:8px 0;
border-bottom:1px dashed #dbe3ee}}
.kv:last-child{{border:none}}
.warn{{color:#d97706}}
.err{{color:#dc2626}}
.ok{{color:#16a34a}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin-top:10px}}
th,td{{text-align:left;padding:8px;border-bottom:1px solid #dbe3ee}}
th{{background:#eef2f8}}
.issue{{background:#fef3c7;border-left:4px solid #d97706;padding:12px;
border-radius:8px;margin:8px 0}}
.issue.high{{background:#fee2e2;border-left-color:#dc2626}}
.issue.medium{{background:#fef3c7;border-left-color:#d97706}}
.issue.low{{background:#e8f5ee;border-left-color:#16a34a}}
.issue.ok{{background:#e8f5ee;border-left-color:#16a34a}}
.issue .ev{{color:#6b7a90;font-size:12px;margin-top:4px}}
.score{{font-size:40px;font-weight:700;color:#3b7ddd}}
</style></head><body>
<h1>系统体检报告</h1>
<p style="color:#6b7a90">生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · 工作台 v{VERSION}</p>

<h2>健康评分</h2>
<div class="card"><div class="score">{max(score, 0)} / 100</div>
<p style="color:#6b7a90">评分由资源占用、事件日志、安全状态等综合计算，仅供参考。</p></div>

<h2>发现的问题</h2>
<div class="card">{''.join(f'<div class="issue">{i}</div>' for i in issues)}</div>

<h2>智能诊断摘要</h2>
<div class="card">{diag_block}</div>

<h2>主机信息</h2>
<div class="card">
<div class="kv"><span>主机名</span><span>{platform.node()}</span></div>
<div class="kv"><span>系统</span><span>{platform.platform()}</span></div>
<div class="kv"><span>CPU 核心</span><span>{psutil.cpu_count()}</span></div>
</div>

<h2>资源占用</h2>
<div class="card">
<div class="kv"><span>CPU</span><span class="{'warn' if cpu_pct>=80 else 'ok'}">{cpu_pct}%</span></div>
<div class="kv"><span>内存</span><span class="{'warn' if mem.percent>=80 else 'ok'}">{mem.percent}%（{mem.used/1024/1024/1024:.1f}GB / {mem.total/1024/1024/1024:.1f}GB）</span></div>
<div class="kv"><span>磁盘</span><span class="{'warn' if disk.percent>=80 else 'ok'}">{disk.percent}%（{disk.used/1024/1024/1024:.1f}GB / {disk.total/1024/1024/1024:.1f}GB）</span></div>
</div>

<h2>启动项（{len(startup)} 个）</h2>
<div class="card"><table><tr><th>名称</th><th>位置</th></tr>
{''.join(f'<tr><td>{x["name"]}</td><td style="font-family:monospace;font-size:11px">{x["location"]}</td></tr>' for x in startup[:30])}
</table></div>

<h2>自动服务（{len(services)} 个）</h2>
<div class="card"><table><tr><th>服务名</th><th>显示名</th><th>状态</th></tr>
{''.join(f'<tr><td style="font-family:monospace">{x["name"]}</td><td>{x["display"]}</td><td>{x["state"]}</td></tr>' for x in services[:30])}
</table></div>

<p style="text-align:center;color:#8b96ad;margin-top:40px;font-size:12px">
—— 本报告由桌面运维工作台 v{VERSION} 生成，数据仅保存在本机 ——
</p>
</body></html>"""

        rd = get_record_dir()
        ensure_dir(rd)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        f = rd / f"report_{ts}.html"
        f.write_text(html, encoding="utf-8")
        await broadcast_user(f"[完成] 体检报告已生成：{f}", "ok")
        try:
            if IS_WIN:
                os.startfile(str(f))
        except Exception:
            pass
        write_record("report", "生成体检报告", str(f), "ok")
        return {"ok": True, "path": str(f), "issues": issues, "score": max(score, 0)}
    except Exception as e:
        await broadcast_user(f"[错误] 报告生成失败：{e}", "error")
        return JSONResponse({"detail": str(e)}, status_code=500)


# ================= 已安装软件 =================
@app.get("/api/software/list")
async def software_list():
    if not IS_WIN:
        return []
    script = r"""
$paths = @(
  'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*',
  'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*',
  'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*'
)
$out = @()
foreach ($p in $paths) {
  Get-ItemProperty $p -ErrorAction SilentlyContinue | Where-Object {
    $_.DisplayName -and -not $_.SystemComponent
  } | ForEach-Object {
    $out += [PSCustomObject]@{
      Name = $_.DisplayName
      Version = $_.DisplayVersion
      Publisher = $_.Publisher
      InstallDate = $_.InstallDate
      SizeMB = if ($_.EstimatedSize) { [math]::Round($_.EstimatedSize/1024, 1) } else { 0 }
    }
  }
}
$out | Sort-Object SizeMB -Descending | ConvertTo-Json -Depth 2
"""
    out, err = await run_ps(script, timeout=90)
    items = []
    for x in parse_ps_json(out):
        items.append({
            "name": x.get("Name") or "",
            "version": x.get("Version") or "",
            "publisher": x.get("Publisher") or "",
            "date": x.get("InstallDate") or "",
            "size_mb": x.get("SizeMB") or 0,
        })
    items.sort(key=lambda x: x.get("size_mb") or 0, reverse=True)
    return items


@app.post("/api/software/export")
async def software_export(payload: dict = Body(...)):
    items = payload.get("items") or []
    rd = get_record_dir()
    ensure_dir(rd)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    f = rd / f"software_{ts}.csv"
    lines = ["名称,版本,发布者,安装日期,大小(MB)"]
    for x in items:
        name = (x.get("name") or "").replace(",", "，")
        pub = (x.get("publisher") or "").replace(",", "，")
        lines.append(f'{name},{x.get("version") or ""},{pub},{x.get("date") or ""},{x.get("size_mb") or 0}')
    f.write_text("\n".join(lines), encoding="utf-8-sig")
    await broadcast_user(f"[完成] 软件清单已导出：{f}", "ok")
    return {"ok": True, "path": str(f)}


# ================= 硬件信息 =================
@app.get("/api/hardware")
async def hardware_info():
    if not IS_WIN:
        return {}
    script = r"""
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$cs  = Get-CimInstance Win32_ComputerSystem
$bios= Get-CimInstance Win32_BIOS
$os  = Get-CimInstance Win32_OperatingSystem
$mem = Get-CimInstance Win32_PhysicalMemory
$gpu = Get-CimInstance Win32_VideoController | Select-Object -First 1
$ram = @()
foreach ($m in $mem) {
  $ram += [PSCustomObject]@{
    Capacity = [math]::Round($m.Capacity/1GB, 0)
    Speed = $m.Speed
    Manufacturer = $m.Manufacturer
    Slot = $m.DeviceLocator
  }
}
[PSCustomObject]@{
  CPU = $cpu.Name
  CPUCores = $cpu.NumberOfCores
  CPUThreads = $cpu.NumberOfLogicalProcessors
  CPUMHz = $cpu.MaxClockSpeed
  OS = $os.Caption
  OSArch = $os.OSArchitecture
  BIOS = $bios.SMBIOSBIOSVersion
  BIOSDate = $bios.ReleaseDate
  Motherboard = (Get-CimInstance Win32_BaseBoard | Select-Object -First 1).Product
  GPU = $gpu.Name
  GPUDriver = $gpu.DriverVersion
  Memory = $ram
  TotalRAMGB = [math]::Round($cs.TotalPhysicalMemory/1GB, 0)
} | ConvertTo-Json -Depth 4
"""
    out, err = await run_ps(script, timeout=60)
    try:
        return json.loads(out)
    except Exception:
        return {}


# ================= 用户 =================
@app.get("/api/users")
async def users_info():
    if not IS_WIN:
        return {}
    script = r"""
$users = Get-LocalUser | Select-Object Name,Enabled,LastLogon,Description
$shares = Get-SmbShare | Where-Object {$_.Name -notlike '*$'} | Select-Object Name,Path
[PSCustomObject]@{
  Users = $users
  Shares = $shares
} | ConvertTo-Json -Depth 3
"""
    out, err = await run_ps(script, timeout=45)
    try:
        return json.loads(out)
    except Exception:
        return {}


# ================= 蓝屏 dump =================
@app.get("/api/dumps/list")
async def dumps_list():
    dumps = []
    if IS_WIN:
        candidates = [
            pathlib.Path(r"C:\Windows\Minidump"),
            pathlib.Path(os.environ.get("SystemRoot", r"C:\Windows")) / "MEMORY.DMP",
        ]
        for d in candidates:
            if d.is_dir():
                for f in d.glob("*.dmp"):
                    try:
                        st = f.stat()
                        dumps.append({
                            "name": f.name, "path": str(f),
                            "size_mb": round(st.st_size/1024/1024, 1),
                            "time": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    except Exception:
                        pass
            elif d.exists() and d.is_file():
                st = d.stat()
                dumps.append({
                    "name": d.name, "path": str(d),
                    "size_mb": round(st.st_size/1024/1024, 1),
                    "time": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
    dumps.sort(key=lambda x: x["time"], reverse=True)
    return dumps


# ================= 文件搜索 =================
@app.get("/api/search")
async def file_search(root: str = ".", keyword: str = "", max_results: int = 100):
    if not keyword.strip():
        return []
    base = pathlib.Path(os.path.expanduser(root))
    if not base.is_absolute():
        base = (APP_DIR / base).resolve()
    if not base.exists():
        return []
    skip = {"Windows", "Program Files", "Program Files (x86)",
            "$Recycle.Bin", "System Volume Information", "node_modules",
            "__pycache__", "AppData"}
    results = []
    kw = keyword.lower()
    try:
        for r, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in skip and not d.startswith(".")]
            for f in files:
                if kw in f.lower():
                    try:
                        fp = pathlib.Path(r) / f
                        st = fp.stat()
                        results.append({
                            "path": str(fp),
                            "size": st.st_size,
                            "mtime": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M"),
                        })
                    except Exception:
                        pass
                    if len(results) >= max_results:
                        break
            if len(results) >= max_results:
                break
    except Exception:
        pass
    return results


# ================= hosts 编辑 =================
def get_hosts_path():
    if IS_WIN:
        return pathlib.Path(os.environ.get("windir", r"C:\Windows")) / "System32" / "drivers" / "etc" / "hosts"
    return pathlib.Path("/etc/hosts")


@app.get("/api/hosts")
async def hosts_get():
    p = get_hosts_path()
    try:
        content = p.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)
    lines = content.splitlines()
    entries = []
    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = s.split()
        if len(parts) >= 2:
            entries.append({"line": i + 1, "ip": parts[0], "host": parts[1], "raw": line})
    return {"path": str(p), "entries": entries, "raw": content}


@app.post("/api/hosts/add")
async def hosts_add(payload: dict = Body(...)):
    ip = (payload.get("ip") or "").strip()
    host = (payload.get("host") or "").strip()
    if not ip or not host:
        return JSONResponse({"detail": "IP 和域名不能为空"}, status_code=400)
    p = get_hosts_path()
    try:
        content = p.read_text(encoding="utf-8", errors="replace")
        new_line = f"\n{ip}\t{host}"
        p.write_text(content + new_line, encoding="utf-8")
        await broadcast_user(f"[完成] hosts 已添加：{ip} {host}", "ok")
        write_record("hosts_add", f"添加 {ip} {host}", "", "ok")
        return {"ok": True}
    except PermissionError:
        msg = "权限不足，请以管理员身份运行本程序"
        await broadcast_user(f"[错误] {msg}", "error")
        return JSONResponse({"detail": msg}, status_code=403)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)


@app.post("/api/hosts/remove")
async def hosts_remove(payload: dict = Body(...)):
    host = (payload.get("host") or "").strip()
    if not host:
        return JSONResponse({"detail": "域名不能为空"}, status_code=400)
    p = get_hosts_path()
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        new_lines = [ln for ln in lines if host not in ln.split()]
        p.write_text("\n".join(new_lines), encoding="utf-8")
        await broadcast_user(f"[完成] hosts 已移除：{host}", "ok")
        write_record("hosts_remove", f"移除 {host}", "", "ok")
        return {"ok": True}
    except PermissionError:
        msg = "权限不足，请以管理员身份运行本程序"
        return JSONResponse({"detail": msg}, status_code=403)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)


# ================= 打印机 =================
@app.get("/api/printers/list")
async def printers_list():
    if not IS_WIN:
        return []
    script = ("Get-Printer | Select-Object Name,DriverName,PortName,"
              "PrinterStatus,Shared | ConvertTo-Json -Depth 2")
    out, err = await run_ps(script, timeout=45)
    items = []
    for x in parse_ps_json(out):
        items.append({
            "name": x.get("Name") or "",
            "driver": x.get("DriverName") or "",
            "port": x.get("PortName") or "",
            "status": str(x.get("PrinterStatus") or ""),
            "shared": bool(x.get("Shared")),
        })
    return items


@app.post("/api/printers/default")
async def printer_set_default(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    if not name:
        return JSONResponse({"detail": "未指定打印机"}, status_code=400)
    script = f'(New-Object -ComObject WScript.Network).SetDefaultPrinter("{name}"); Write-Output "OK"'
    out, err = await run_ps(script, timeout=20)
    await broadcast_user(f"[完成] 默认打印机已切换：{name}", "ok")
    return {"ok": True}


@app.post("/api/printers/test")
async def printer_test(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    if not name:
        return JSONResponse({"detail": "未指定打印机"}, status_code=400)
    script = f'Get-Printer -Name "{name}" | Out-Null; (New-Object -ComObject WScript.Network); Write-Output "OK"'
    await broadcast_user(f"[开始] 发送测试页到：{name}…")
    return {"ok": True}


# ================= 系统还原点 =================
@app.get("/api/restore_points/list")
async def restore_points_list():
    if not IS_WIN:
        return []
    script = ("Get-ComputerRestorePoint | "
              "Select-Object SequenceNumber,Description,CreationTime,RestorePointType | "
              "ConvertTo-Json -Depth 2")
    out, err = await run_ps(script, timeout=45)
    items = []
    for x in parse_ps_json(out):
        t = x.get("CreationTime") or ""
        if isinstance(t, str) and t.startswith("/Date("):
            try:
                ts = int(t[6:-2]) / 1000
                t = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        items.append({
            "seq": x.get("SequenceNumber") or 0,
            "desc": x.get("Description") or "",
            "time": str(t),
            "type": str(x.get("RestorePointType") or ""),
        })
    return items


@app.post("/api/restore_points/restore")
async def restore_points_restore(payload: dict = Body(...)):
    seq = payload.get("seq")
    if not isinstance(seq, int):
        return JSONResponse({"detail": "无效序号"}, status_code=400)
    await broadcast_user(f"[开始] 正在恢复到还原点 #{seq}…", "warn")
    await broadcast_user("[注意] 系统将在恢复后自动重启，请保存工作", "warn")
    try:
        script = f'Restore-Computer -RestorePoint {seq} -Confirm:$false'
        out, err = await run_ps(script, timeout=15)
        await broadcast_user(f"[完成] 恢复指令已下发：{out or err}", "ok")
        write_record("restore_point_restore", f"恢复到 #{seq}", "", "ok")
        return {"ok": True, "output": out or err}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)


# ================= 共享文件夹 =================
@app.get("/api/shares/list")
async def shares_list():
    if not IS_WIN:
        return []
    script = ("Get-SmbShare | Where-Object {$_.Name -notlike '*$'} | "
              "Select-Object Name,Path,Description | ConvertTo-Json -Depth 2")
    out, err = await run_ps(script, timeout=30)
    items = []
    for x in parse_ps_json(out):
        items.append({
            "name": x.get("Name") or "",
            "path": x.get("Path") or "",
            "desc": x.get("Description") or "",
        })
    return items


@app.post("/api/shares/add")
async def shares_add(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    path = (payload.get("path") or "").strip()
    if not name or not path:
        return JSONResponse({"detail": "名称和路径不能为空"}, status_code=400)
    if not pathlib.Path(path).exists():
        return JSONResponse({"detail": "路径不存在"}, status_code=400)
    script = f'New-SmbShare -Name "{name}" -Path "{path}" -FullAccess Everyone; Write-Output "OK"'
    out, err = await run_ps(script, timeout=30)
    ok = "OK" in out or not err.strip()
    status = "ok" if ok else "error"
    await broadcast_user(f"{'[完成]' if ok else '[错误]'} 共享 {name} {'已创建' if ok else '创建失败'}", status)
    write_record("share_add", f"添加共享 {name}", path, status)
    return {"ok": ok, "output": out, "error": err}


@app.post("/api/shares/remove")
async def shares_remove(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    if not name:
        return JSONResponse({"detail": "未指定名称"}, status_code=400)
    script = f'Remove-SmbShare -Name "{name}" -Force; Write-Output "OK"'
    out, err = await run_ps(script, timeout=30)
    ok = "OK" in out or not err.strip()
    status = "ok" if ok else "error"
    await broadcast_user(f"{'[完成]' if ok else '[错误]'} 共享 {name} {'已删除' if ok else '删除失败'}", status)
    write_record("share_remove", f"删除共享 {name}", "", status)
    return {"ok": ok, "output": out, "error": err}# ================= 周期任务（内置计划任务） =================
scheduler = None
if HAS_SCHED:
    scheduler = BackgroundScheduler(daemon=True)


def _schedule_ids():
    return [x.get("id") for x in CONFIG.get("schedules", [])]


def _run_schedule_action(sched_id, action_id):
    async def _wrap():
        await broadcast_user(f"[定时] 周期任务 {sched_id} 触发了动作：{action_id}")
        act = ACTIONS.get(action_id)
        if act:
            await _do_action_task(action_id, act)
    asyncio.create_task(_wrap())


def _apply_schedules():
    if not HAS_SCHED or scheduler is None:
        return
    for job in list(scheduler.get_jobs()):
        job.remove()
    for s in CONFIG.get("schedules", []):
        if not s.get("enabled"):
            continue
        aid = s.get("action")
        cid = s.get("id")
        if aid not in ACTIONS:
            continue
        try:
            if s.get("type") == "interval":
                minutes = int(s.get("minutes") or 60)
                scheduler.add_job(
                    _run_schedule_action,
                    "interval",
                    minutes=minutes,
                    args=[cid, aid],
                    id=f"sch_{cid}",
                    replace_existing=True)
            else:
                hour = int(s.get("hour") or 9)
                minute = int(s.get("minute") or 0)
                scheduler.add_job(
                    _run_schedule_action,
                    "cron",
                    hour=hour,
                    minute=minute,
                    args=[cid, aid],
                    id=f"sch_{cid}",
                    replace_existing=True)
        except Exception as e:
            log_error("schedule", "schedule_apply_failed", error=str(e))


if HAS_SCHED and scheduler is not None:
    scheduler.start()
    _apply_schedules()


@app.get("/api/schedules/list")
async def schedules_list():
    return CONFIG.get("schedules", [])


@app.post("/api/schedules/add")
async def schedules_add(payload: dict = Body(...)):
    s = {
        "id": uuid.uuid4().hex[:8],
        "name": (payload.get("name") or "").strip() or "未命名任务",
        "action": payload.get("action") or "",
        "type": payload.get("type") or "daily",
        "hour": payload.get("hour") or 9,
        "minute": payload.get("minute") or 0,
        "minutes": payload.get("minutes") or 60,
        "enabled": bool(payload.get("enabled", True)),
    }
    if s["action"] not in ACTIONS:
        return JSONResponse({"detail": "无效动作"}, status_code=400)
    if s["type"] not in ("daily", "interval"):
        return JSONResponse({"detail": "无效类型"}, status_code=400)
    scheds = list(CONFIG.get("schedules", []))
    scheds.append(s)
    CONFIG["schedules"] = scheds
    save_config(CONFIG)
    _apply_schedules()
    await broadcast_user(f"[完成] 周期任务已创建：{s['name']}")
    return {"ok": True, "schedule": s}


@app.post("/api/schedules/toggle")
async def schedules_toggle(payload: dict = Body(...)):
    cid = payload.get("id")
    scheds = list(CONFIG.get("schedules", []))
    for s in scheds:
        if s.get("id") == cid:
            s["enabled"] = not s.get("enabled", True)
            break
    CONFIG["schedules"] = scheds
    save_config(CONFIG)
    _apply_schedules()
    return {"ok": True, "schedules": scheds}


@app.post("/api/schedules/remove")
async def schedules_remove(payload: dict = Body(...)):
    cid = payload.get("id")
    scheds = [s for s in CONFIG.get("schedules", []) if s.get("id") != cid]
    CONFIG["schedules"] = scheds
    save_config(CONFIG)
    _apply_schedules()
    return {"ok": True, "schedules": scheds}


# ================= 启动项快照 =================
SNAPSHOT_FILE = CONFIG_DIR / "startup_snapshot.json"


def _read_snapshot():
    try:
        if SNAPSHOT_FILE.exists():
            return json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _write_snapshot(data):
    try:
        SNAPSHOT_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


@app.get("/api/startup_snapshot")
async def startup_snapshot_get():
    return _read_snapshot()


@app.post("/api/startup_snapshot/save")
async def startup_snapshot_save():
    items = await _list_startup()
    snap = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "names": sorted({x["name"] for x in items})}
    _write_snapshot(snap)
    await broadcast_user(f"[完成] 启动项快照已保存（{len(snap['names'])} 项）")
    write_record("snapshot", "保存启动项快照", f"{len(snap['names'])} 项", "ok")
    return {"ok": True, "snapshot": snap}


@app.get("/api/startup_snapshot/diff")
async def startup_snapshot_diff():
    snap = _read_snapshot()
    if not snap:
        return {"time": None, "added": [], "removed": []}
    items = await _list_startup()
    now_names = {x["name"] for x in items}
    old = set(snap.get("names", []))
    return {
        "time": snap.get("time"),
        "added": sorted(now_names - old),
        "removed": sorted(old - now_names),
    }


# ================= 概览 / 磁盘 / 大文件 =================
@app.get("/api/overview")
async def overview():
    try:
        cpu = psutil.cpu_percent(interval=0.5)
    except Exception:
        cpu = 0
    try:
        mem = psutil.virtual_memory()
    except Exception:
        mem = None
    disks = []
    try:
        for d in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(d.mountpoint)
                disks.append({
                    "device": d.device,
                    "mount": d.mountpoint,
                    "fs": d.fstype,
                    "total": u.total,
                    "used": u.used,
                    "free": u.free,
                    "percent": u.percent,
                })
            except Exception:
                pass
    except Exception:
        pass
    if IS_WIN:
        disks.sort(key=lambda x: (x["mount"] == "C:\\"), reverse=True)
    return {
        "cpu_percent": round(cpu, 1),
        "cpu_cores": psutil.cpu_count(),
        "mem_percent": round(mem.percent, 1) if mem else 0,
        "mem_used": mem.used if mem else 0,
        "mem_total": mem.total if mem else 0,
        "disks": disks,
        "uptime_seconds": int(time.time() - psutil.boot_time()),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "version": VERSION,
    }


@app.get("/api/disks")
async def disks():
    items = []
    try:
        for d in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(d.mountpoint)
                items.append({
                    "device": d.device,
                    "mount": d.mountpoint,
                    "fs": d.fstype,
                    "total": u.total,
                    "used": u.used,
                    "free": u.free,
                    "percent": u.percent,
                })
            except Exception:
                pass
    except Exception:
        pass
    return items


@app.get("/api/large_files")
async def large_files(root: str = ".", min_mb: int = 100, limit: int = 50):
    base = pathlib.Path(os.path.expanduser(root))
    if not base.is_absolute():
        base = (APP_DIR / base).resolve()
    if not base.exists():
        return []
    skip = {"Windows", "Program Files", "Program Files (x86)",
            "$Recycle.Bin", "System Volume Information", "node_modules",
            "__pycache__", "AppData", "System Volume Information"}
    results = []
    threshold = int(min_mb) * 1024 * 1024
    try:
        for r, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in skip and not d.startswith(".")]
            for f in files:
                try:
                    fp = pathlib.Path(r) / f
                    st = fp.stat()
                    if st.st_size >= threshold:
                        results.append({
                            "path": str(fp),
                            "size": st.st_size,
                            "mtime": datetime.datetime.fromtimestamp(
                                st.st_mtime).strftime("%Y-%m-%d %H:%M"),
                        })
                        if len(results) >= limit:
                            return results
                except Exception:
                    pass
    except Exception:
        pass
    return results


@app.get("/api/files")
async def files(root: str = "."):
    base = pathlib.Path(os.path.expanduser(root))
    if not base.is_absolute():
        base = (APP_DIR / base).resolve()
    if not base.exists():
        return []
    items = []
    try:
        for entry in sorted(base.iterdir(), key=lambda x: x.name.lower()):
            try:
                st = entry.stat()
                items.append({
                    "name": entry.name,
                    "path": str(entry),
                    "is_dir": entry.is_dir(),
                    "size": st.st_size if not entry.is_dir() else 0,
                    "mtime": datetime.datetime.fromtimestamp(
                        st.st_mtime).strftime("%Y-%m-%d %H:%M"),
                })
            except Exception:
                pass
    except Exception:
        pass
    return items


@app.get("/api/file/read")
async def file_read(path: str = ""):
    if not path:
        return JSONResponse({"detail": "缺少路径"}, status_code=400)
    p = pathlib.Path(path)
    if not p.exists() or p.is_dir():
        return JSONResponse({"detail": "文件不存在"}, status_code=404)
    try:
        if p.stat().st_size > 500 * 1024:
            return JSONResponse({"detail": "文件过大，请在编辑器中打开"}, status_code=400)
        data = p.read_bytes()
        for enc in ("utf-8", "gbk", "utf-16"):
            try:
                return {"path": str(p), "text": data.decode(enc)}
            except Exception:
                continue
        return {"path": str(p), "text": data.decode("utf-8", errors="replace")}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)


@app.get("/api/file/delete")
async def file_delete(path: str = ""):
    if not path:
        return JSONResponse({"detail": "缺少路径"}, status_code=400)
    p = pathlib.Path(path)
    if not p.exists():
        return JSONResponse({"detail": "文件不存在"}, status_code=404)
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink()
        await broadcast_user(f"[完成] 已删除：{p}", "ok")
        write_record("file_delete", f"删除 {p.name}", str(p), "ok")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)# ================= 实时指标采样器（趋势数据源） =================
METRICS = collections.deque(maxlen=720)  # 每 5s 一点，最多 1 小时


async def _metric_sampler():
    while True:
        try:
            cpu = psutil.cpu_percent(interval=0.3)
            mem = psutil.virtual_memory().percent
            disk = 0
            try:
                disk = psutil.disk_usage(str(APP_DIR)).percent
            except Exception:
                pass
            METRICS.append({"t": time.time(), "cpu": round(cpu, 1),
                            "mem": round(mem, 1), "disk": round(disk, 1)})
        except Exception:
            pass
        await asyncio.sleep(5)


@app.get("/api/metrics/history")
async def metrics_history(minutes: int = 30):
    cutoff = time.time() - max(1, min(minutes, 120)) * 60
    return [m for m in METRICS if m["t"] >= cutoff]


# ================= 一键健康检查（LLMOps：可观测性） =================
@app.get("/api/health")
async def health_check():
    checks = []

    def add(name, ok, detail=""):
        checks.append({"name": name, "ok": bool(ok), "detail": str(detail)})

    add("后端服务", True, f"v{VERSION}")
    add("配置目录可写", _touch_writable(CONFIG_DIR), str(CONFIG_DIR))
    add("日志目录可写", _touch_writable(LOGS_DIR), str(LOGS_DIR))
    add("配置备份目录", _touch_writable(CONFIG_BACKUP_DIR),
        str(CONFIG_BACKUP_DIR))
    rd = get_record_dir()
    add("记录目录", ensure_dir(rd) and _touch_writable(rd), str(rd))
    bd = get_backup_dir()
    add("备份目录", ensure_dir(bd) and _touch_writable(bd), str(bd))

    deps = []
    for name in ("fastapi", "uvicorn", "psutil", "apscheduler", "webview"):
        try:
            __import__(name)
            deps.append(True)
        except Exception:
            deps.append(False)
    add("依赖完整性", all(deps),
        ", ".join(n for n, ok in zip(
            ("fastapi", "uvicorn", "psutil", "apscheduler", "webview"),
            deps) if not ok) or "全部就绪")

    if HAS_SCHED and scheduler is not None:
        add("周期任务调度", scheduler.running,
            f"{len(scheduler.get_jobs())} 个任务")
    else:
        add("周期任务调度", False, "apscheduler 未安装")

    add("指标采样", len(METRICS) > 0, f"{len(METRICS)} 个采样点")
    try:
        disk = psutil.disk_usage(str(APP_DIR))
        add("磁盘空间", disk.free > 500 * 1024 * 1024,
            f"剩余 {disk.free/1024/1024/1024:.1f} GB")
    except Exception:
        add("磁盘空间", False, "无法读取")

    failed = [c for c in checks if not c["ok"]]
    status = "ok" if not failed else ("degraded" if len(failed) <= 2 else "error")
    log_json("app", "info", "health_check", status=status,
             failed=len(failed), total=len(checks))
    return {
        "status": status,
        "score": max(0, 100 - len(failed) * 15),
        "checks": checks,
        "version": VERSION,
        "rules_version": DIAG_RULES_VERSION,
        "uptime_seconds": int(time.time() - START_TIME),
        "listen": "127.0.0.1",
    }


# ================= 智能诊断引擎（AI 辅助 · 人工复核 · 不自动执行） =================
DIAG_HISTORY = collections.deque(maxlen=20)

SEVERITY_LABEL = {"high": "高", "medium": "中", "low": "低"}
SEVERITY_PENALTY = {"high": 30, "medium": 12, "low": 3}


async def _collect_evidence():
    """采集诊断证据。每项独立 try/except，单点失败不影响整体（故障降级）。"""
    ev = {}
    try:
        ov = await overview()
        ev["overview"] = ov
    except Exception:
        ev["overview"] = None
    try:
        ev["events"] = await event_logs(log_name="System", level="error",
                                        hours=24, limit=20)
    except Exception:
        ev["events"] = []
    if IS_WIN:
        script = ("Get-PhysicalDisk | "
                  "Select-Object FriendlyName,HealthStatus | ConvertTo-Json")
        out, err = await run_ps(script, timeout=30)
        ev["smart"] = parse_ps_json(out)
        script = ("Get-MpComputerStatus | "
                  "Select-Object AMServiceEnabled,RealTimeProtectionEnabled,"
                  "AntivirusEnabled | ConvertTo-Json")
        out, err = await run_ps(script, timeout=30)
        ev["defender"] = parse_ps_json(out)
        script = ("Get-NetFirewallProfile | "
                  "Select-Object Name,Enabled | ConvertTo-Json")
        out, err = await run_ps(script, timeout=30)
        ev["firewall"] = parse_ps_json(out)
    else:
        ev["smart"] = []
        ev["defender"] = []
        ev["firewall"] = []
    try:
        ev["snap"] = await startup_snapshot_diff()
    except Exception:
        ev["snap"] = {"time": None, "added": [], "removed": []}
    try:
        ev["procs"] = await processes()
    except Exception:
        ev["procs"] = []
    try:
        ev["large"] = await large_files(root=str(APP_DIR), min_mb=200, limit=10)
    except Exception:
        ev["large"] = []
    return ev


def _rule_memory(ev):
    ov = ev.get("overview") or {}
    pct = ov.get("mem_percent") or 0
    if pct >= 85:
        return {"severity": "high", "title": "内存占用过高",
                "evidence": f"当前内存占用 {pct}%，接近饱和，可能引发卡顿",
                "actions": ["clean_user_temp"], "advice": "关闭大程序或清理后台进程，必要时重启电脑"}
    if pct >= 70:
        return {"severity": "medium", "title": "内存占用偏高",
                "evidence": f"当前内存占用 {pct}%", "actions": [],
                "advice": "留意内存大户，长期偏高建议检查是否有异常进程"}
    return None


def _rule_disk(ev):
    ov = ev.get("overview") or {}
    disks = ov.get("disks") or []
    worst = max(disks, key=lambda x: x.get("percent", 0)) if disks else None
    if not worst:
        return None
    pct = worst.get("percent", 0)
    mount = worst.get("mount", "")
    if pct >= 90:
        return {"severity": "high", "title": f"磁盘 {mount} 空间告急",
                "evidence": f"占用 {pct}%，剩余 {worst.get('free', 0)/1024**3:.1f} GB",
                "actions": ["clean_user_temp", "clean_recycle"],
                "advice": "立即清理临时文件与回收站，空间耗尽会导致系统异常"}
    if pct >= 80:
        return {"severity": "medium", "title": f"磁盘 {mount} 空间偏高",
                "evidence": f"占用 {pct}%", "actions": ["scan_temp"],
                "advice": "建议扫描并清理临时文件，保持 15% 以上余量"}
    return None


def _rule_cpu(ev):
    ov = ev.get("overview") or {}
    pct = ov.get("cpu_percent") or 0
    if pct >= 85:
        return {"severity": "medium", "title": "CPU 持续高负载",
                "evidence": f"当前 CPU {pct}%", "actions": [],
                "advice": "查看性能优化页找出占用大户，留意异常挖矿/木马进程"}
    return None


def _rule_events(ev):
    events = ev.get("events") or []
    errors = [e for e in events if (e.get("level") or "") in
              ("错误", "Error", "严重", "Critical")]
    if len(errors) >= 5:
        tops = {}
        for e in errors:
            tops[e.get("provider", "?")] = tops.get(e.get("provider", "?"), 0) + 1
        top = max(tops.items(), key=lambda x: x[1])
        return {"severity": "high",
                "title": "系统事件日志出现大量错误",
                "evidence": f"24 小时内 {len(errors)} 条错误，主要来源 {top[0]}（{top[1]} 次）",
                "actions": [], "advice": "到事件日志页筛选错误级别，定位根因后再处理"}
    if errors:
        return {"severity": "medium", "title": "系统事件日志有错误记录",
                "evidence": f"24 小时内 {len(errors)} 条错误",
                "actions": [], "advice": "可到事件日志页查看具体来源"}
    return None


def _rule_smart(ev):
    smart = ev.get("smart") or []
    bad = [s for s in smart if (s.get("HealthStatus") or "") != "Healthy"]
    if bad:
        names = "、".join(s.get("FriendlyName", "未知") for s in bad[:3])
        return {"severity": "high", "title": "磁盘健康状态异常",
                "evidence": f"磁盘 {names} 状态：{bad[0].get('HealthStatus')}",
                "actions": ["backup_folder", "disk_health"],
                "advice": "立即备份重要数据，该磁盘可能即将损坏"}
    return None


def _rule_defender(ev):
    defender = ev.get("defender") or []
    if not defender:
        return None
    d = defender[0]
    off = [k for k in ("AMServiceEnabled", "RealTimeProtectionEnabled",
                       "AntivirusEnabled") if d.get(k) is False]
    if off:
        return {"severity": "high", "title": "Defender 防护被关闭",
                "evidence": f"以下防护未启用：{', '.join(off)}",
                "actions": ["defender_status"],
                "advice": "杀毒被关闭可能是病毒所为，请立即检查并恢复防护"}
    return None


def _rule_firewall(ev):
    fw = ev.get("firewall") or []
    disabled = [f.get("Name", "?") for f in fw if f.get("Enabled") is False]
    if disabled:
        return {"severity": "high", "title": "防火墙存在未启用配置",
                "evidence": f"{', '.join(disabled)} 配置文件已关闭",
                "actions": ["firewall_status"],
                "advice": "防火墙全部关闭会让机器暴露在公网风险中，请重新开启"}
    return None


def _rule_startup(ev):
    snap = ev.get("snap") or {}
    added = snap.get("added") or []
    if added:
        return {"severity": "medium", "title": "启动项发生变化",
                "evidence": f"相对上次快照新增 {len(added)} 项：{'、'.join(added[:5])}",
                "actions": ["startup_list"],
                "advice": "排查新增启动项，可疑项建议禁用；确认无误后请更新快照"}
    return None


def _rule_procs(ev):
    procs = ev.get("procs") or []
    unknown = [p for p in procs if not p.get("known") and
               (p.get("memory_percent") or 0) > 5]
    if unknown:
        p = unknown[0]
        return {"severity": "low", "title": "发现高内存未知进程",
                "evidence": f"{p.get('name')} 占用内存 {p.get('memory_percent')}%",
                "actions": [], "advice": "在进程管理页查看其路径，确认是否可信"}
    return None


def _rule_large(ev):
    large = ev.get("large") or []
    if len(large) >= 5:
        total = sum(x.get("size", 0) for x in large) / 1024 ** 3
        return {"severity": "low", "title": "程序目录存在较多大文件",
                "evidence": f"发现 {len(large)} 个 200MB 以上文件，合计 {total:.1f} GB",
                "actions": [], "advice": "在健康检查页查看大文件清单，确认可否清理"}
    return None


DIAG_RULES = [
    _rule_memory, _rule_disk, _rule_cpu, _rule_events, _rule_smart,
    _rule_defender, _rule_firewall, _rule_startup, _rule_procs, _rule_large,
]


def _score_from_findings(findings):
    score = 100
    for f in findings:
        score -= SEVERITY_PENALTY.get(f.get("severity", "low"), 3)
    return max(0, score)


def _llm_explain_sync(base_url, model, api_key, prompt):
    import urllib.request
    url = base_url.rstrip("/") + "/chat/completions"
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 800,
        "temperature": 0.3,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"]


async def _llm_explain(findings, ev):
    """可选外部 LLM 解读；任何失败自动降级为本地结论（故障降级）。"""
    key = SECRETS.get("llm_api_key") or ""
    base = SECRETS.get("llm_base_url") or ""
    model = SECRETS.get("llm_model") or ""
    if not key or not base or not model:
        return None
    brief = []
    for f in findings[:6]:
        brief.append(f"- [{SEVERITY_LABEL.get(f['severity'], f['severity'])}] "
                     f"{f['title']}：{f['evidence']}")
    if not brief:
        brief.append("- 未发现明显问题")
    prompt = (
        "你是本地运维工作台的诊断助手。以下是对这台 Windows 电脑的本地诊断摘要，"
        "请用中文给出 3 条以内、可直接执行的处置建议，按优先级排序。"
        "只做建议，不声称已执行任何操作。\n" + "\n".join(brief))
    try:
        text = await asyncio.to_thread(
            _llm_explain_sync, base, model, key, prompt)
        return text.strip()
    except Exception as e:
        log_json("diag", "warn", "llm_fallback", error=str(e)[:200])
        return None


@app.post("/api/diag/run")
async def diag_run():
    """运行智能诊断。AI 只出建议，不自动执行任何操作。"""
    await broadcast_user("[诊断] 开始采集证据（资源/事件/安全/启动项）…")
    t0 = time.time()
    ev = await _collect_evidence()
    findings = []
    for rule in DIAG_RULES:
        try:
            r = rule(ev)
            if r:
                r["rule"] = getattr(rule, "__name__", "rule")
                findings.append(r)
        except Exception as e:
            log_error("diag", "rule_failed", rule=rule.__name__, error=str(e))
    findings.sort(key=lambda x: SEVERITY_PENALTY.get(x["severity"], 0),
                  reverse=True)
    for f in findings:
        f["severity_label"] = SEVERITY_LABEL.get(f["severity"], f["severity"])
        f["require_review"] = True
    score = _score_from_findings(findings)
    summary = await _llm_explain(findings, ev)
    result = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "score": score,
        "findings": findings,
        "summary": summary,
        "llm_used": bool(summary),
        "rules_version": DIAG_RULES_VERSION,
        "duration_s": round(time.time() - t0, 1),
        "evidence_count": sum(1 for v in ev.values() if v),
        "security": {
            "defender": ev.get("defender") or [],
            "firewall": ev.get("firewall") or [],
            "smart": ev.get("smart") or [],
        },
    }
    DIAG_HISTORY.appendleft(result)
    log_json("diag", "info", "diag_run", score=score,
             findings=len(findings), llm_used=bool(summary),
             duration_s=result["duration_s"])
    try:
        with open(LOGS_DIR / "diag.jsonl", "a", encoding="utf-8") as fp:
            fp.write(json.dumps(result, ensure_ascii=False) + "\n")
    except Exception:
        pass
    await broadcast_user(f"[诊断] 完成：得分 {score}，发现 {len(findings)} 项"
                         f"{'，已使用外部 LLM 解读' if summary else '（本地规则引擎，未启用外部 LLM）'}")
    return result


@app.get("/api/diag/latest")
async def diag_latest():
    if DIAG_HISTORY:
        return DIAG_HISTORY[0]
    try:
        f = LOGS_DIR / "diag.jsonl"
        if f.exists():
            lines = f.read_text(encoding="utf-8").strip().splitlines()
            if lines:
                return json.loads(lines[-1])
    except Exception:
        pass
    return None


@app.get("/api/diag/history")
async def diag_history(limit: int = 10):
    items = list(DIAG_HISTORY)
    if len(items) < limit:
        seen = {it.get("generated_at") for it in items}
        try:
            f = LOGS_DIR / "diag.jsonl"
            if f.exists():
                lines = f.read_text(encoding="utf-8").strip().splitlines()
                for line in reversed(lines):
                    try:
                        it = json.loads(line)
                    except Exception:
                        continue
                    key = it.get("generated_at")
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(it)
                    if len(items) >= limit:
                        break
        except Exception:
            pass
    return items[:limit]


@app.get("/api/diag/rules")
async def diag_rules():
    return {
        "version": DIAG_RULES_VERSION,
        "rules": [
            {"name": r.__name__,
             "desc": (r.__doc__ or "").strip()} for r in DIAG_RULES
        ],
        "count": len(DIAG_RULES),
        "llm_configured": bool(SECRETS.get("llm_api_key")),
    }


# ================= 普通人视角：体检聚合 / 可释放空间 / 一键清理 =================
def _fmt_bytes(n):
    try:
        n = float(n or 0)
    except Exception:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return "0 B"


def _dir_size(path, cap_files=100000):
    """递归统计目录大小（容错：跳过权限错误与占用文件）。"""
    total = 0
    count = 0
    try:
        with os.scandir(str(path)) as it:
            for e in it:
                try:
                    if e.is_dir(follow_symlinks=False):
                        total += _dir_size(e.path, cap_files - count)
                    elif e.is_file(follow_symlinks=False):
                        total += e.stat().st_size
                    count += 1
                    if count >= cap_files:
                        return total
                except Exception:
                    continue
    except Exception:
        pass
    return total


def _clean_target_paths(aid):
    home = pathlib.Path.home()
    if aid == "clean_user_temp":
        return [pathlib.Path(os.environ.get("TEMP", home / "AppData/Local/Temp"))]
    if aid == "clean_sys_temp":
        return [pathlib.Path(os.environ.get("SystemRoot", r"C:\Windows")) / "Temp"]
    if aid == "clean_recycle":
        paths = []
        try:
            for d in psutil.disk_partitions():
                try:
                    if d.fstype and d.fstype.lower().startswith(
                            ("ntfs", "fat", "exfat", "refs")):
                        paths.append(pathlib.Path(d.mountpoint) / "$Recycle.Bin")
                except Exception:
                    continue
        except Exception:
            pass
        return paths
    if aid == "clean_thumb":
        la = os.environ.get("LOCALAPPDATA", home / "AppData/Local")
        return [pathlib.Path(la) / "Microsoft" / "Windows" / "Explorer"]
    return []


async def _clean_target_size(aid):
    total = 0
    for p in _clean_target_paths(aid):
        total += _dir_size(p)
    return total


@app.get("/api/clean/estimate")
async def clean_estimate():
    """预估可释放空间（只统计，不删除）。"""
    items = {}
    for aid in ("clean_user_temp", "clean_sys_temp", "clean_recycle",
                "clean_thumb"):
        try:
            items[aid] = await _clean_target_size(aid)
        except Exception:
            items[aid] = 0
    items["total"] = sum(items.values())
    return items


def _checkup_verdict(score):
    if score >= 90:
        return "状态良好", "ok"
    if score >= 75:
        return "基本正常", "ok"
    if score >= 60:
        return "需要关注", "warn"
    if score >= 40:
        return "需要处理", "warn"
    return "问题较多", "err"


def _checkup_summary(diag):
    finds = diag.get("findings") or []
    if not finds:
        return "一切正常，保持常规维护即可。建议每周清理一次临时文件，并在重大变更前创建系统还原点。"
    high = [f for f in finds if f.get("severity") == "high"]
    mid = [f for f in finds if f.get("severity") == "medium"]
    parts = []
    if high:
        names = "、".join(f.get("title", "") for f in high[:2])
        parts.append(f"有 {len(high)} 个需要优先处理的问题（{names}）")
    if mid:
        parts.append(f"{len(mid)} 个建议关注项")
    low = len(finds) - len(high) - len(mid)
    if low:
        parts.append(f"{low} 个可留意项")
    tip = finds[0].get("advice") or ""
    text = "；".join(parts) + "。"
    if tip:
        text += f"优先建议：{tip}"
    return text


@app.post("/api/checkup")
async def checkup():
    """一键体检（普通人视角）：诊断 + 安全 + 概览 + 可释放空间，聚合为一份结论。"""
    diag = await diag_run()
    try:
        ov = await overview()
    except Exception:
        ov = {}
    try:
        h = await health_check()
    except Exception:
        h = {}
    try:
        est = await clean_estimate()
    except Exception:
        est = {}
    issues = []
    for f in diag.get("findings") or []:
        acts = [{"id": a, "name": (ACTIONS.get(a) or {}).get("name", a)}
                for a in (f.get("actions") or [])]
        issues.append({
            "severity": f.get("severity"),
            "severity_label": f.get("severity_label"),
            "title": f.get("title"),
            "evidence": f.get("evidence"),
            "advice": f.get("advice"),
            "actions": acts,
            "require_review": bool(f.get("require_review", True)),
        })
    score = diag.get("score") or 0
    verdict, vlevel = _checkup_verdict(score)
    return {
        "generated_at": diag.get("generated_at"),
        "score": score,
        "verdict": verdict,
        "verdict_level": vlevel,
        "summary_text": _checkup_summary(diag),
        "issues": issues,
        "security": diag.get("security") or {},
        "overview": ov,
        "health": h,
        "clean_estimate": est,
        "llm_used": bool(diag.get("summary")),
        "rules_version": diag.get("rules_version"),
    }


QUICK_CLEAN_STEPS = []


async def task_quick_clean():
    """一键清理（可被动作框架与定时任务调度）。串行低风险清理，逐项统计释放量。"""
    QUICK_CLEAN_STEPS.clear()
    for aid in ("clean_user_temp", "clean_recycle", "clean_thumb"):
        act = ACTIONS.get(aid)
        if not act:
            continue
        before = await _clean_target_size(aid)
        await _do_action_task(aid, act)
        after = await _clean_target_size(aid)
        QUICK_CLEAN_STEPS.append({"action": aid, "name": act["name"],
                                  "released": max(0, before - after)})
    total = sum(s["released"] for s in QUICK_CLEAN_STEPS)
    await broadcast_user(f"[完成] 一键清理结束，共释放 {_fmt_bytes(total)}", "ok")
    return f"一键清理完成，共释放 {_fmt_bytes(total)}"


@app.post("/api/maintain/quick_clean")
async def quick_clean():
    """一键清理：串行执行低风险清理动作，逐项统计实际释放空间。"""
    await task_quick_clean()
    return {"steps": list(QUICK_CLEAN_STEPS),
            "total_released": sum(s["released"] for s in QUICK_CLEAN_STEPS)}


@app.get("/api/checkup/history")
async def checkup_history(limit: int = 20):
    """体检历史（分数 / 问题数），用于趋势展示。"""
    items = []
    f = LOGS_DIR / "diag.jsonl"
    if f.exists():
        lines = f.read_text(encoding="utf-8").strip().splitlines()
        for line in reversed(lines[-limit * 3:]):
            try:
                d = json.loads(line)
                items.append({
                    "generated_at": d.get("generated_at"),
                    "score": d.get("score"),
                    "findings_count": len(d.get("findings") or []),
                })
            except Exception:
                continue
            if len(items) >= limit:
                break
    items.reverse()
    return items


# 一键清理注册为标准动作：可被动作卡、/api/action 与定时任务调度
PY_TASKS.update({"quick_clean": task_quick_clean})


# ================= 首页与启动 =================
@app.get("/")
async def index():
    return FileResponse(RES_DIR / "index.html")


@app.on_event("startup")
async def _startup():
    log_deploy("server_start", version=VERSION, port=os.environ.get("OPS_PORT", "8000"))
    asyncio.create_task(_metric_sampler())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("OPS_PORT", "8000"))
    log_deploy("server_enter", version=VERSION, port=port)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")