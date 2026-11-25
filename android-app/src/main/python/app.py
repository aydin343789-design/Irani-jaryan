from __future__ import annotations

import asyncio
import inspect
import json
import os
import sqlite3
import smtplib
import tempfile
import imaplib
import email
import re
from collections import defaultdict
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import decode_header
from pathlib import Path
from typing import Any, Dict, List, Optional
import base64
import hashlib
import hmac
import time
import secrets
import mimetypes
from urllib.parse import quote

import aiohttp
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel, Field

load_dotenv()

APP_TITLE = "جریان ایرانی Pro"
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR

# حریم خصوصی: هیچ دیتابیس دائمی روی سرور ساخته نمی‌شود.
# همه تنظیمات حساس باید فقط در localStorage گوشی کاربر ذخیره شوند و سرور فقط هنگام اجرای زنده
# داده‌ها را در RAM نگه می‌دارد.
DB_PATH = "file:iran_flow_runtime?mode=memory&cache=shared"
_RUNTIME_DB = sqlite3.connect(DB_PATH, uri=True, check_same_thread=False)
_RUNTIME_DB.row_factory = sqlite3.Row
MEDIA_DIR = BASE_DIR / "app_memory_media"
MEDIA_DIR.mkdir(exist_ok=True)
MEDIA_TTL_SECONDS = int(os.getenv("MEDIA_TTL_SECONDS", "3600"))
TEHRAN_TZ = pytz.timezone("Asia/Tehran")

app = FastAPI(title=APP_TITLE)

# ============================================================
# مدل‌های داده
# ============================================================
class UserSettings(BaseModel):
    # تلگرام
    tg_token: str = ""
    tg_chat: str = ""
    
    # پیام‌رسان‌های ایرانی
    bale_token: str = ""
    bale_chat: str = ""
    
    rubika_token: str = ""
    rubika_chat: str = ""
    
    soroush_token: str = ""
    soroush_chat: str = ""
    
    gap_token: str = ""
    gap_chat: str = ""
    
    # اینستاگرام
    ig_username: str = ""
    ig_password: str = ""
    ig_business_id: str = ""
    ig_access_token: str = ""
    
    # ایمیل
    email_host: str = ""
    email_user: str = ""
    email_pass: str = ""
    email_imap: str = ""
    
    ai_api_key: str = ""
    ai_base_url: str = ""
    # آدرس عمومی اپلیکیشن برای ساخت لینک دانلود مستقیم روبیکا؛ فقط از درخواست کاربر خوانده می‌شود و ذخیره نمی‌شود.
    public_base_url: str = ""

class Node(BaseModel):
    id: str
    type: str
    data: Dict[str, Any]
    position: Dict[str, float]

class Edge(BaseModel):
    source: str
    target: str
    # برچسب شاخه برای گره‌های شرطی/سوییچ، مثل "true"/"false" یا مقدار یک case.
    # اگر خالی باشد، این یال بدون قید همیشه دنبال می‌شود (سازگار با ورک‌فلوهای قدیمی خطی).
    data: Dict[str, Any] = Field(default_factory=dict)

class Workflow(BaseModel):
    nodes: List[Node]
    edges: List[Edge]
    meta: Dict[str, Any] = Field(default_factory=dict)

class ExecuteRequest(BaseModel):
    workflow: Dict[str, Any]
    input_text: str = ""
    settings: UserSettings = Field(default_factory=UserSettings)
    extra_data: Dict[str, Any] = Field(default_factory=dict)
    # اجرای تستی مثل دکمه‌ی «Execute workflow» در n8n: بدون فعال‌سازی شنونده/تایمر/وب‌هوک،
    # فقط یک‌بار همین الان با متن ورودی اجرا می‌شود و نتیجه‌ی هر گره برمی‌گردد.
    test_mode: bool = False

class SaveWorkflowRequest(BaseModel):
    name: str
    workflow: Dict[str, Any]
    description: str = ""
    status: str = "draft"

# ============================================================
# پایگاه داده
# ============================================================
def now_iso() -> str:
    return datetime.now(TEHRAN_TZ).isoformat()

def db_conn():
    # اتصال مشترک in-memory؛ با context manager بسته نمی‌شود و هیچ فایل DB روی دیسک ساخته نمی‌شود.
    return _RUNTIME_DB

def _privacy_safe(text: str, limit: int = 220) -> str:
    """حذف داده‌های حساس از لاگ‌های موقت RAM. این لاگ‌ها روی دیسک ذخیره نمی‌شوند."""
    t = str(text or "")
    # توکن‌های رایج بات و کلیدهای بلند را ماسک کن
    t = re.sub(r"\b\d{5,}:[A-Za-z0-9_\-]{12,}\b", "[TOKEN]", t)
    t = re.sub(r"(?i)(token|password|pass|api[_-]?key)\s*[:=]\s*[^,\s}]+", r"\1=[HIDDEN]", t)
    if len(t) > limit:
        t = t[:limit] + "…"
    return t

def save_history(action: str, details: str, status: str, type_: str = "info") -> None:
    try:
        # برای حفظ حریم خصوصی، تاریخچه فقط در RAM و با جزئیات ماسک‌شده نگه‌داری می‌شود.
        safe_details = _privacy_safe(details)
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO history (timestamp, action, details, status, type) VALUES (?, ?, ?, ?, ?)",
                (now_iso(), _privacy_safe(action, 80), safe_details, status, type_),
            )
            conn.commit()
    except Exception:
        pass

def add_notification(title: str, message: str) -> None:
    try:
        # متن پیام مخاطب در اعلان ذخیره نمی‌شود؛ فقط وضعیت عملیاتی موقت نمایش داده می‌شود.
        safe_title = _privacy_safe(title, 80)
        safe_message = "پیام جدید پردازش شد؛ متن برای حفظ حریم خصوصی ذخیره نشد." if "پیام از" in safe_title else _privacy_safe(message)
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO notifications (timestamp, title, message, read) VALUES (?, ?, ?, 0)",
                (now_iso(), safe_title, safe_message),
            )
            conn.commit()
    except Exception:
        pass

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT NOT NULL,
                status TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'info'
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                workflow TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT DEFAULT 'draft'
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                read INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
        save_history("سیستم", "پایگاه داده راه‌اندازی شد", "موفق", "system")

init_db()

def get_setting(key: str, default: str = "") -> str:
    # سرور هیچ تنظیمات/توکنی را ذخیره یا بازیابی نمی‌کند. تنظیمات فقط از گوشی کاربر با هر درخواست ارسال می‌شوند.
    return default

def save_setting(key: str, value: str) -> None:
    # no-op به‌خاطر سیاست local-only
    return None

def list_history(limit: int = 50):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM history ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]

def list_notifications(limit: int = 20):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM notifications ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]

def list_workflows():
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, name, description, created_at, updated_at, status FROM workflows ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]

# ============================================================
# توابع کمکی
# ============================================================
def _guess_filename(media_type: str, file_meta: Dict[str, Any]) -> str:
    original = (file_meta or {}).get("file_name") or ""
    if original:
        return original
    if media_type == "photo":
        return "photo.jpg"
    if media_type == "video":
        return "video.mp4"
    return "file.bin"

def _guess_content_type(media_type: str, file_meta: Dict[str, Any]) -> str:
    mime = (file_meta or {}).get("mime_type") or ""
    if mime:
        return mime
    if media_type == "photo":
        return "image/jpeg"
    if media_type == "video":
        return "video/mp4"
    return "application/octet-stream"

def _normalize_text(value: str) -> str:
    return (value or "").strip().lower()

TEMP_MEDIA: Dict[str, Dict[str, Any]] = {}

def _safe_filename(name: str, fallback: str = "rubika-video.mp4") -> str:
    name = os.path.basename(name or fallback).strip() or fallback
    name = re.sub(r"[^A-Za-z0-9._\- آ-ی()]+", "_", name)
    return name[:120] or fallback

def _delete_quietly(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

def create_temp_download_link(file_bytes: bytes, file_name: str, mime_type: str, public_base_url: str = "") -> str:
    """ذخیره موقت فایل در حافظه/فضای خصوصی اپ و ساخت لینک یک‌بارمصرف دانلود.
    فایل بعد از اولین دانلود یا پایان TTL حذف می‌شود.
    """
    if not file_bytes:
        raise ValueError("file_bytes is empty")
    token = secrets.token_urlsafe(32)
    filename = _safe_filename(file_name)
    guessed_ext = mimetypes.guess_extension(mime_type or "") or Path(filename).suffix or ".bin"
    stored_path = MEDIA_DIR / f"{token}{guessed_ext}"
    stored_path.write_bytes(file_bytes)
    TEMP_MEDIA[token] = {
        "path": str(stored_path),
        "filename": filename,
        "mime_type": mime_type or "application/octet-stream",
        "created_at": time.time(),
        "expires_at": time.time() + MEDIA_TTL_SECONDS,
        "size": len(file_bytes),
    }
    base = (public_base_url or os.getenv("PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:8000"
    return f"{base}/download/{token}/{quote(filename)}"

async def cleanup_temp_media_loop():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for token, info in list(TEMP_MEDIA.items()):
            if info.get("expires_at", 0) < now:
                TEMP_MEDIA.pop(token, None)
                _delete_quietly(Path(info.get("path", "")))

async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value

# ============================================================
# دانلود فایل از تلگرام و پلتفرم‌های مشابه
# ============================================================
async def download_telegram_file(token: str, file_id: str) -> Optional[bytes]:
    if not token or not file_id:
        return None
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"https://api.telegram.org/bot{token}/getFile",
                params={"file_id": file_id},
            ) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json()
                if not payload.get("ok"):
                    return None
                file_path = payload["result"]["file_path"]

            async with session.get(
                f"https://api.telegram.org/file/bot{token}/{file_path}"
            ) as file_resp:
                if file_resp.status != 200:
                    return None
                return await file_resp.read()
    except Exception as e:
        save_history("دانلود تلگرام", str(e), "خطا", "error")
        return None

async def get_telegram_file_url(token: str, file_id: str, platform: str = "telegram") -> Optional[str]:
    """دریافت URL مستقیم فایل از تلگرام/بله"""
    base_url = "https://tapi.bale.ai" if platform == "bale" else "https://api.telegram.org"
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"{base_url}/bot{token}/getFile",
                params={"file_id": file_id},
            ) as resp:
                if resp.status != 200:
                    save_history("getFile", f"{platform} status={resp.status}", "خطا", "error")
                    return None
                payload = await resp.json()
                if not payload.get("ok"):
                    save_history("getFile", f"{platform}: {payload.get('description','')}", "خطا", "error")
                    return None
                file_path = payload["result"]["file_path"]
                return f"{base_url}/file/bot{token}/{file_path}"
    except Exception as e:
        save_history("getFile URL", f"{platform}: {str(e)[:200]}", "خطا", "error")
        return None

async def download_file(token: str, file_id: str, platform: str = "telegram") -> Optional[bytes]:
    """دانلود فایل از سرور پلتفرم - با timeout بیشتر برای ویدئو"""
    if not token or not file_id:
        return None
    
    if platform in ["telegram", "bale"]:
        url = await get_telegram_file_url(token, file_id, platform)
        if not url:
            return None
        try:
            # timeout بیشتر برای ویدئو - 3 دقیقه
            timeout = aiohttp.ClientTimeout(total=180, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as file_resp:
                    if file_resp.status != 200:
                        save_history("دانلود فایل", f"{platform}: HTTP {file_resp.status}", "خطا", "error")
                        return None
                    data = await file_resp.read()
                    save_history("دانلود فایل", f"{platform}: {len(data)} bytes دانلود شد", "موفق", "info")
                    return data
        except asyncio.TimeoutError:
            save_history("دانلود فایل", f"{platform}: timeout - فایل خیلی بزرگ است", "خطا", "error")
            return None
        except Exception as e:
            save_history("دانلود فایل", f"{platform}: {str(e)[:200]}", "خطا", "error")
            return None
    
    return None

# ============================================================
# ارسال به تلگرام (سالم از فایل اول)
# ============================================================
async def send_telegram_message(token: str, chat_id: str, text: str) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": str(chat_id), "text": text},
            ) as resp:
                if resp.status == 200:
                    return "✅ تلگرام: متن ارسال شد"
                return f"❌ تلگرام: {resp.status}"
    except Exception as e:
        return f"❌ تلگرام: {str(e)[:120]}"

async def send_telegram_media(
    token: str,
    chat_id: str,
    media_type: str,
    file_bytes: bytes,
    caption: str = "",
    file_name: str = "",
    mime_type: str = "",
) -> str:
    if not token or not chat_id or not file_bytes:
        return "❌ تلگرام: اطلاعات ناقص است"
    endpoint_map = {
        "photo": ("sendPhoto", "photo"),
        "video": ("sendVideo", "video"),
        "document": ("sendDocument", "document"),
    }
    endpoint, field_name = endpoint_map.get(media_type, ("sendDocument", "document"))
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if caption:
            form.add_field("caption", caption)
        form.add_field(
            field_name,
            file_bytes,
            filename=file_name or _guess_filename(media_type, {"mime_type": mime_type}),
            content_type=mime_type or _guess_content_type(media_type, {}),
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{token}/{endpoint}",
                data=form,
            ) as resp:
                if resp.status == 200:
                    return f"✅ تلگرام: {media_type} ارسال شد"
                return f"❌ تلگرام: {resp.status}"
    except Exception as e:
        return f"❌ تلگرام: {str(e)[:120]}"

# ============================================================
# ارسال به بله (سالم از فایل دوم)
# ============================================================
async def send_bale(
    token: str,
    chat_id: str,
    msg: Dict[str, Any],
    file_bytes: Optional[bytes] = None,
    tg_token: str = "",
) -> str:
    if not token or not chat_id:
        return "❌ بله: توکن یا Chat ID تنظیم نشده"

    media_type = msg.get("type", "text")
    caption = msg.get("caption", "") or ""
    text = msg.get("text") or caption or ""
    fn = msg.get("file_name", "") or ""
    mime = msg.get("mime_type", "") or ""
    file_id = msg.get("file_id", "") or ""

    base = f"https://tapi.bale.ai/bot{token}"

    try:
        if media_type in {"photo", "video", "document"}:
            ep_map = {
                "photo":    ("sendPhoto",    "photo"),
                "video":    ("sendVideo",    "video"),
                "document": ("sendDocument", "document"),
            }
            endpoint, field = ep_map.get(media_type, ("sendDocument", "document"))

            # اگر file_bytes داریم، مستقیم آپلود کن
            if file_bytes:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(chat_id))
                if caption:
                    form.add_field("caption", caption)
                form.add_field(
                    field, file_bytes,
                    filename=fn or _guess_filename(media_type, {"mime_type": mime}),
                    content_type=mime or _guess_content_type(media_type, {}),
                )
                timeout = aiohttp.ClientTimeout(total=180)
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.post(f"{base}/{endpoint}", data=form) as r:
                        if r.status == 200:
                            return f"✅ بله: {media_type} ارسال شد"
                        try:
                            err = await r.json()
                            err_desc = err.get("description", str(r.status))
                        except:
                            err_desc = str(r.status)
                        # اگر خطای حجم داشت، با URL امتحان کن
                        if "file" in err_desc.lower() or "large" in err_desc.lower() or r.status == 400:
                            save_history("بله آپلود", f"آپلود مستقیم شکست: {err_desc} - تلاش با URL", "خطا", "error")
                        else:
                            return f"❌ بله: {err_desc}"

            # اگر file_bytes نداریم یا آپلود شکست خورد، از URL مستقیم تلگرام استفاده کن
            if file_id and tg_token:
                tg_url = await get_telegram_file_url(tg_token, file_id, "telegram")
                if tg_url:
                    json_body = {"chat_id": str(chat_id), field: tg_url}
                    if caption:
                        json_body["caption"] = caption
                    async with aiohttp.ClientSession() as s:
                        async with s.post(f"{base}/{endpoint}", json=json_body) as r:
                            if r.status == 200:
                                return f"✅ بله: {media_type} (URL) ارسال شد"
                            try:
                                err = await r.json()
                                err_desc = err.get("description", str(r.status))
                            except:
                                err_desc = str(r.status)
                            save_history("بله URL", f"{err_desc}", "خطا", "error")

            # fallback: متن با اطلاعات فایل
            fallback_text = caption or text or f"[{media_type}]"
            if not fallback_text:
                fallback_text = f"[پیام {media_type} دریافت شد]"
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{base}/sendMessage",
                                  json={"chat_id": str(chat_id), "text": fallback_text}) as r:
                    if r.status == 200:
                        return f"⚠️ بله: {media_type} به صورت متن ارسال شد (فایل قابل انتقال نبود)"
                    return f"❌ بله: {r.status}"

        else:
            # پیام متنی
            send_text = text if text else "پیام دریافتی"
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{base}/sendMessage",
                                  json={"chat_id": str(chat_id), "text": send_text}) as r:
                    if r.status == 200:
                        return "✅ بله: متن ارسال شد"
                    try:
                        err = await r.json()
                        return f"❌ بله: {err.get('description', r.status)}"
                    except:
                        return f"❌ بله: {r.status}"
    except Exception as e:
        save_history("send_bale خطا", str(e)[:300], "خطا", "error")
        return f"❌ بله: {str(e)[:120]}"

# ============================================================
# ارسال به روبیکا
# ============================================================
async def send_rubika(
    token: str,
    chat_id: str,
    msg: Dict[str, Any],
    file_bytes: Optional[bytes] = None,
    tg_token: str = "",
    public_base_url: str = "",
) -> str:
    """ارسال به روبیکا.
    روبیکا در این اپ به دلیل محدودیت زیرساخت فقط متن می‌گیرد؛ برای ویدئو، فایل موقت در
    حافظه اپ ذخیره و یک لینک دانلود مستقیم/یک‌بارمصرف ارسال می‌شود. پس از اولین دانلود،
    فایل از حافظه اپ حذف می‌شود.
    """
    if not token or not chat_id:
        return "❌ روبیکا: توکن یا Chat ID تنظیم نشده"

    media_type = msg.get("type", "text")
    caption = msg.get("caption", "") or ""
    raw_text = msg.get("text") or caption or ""
    file_name = msg.get("file_name") or _guess_filename(media_type, msg)
    mime_type = msg.get("mime_type") or _guess_content_type(media_type, msg)

    if media_type == "video":
        if file_bytes:
            try:
                download_url = create_temp_download_link(
                    file_bytes=file_bytes,
                    file_name=file_name or "rubika-video.mp4",
                    mime_type=mime_type or "video/mp4",
                    public_base_url=public_base_url,
                )
                parts = [
                    "🎬 پیام تصویری دریافت شد",
                ]
                if caption or raw_text:
                    parts.append(f"📝 توضیح: {caption or raw_text}")
                parts.extend([
                    "🔗 لینک دانلود مستقیم و یک‌بارمصرف:",
                    download_url,
                    "♻️ بعد از اولین دانلود، ویدئو به‌صورت خودکار و کامل از حافظه اپلیکیشن حذف می‌شود.",
                ])
                if "127.0.0.1" in download_url or "localhost" in download_url:
                    parts.append("⚠️ برای قابل‌دسترس بودن لینک برای مخاطب روبیکا، آدرس عمومی اپلیکیشن را در تنظیمات وارد کنید.")
                text = "\n".join(parts)
            except Exception as exc:
                save_history("لینک ویدئوی روبیکا", str(exc), "خطا", "error")
                text = raw_text or "[ویدئو دریافت شد، اما ساخت لینک دانلود ناموفق بود]"
        else:
            text = raw_text or "[ویدئو دریافت شد، اما فایل برای ساخت لینک دانلود در دسترس نبود]"
    elif media_type in {"photo", "document", "voice", "audio"}:
        type_names = {"photo":"عکس","document":"فایل","voice":"پیام صوتی","audio":"موزیک"}
        type_label = type_names.get(media_type, media_type)
        text = raw_text if raw_text else f"[{type_label} دریافت شد]"
        if caption and media_type != "text":
            text = f"[{type_label}] {caption}"
    else:
        text = raw_text if raw_text else "پیام دریافتی"

    try:
        async with aiohttp.ClientSession() as s:
            payload = {
                "chat_id": str(chat_id),
                "text": text
            }
            async with s.post(f"https://botapi.rubika.ir/v3/{token}/sendMessage",
                             json=payload,
                             headers={"Content-Type": "application/json"},
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("ok") or data.get("status") == "OK":
                        return "✅ روبیکا: متن/لینک ارسال شد" if media_type == "video" else "✅ روبیکا: متن ارسال شد"
                    return f"❌ روبیکا: {data.get('description', 'خطای نامشخص')}"
                else:
                    try:
                        err = await r.json()
                        return f"❌ روبیکا: {err.get('description', r.status)}"
                    except:
                        return f"❌ روبیکا: {r.status}"
    except Exception as e:
        return f"❌ روبیکا: {str(e)[:120]}"

# ============================================================
# ارسال به سروش (با پشتیبانی از فایل)
# ============================================================
async def send_soroush(
    token: str,
    chat_id: str,
    msg: Dict[str, Any],
    file_bytes: Optional[bytes] = None,
) -> str:
    if not token or not chat_id:
        return "❌ سروش: توکن یا Chat ID تنظیم نشده"

    media_type = msg.get("type", "text")
    caption = msg.get("caption", "")
    text = msg.get("text") or caption or "پیام دریافتی"
    fn = msg.get("file_name", "")
    mime = msg.get("mime_type", "")

    # ساختار API سروش
    base = f"https://bot.sapp.ir/{token}"

    try:
        if file_bytes and media_type in {"photo", "video", "document"}:
            # آپلود فایل
            form = aiohttp.FormData()
            form.add_field("to", str(chat_id))
            if caption:
                form.add_field("text", caption)
            
            # تعیین نوع فایل
            if media_type == "photo":
                form.add_field("type", "PHOTO")
                form.add_field("file", file_bytes, filename=fn or "photo.jpg", content_type="image/jpeg")
            elif media_type == "video":
                form.add_field("type", "VIDEO")
                form.add_field("file", file_bytes, filename=fn or "video.mp4", content_type="video/mp4")
            else:
                form.add_field("type", "DOCUMENT")
                form.add_field("file", file_bytes, filename=fn or "document.pdf", content_type="application/pdf")

            async with aiohttp.ClientSession() as s:
                async with s.post(f"{base}/sendMessage", data=form) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data.get("resultCode") == 200 or data.get("success"):
                            return f"✅ سروش: {media_type} ارسال شد"
                        return f"❌ سروش: {data.get('resultMessage', 'خطای نامشخص')}"
                    else:
                        try:
                            err = await r.json()
                            return f"❌ سروش: {err.get('resultMessage', r.status)}"
                        except:
                            return f"❌ سروش: {r.status}"
        else:
            async with aiohttp.ClientSession() as s:
                payload = {
                    "to": str(chat_id),
                    "body": text,
                    "type": "TEXT"
                }
                async with s.post(f"{base}/sendMessage",
                                 json=payload,
                                 headers={"Content-Type": "application/json"},
                                 timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data.get("resultCode") == 200 or data.get("success"):
                            return "✅ سروش: متن ارسال شد"
                        return f"❌ سروش: {data.get('resultMessage', 'خطای نامشخص')}"
                    else:
                        try:
                            err = await r.json()
                            return f"❌ سروش: {err.get('resultMessage', r.status)}"
                        except:
                            return f"❌ سروش: {r.status}"
    except Exception as e:
        return f"❌ سروش: {str(e)[:120]}"

# ============================================================
# ارسال به گپ
# ============================================================
async def send_gap(
    token: str,
    chat_id: str,
    msg: Dict[str, Any],
    file_bytes: Optional[bytes] = None,
) -> str:
    if not token or not chat_id:
        return "❌ گپ: توکن یا Chat ID تنظیم نشده"

    text = msg.get("text") or msg.get("caption") or "پیام دریافتی"

    try:
        async with aiohttp.ClientSession() as s:
            headers = {"token": token}
            payload = {
                "chat_id": str(chat_id),
                "type": "text",
                "data": text
            }
            async with s.post("https://api.gap.im/v1/sendMessage",
                             json=payload,
                             headers=headers,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("ok") or data.get("status") == "OK":
                        return "✅ گپ: متن ارسال شد"
                    return f"❌ گپ: {data.get('description', 'خطای نامشخص')}"
                else:
                    try:
                        err = await r.json()
                        return f"❌ گپ: {err.get('description', r.status)}"
                    except:
                        return f"❌ گپ: {r.status}"
    except Exception as e:
        return f"❌ گپ: {str(e)[:120]}"

# ============================================================
# ارسال به اینستاگرام
# ============================================================
async def send_instagram(
    access_token: str,
    ig_user_id: str,
    msg: Dict[str, Any],
    file_bytes: Optional[bytes] = None,
) -> str:
    """ارسال به اینستاگرام (بزنس/کریتور اکانت)"""
    media_type = msg.get("type", "text")
    caption = msg.get("caption") or msg.get("text") or "پیام دریافتی"

    if not access_token or not ig_user_id:
        return "❌ اینستاگرام: توکن یا User ID تنظیم نشده"

    if not file_bytes or media_type not in {"photo", "video"}:
        return "❌ اینستاگرام: فقط عکس و ویدئو پشتیبانی می‌شود"

    suffix = ".jpg" if media_type == "photo" else ".mp4"
    tmp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        async with aiohttp.ClientSession() as s:
            upload_url = f"https://graph.facebook.com/v18.0/{ig_user_id}/media"

            with open(tmp_path, 'rb') as f:
                file_data = f.read()

            form = aiohttp.FormData()
            form.add_field("access_token", access_token)
            form.add_field("caption", caption)

            if media_type == "photo":
                form.add_field("media_type", "IMAGE")
                form.add_field("image", file_data, filename=f"upload{suffix}",
                             content_type="image/jpeg")
            else:
                form.add_field("media_type", "REELS")
                form.add_field("video", file_data, filename=f"upload{suffix}",
                             content_type="video/mp4")

            async with s.post(upload_url, data=form) as upload:
                upload_data = await upload.json()
                if upload.status != 200:
                    return f"❌ اینستاگرام: آپلود ناموفق - {upload_data.get('error', {}).get('message', upload.status)}"

                creation_id = upload_data.get("id")
                if not creation_id:
                    return f"❌ اینستاگرام: {upload_data}"

            publish_url = f"https://graph.facebook.com/v18.0/{ig_user_id}/media_publish"
            async with s.post(publish_url, data={
                "access_token": access_token,
                "creation_id": creation_id
            }) as pub:
                if pub.status == 200:
                    return f"✅ اینستاگرام: {media_type} ارسال شد"
                else:
                    pub_data = await pub.json()
                    return f"❌ اینستاگرام: انتشار ناموفق - {pub_data.get('error', {}).get('message', pub.status)}"

    except Exception as e:
        return f"❌ اینستاگرام: {str(e)[:120]}"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass

# ============================================================
# ایمیل (ارسال و دریافت)
# ============================================================
async def send_email(
    settings: UserSettings,
    to: str,
    subject: str,
    body: str,
    files: Optional[List[str]] = None,
) -> bool:
    try:
        if not settings.email_host or not settings.email_user or not settings.email_pass:
            save_history("خطا در ارسال ایمیل", "تنظیمات ایمیل ناقص است", "خطا", "error")
            return False

        if not to:
            to = settings.email_user

        msg = MIMEMultipart()
        msg["From"] = settings.email_user
        msg["To"] = to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        for file_path in files or []:
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{os.path.basename(file_path)}"',
                    )
                    msg.attach(part)

        server = smtplib.SMTP(settings.email_host, 587, timeout=30)
        server.starttls()
        server.login(settings.email_user, settings.email_pass)
        server.send_message(msg)
        server.quit()

        save_history("ایمیل ارسال شد", f"به: {to} | موضوع: {subject}", "موفق", "email")
        return True
    except Exception as e:
        save_history("خطا در ارسال ایمیل", str(e), "خطا", "error")
        return False

async def receive_emails(settings: UserSettings, limit: int = 5) -> List[Dict]:
    """دریافت ایمیل با IMAP"""
    try:
        if not settings.email_imap or not settings.email_user or not settings.email_pass:
            return []

        imap = imaplib.IMAP4_SSL(settings.email_imap, timeout=30)
        imap.login(settings.email_user, settings.email_pass)
        imap.select("INBOX")

        status, messages = imap.search(None, "UNSEEN")
        if status != "OK":
            imap.close()
            imap.logout()
            return []

        email_ids = messages[0].split()
        if not email_ids:
            return []

        email_ids = email_ids[-limit:]
        results = []

        for e_id in email_ids:
            try:
                status, msg_data = imap.fetch(e_id, "(RFC822)")
                if status != "OK":
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject = decode_header(msg.get("Subject", ""))[0][0]
                if isinstance(subject, bytes):
                    subject = subject.decode("utf-8", errors="ignore")

                from_ = decode_header(msg.get("From", ""))[0][0]
                if isinstance(from_, bytes):
                    from_ = from_.decode("utf-8", errors="ignore")

                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body = payload.decode("utf-8", errors="ignore")
                                break
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="ignore")

                results.append({
                    "subject": subject,
                    "from": from_,
                    "body": body,
                    "date": msg.get("Date", "")
                })

                imap.store(e_id, '+FLAGS', '\\Seen')

            except Exception as e:
                print(f"Error processing email {e_id}: {e}")
                continue

        imap.close()
        imap.logout()
        return results

    except Exception as e:
        save_history("خطا در دریافت ایمیل", str(e), "خطا", "error")
        return []

# ============================================================
# ساخت ورک‌فلو از متن (تلفیقی از هر دو فایل)
# ============================================================
PLATFORM_KEYWORDS = {
    "telegram": ["تلگرام", "telegram", "tg"],
    "bale": ["بله", "bale"],
    "rubika": ["روبیکا", "rubika"],
    "soroush": ["سروش", "soroush"],
    "gap": ["گپ", "gap", "igap", "آی‌گپ"],
    "instagram": ["اینستاگرام", "instagram", "ig", "اینستا"],
    "email": ["ایمیل", "email", "mail", "جیمیل", "gmail"],
}

PLATFORM_LABELS = {
    "telegram": "تلگرام",
    "bale": "بله",
    "rubika": "روبیکا",
    "soroush": "سروش",
    "gap": "گپ",
    "instagram": "اینستاگرام",
    "email": "ایمیل"
}

PLATFORM_ICONS = {
    "telegram": "📥",    "bale": "📥",
    "rubika": "📥",
    "soroush": "📥",
    "gap": "📥",
    "instagram": "📸",
    "email": "📧"
}

MEDIA_KEYWORDS = {
    "photo": ["عکس", "تصویر", "photo", "image", "picture"],
    "video": ["ویدئو", "ویدیو", "video", "film"],
    "document": ["فایل", "document", "pdf", "doc", "سند"],
}

INPUT_PLATFORMS = ["telegram", "bale", "rubika", "soroush", "gap", "email"]
OUTPUT_PLATFORMS = ["telegram", "bale", "rubika", "soroush", "gap", "instagram", "email"]

def detect_platform(text: str, key: str) -> bool:
    return any(k in text for k in PLATFORM_KEYWORDS.get(key, []))

def detect_media_type(text: str) -> str:
    for media_type, kws in MEDIA_KEYWORDS.items():
        if any(k in text for k in kws):
            return media_type
    return "text"

def generate_workflow_from_text(text: str) -> Dict[str, Any]:
    norm = _normalize_text(text)
    media_type = detect_media_type(norm)

    input_platforms = []
    output_platforms = []

    # Try to parse "از X به Y" pattern
    if "از" in norm:
        after_from = norm.split("از", 1)[1]

        if "به" in after_from:
            from_part = after_from.split("به", 1)[0]
            after_to = after_from.split("به", 1)[1]
        else:
            from_part = after_from
            after_to = ""

        for key in INPUT_PLATFORMS:
            if any(w in from_part for w in PLATFORM_KEYWORDS.get(key, [])):
                if key not in input_platforms:
                    input_platforms.append(key)

        for key in OUTPUT_PLATFORMS:
            if any(w in after_to for w in PLATFORM_KEYWORDS.get(key, [])):
                if key not in output_platforms:
                    output_platforms.append(key)

    if input_platforms and not output_platforms:
        for key in OUTPUT_PLATFORMS:
            if detect_platform(norm, key) and key not in input_platforms:
                output_platforms.append(key)

    if not input_platforms and not output_platforms:
        for key in INPUT_PLATFORMS:
            if detect_platform(norm, key):
                input_platforms.append(key)
                break
        for key in OUTPUT_PLATFORMS:
            if detect_platform(norm, key) and key not in input_platforms:
                output_platforms.append(key)

    if not input_platforms:
        input_platforms = ["manual"]

    if not output_platforms:
        output_platforms = ["log"]

    # Build nodes
    nodes = [
        {"id": "1", "type": "start", "data": {"label": "🚀 شروع", "mode": "manual"}, "position": {"x": 40, "y": 40}}
    ]
    edges = []
    node_id = 2
    last_id = "1"

    # Input node(s)
    if input_platforms and input_platforms[0] != "manual":
        for p in input_platforms[:1]:
            label = f"{PLATFORM_ICONS.get(p, '📥')} دریافت از {PLATFORM_LABELS.get(p, p)}"
            nodes.append({
                "id": str(node_id),
                "type": f"{p}_trigger",
                "data": {"label": label},
                "position": {"x": 40, "y": 120 + (node_id - 2) * 80}
            })
            edges.append({"source": last_id, "target": str(node_id)})
            last_id = str(node_id)
            node_id += 1
    else:
        nodes.append({
            "id": str(node_id),
            "type": "manual_trigger",
            "data": {"label": "📥 ورودی دستی"},
            "position": {"x": 40, "y": 120}
        })
        edges.append({"source": last_id, "target": str(node_id)})
        last_id = str(node_id)
        node_id += 1

    # Timer node (if specified)
    timer_match = re.search(r'هر\s*(\d+)\s*(دقیقه|ساعت|روز)', norm)
    if timer_match:
        val = int(timer_match.group(1))
        unit = timer_match.group(2)
        seconds = val * 60 if "دقیقه" in unit else val * 3600 if "ساعت" in unit else val * 86400
        nodes.append({
            "id": str(node_id),
            "type": "timer",
            "data": {"label": f"⏱ هر {val} {unit}", "interval": seconds},
            "position": {"x": 40, "y": 120 + (node_id - 2) * 80}
        })
        edges.append({"source": last_id, "target": str(node_id)})
        last_id = str(node_id)
        node_id += 1

    # Output nodes
    for p in output_platforms:
        if p == "log":
            nodes.append({
                "id": str(node_id),
                "type": "log",
                "data": {"label": "🧠 فقط تحلیل شد"},
                "position": {"x": 40, "y": 120 + (node_id - 2) * 80}
            })
        else:
            icon = "📤" if p != "instagram" else "📸"
            label = f"{icon} ارسال به {PLATFORM_LABELS.get(p, p)}"
            nodes.append({
                "id": str(node_id),
                "type": f"send_{p}",
                "data": {"label": label, "media_type": media_type},
                "position": {"x": 40, "y": 120 + (node_id - 2) * 80}
            })
        edges.append({"source": last_id, "target": str(node_id)})
        last_id = str(node_id)
        node_id += 1

    return {
        "success": True,
        "workflow": {
            "nodes": nodes,
            "edges": edges,
            "meta": {
                "source_text": text,
                "media_type": media_type,
            },
        },
    }

# ============================================================
# اجرای ورک‌فلو
# ============================================================
def build_graph(workflow: Dict[str, Any]):
    nodes = workflow.get("nodes", []) or []
    edges = workflow.get("edges", []) or []
    node_map = {n["id"]: n for n in nodes if "id" in n}
    # outgoing اکنون علاوه بر target، data یال (برچسب شاخه true/false/case) را هم نگه می‌دارد
    # تا گره‌های شرطی/سوییچ بتوانند مسیر درست را انتخاب کنند.
    outgoing = defaultdict(list)
    incoming = defaultdict(list)
    for edge in edges:
        s = str(edge.get("source"))
        t = str(edge.get("target"))
        if s and t:
            outgoing[s].append({"target": t, "data": edge.get("data") or {}})
            incoming[t].append(s)
    return node_map, outgoing, incoming

def _edge_allowed(edge_data: Dict[str, Any], branch: Optional[str]) -> bool:
    """آیا این یال باید دنبال شود؟ اگر یال برچسب «branch» نداشته باشد همیشه دنبال می‌شود
    (سازگار با ورک‌فلوهای خطی قدیمی). اگر گره مبدا شاخه مشخص کرده (شرط/سوییچ)، فقط یال
    هم‌نام آن شاخه دنبال می‌شود."""
    label = (edge_data or {}).get("branch")
    if not label:
        return True
    if branch is None:
        return False
    return str(label).strip().lower() == str(branch).strip().lower()

async def handle_send_node(
    platform: str,
    node: Dict[str, Any],
    context: Dict[str, Any],
    settings: UserSettings,
) -> str:
    msg = context.get("message", {}) or {}
    file_bytes = context.get("file_bytes")
    media_type = msg.get("type") or node.get("data", {}).get("media_type") or "text"
    caption = msg.get("caption") or node.get("data", {}).get("caption") or ""
    # BUGFIX: متن پیام باید از context["message"]["text"] بیاید، نه از input_text (دستور کاربر)
    text = msg.get("text") or caption or ""
    if not text:
        text = "پیام دریافتی"
    # برای voice/audio به صورت document ارسال می‌شود اگر پلتفرم مقصد پشتیبانی نکند
    if media_type in ("voice", "audio") and not file_bytes:
        media_type = "text"
        text = f"[پیام صوتی دریافت شد از طرف کاربر]"
    file_name = msg.get("file_name") or _guess_filename(media_type, msg)
    mime_type = msg.get("mime_type") or _guess_content_type(media_type, msg)

    if platform == "bale":
        # tg_token برای fallback URL در صورت شکست آپلود مستقیم
        tg_token = settings.tg_token or ""
        return await send_bale(settings.bale_token, settings.bale_chat, msg, file_bytes, tg_token)
    
    if platform == "rubika":
        base_url = (context.get("public_base_url") or settings.public_base_url or os.getenv("PUBLIC_BASE_URL", "") or "")
        return await send_rubika(settings.rubika_token, settings.rubika_chat, msg, file_bytes, settings.tg_token or "", base_url)
    
    if platform == "soroush":
        return await send_soroush(settings.soroush_token, settings.soroush_chat, msg, file_bytes)
    
    if platform == "gap":
        return await send_gap(settings.gap_token, settings.gap_chat, msg, file_bytes)
    
    if platform == "instagram":
        return await send_instagram(settings.ig_access_token, settings.ig_business_id, msg, file_bytes)
    
    if platform == "email":
        subject = node.get("data", {}).get("subject") or "پیام جدید"
        body = (
            f"نوع: {media_type}\n"
            f"متن: {text}\n"
            f"زمان: {now_iso()}\n"
        )
        ok = await send_email(settings, settings.email_user, subject, body)
        return "✅ ایمیل ارسال شد" if ok else "❌ ایمیل خطا"
    
    if platform == "telegram":
        if file_bytes and media_type in {"photo", "video", "document"}:
            return await send_telegram_media(settings.tg_token, settings.tg_chat, media_type, file_bytes, caption, file_name, mime_type)
        return await send_telegram_message(settings.tg_token, settings.tg_chat, text)
    
    return "❌ پلتفرم ناشناخته"

_TEMPLATE_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}")

def render_template(template: str, context: Dict[str, Any]) -> str:
    """جایگزینی ساده و امن عبارت‌های {{...}} با مقادیر context (بدون اجرای کد دلخواه).
    پشتیبانی از: {{input_text}}, {{message.text}}, {{message.caption}}, {{variables.NAME}}
    این معادل امن «Function/Expression node» در n8n است؛ برای جلوگیری از ریسک اجرای کد
    دلخواه روی سرور، عمداً از exec/eval استفاده نشده است."""
    if not template:
        return ""

    def _resolve(path: str):
        parts = path.split(".")
        cur: Any = context
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = getattr(cur, p, None)
            if cur is None:
                return ""
        return cur

    def _sub(m: "re.Match"):
        val = _resolve(m.group(1))
        return "" if val is None else str(val)

    return _TEMPLATE_RE.sub(_sub, str(template))

async def execute_node(node: Dict[str, Any], context: Dict[str, Any], settings: UserSettings) -> Dict[str, Any]:
    """هر گره یک دیکشنری برمی‌گرداند: {message, ok, branch}.
    branch فقط برای گره‌های شرطی/سوییچ پر می‌شود و مشخص می‌کند کدام یال(های) خروجی دنبال شوند."""
    node_type = node.get("type", "unknown")
    data = node.get("data", {}) or {}

    def ok(message: str, branch: Optional[str] = None):
        return {"message": message, "ok": True, "branch": branch}

    def fail(message: str, branch: Optional[str] = None):
        return {"message": message, "ok": False, "branch": branch}

    if node_type in {"start", "telegram_trigger", "bale_trigger", "rubika_trigger",
                     "soroush_trigger", "gap_trigger", "email_trigger",
                     "webhook_trigger", "manual_trigger"}:
        return ok(f"✅ {node_type}")

    if node_type == "timer":
        # BUGFIX: دیگر اینجا sleep نمی‌کنیم؛ زمان‌بندی واقعی توسط create_timer_listener در پس‌زمینه
        # انجام می‌شود تا درخواست /execute یا اجرای دستی به‌مدت interval بلاک نشود.
        interval = data.get("interval", 60)
        return ok(f"⏱ تایمر (هر {interval} ثانیه در پس‌زمینه اجرا می‌شود)")

    if node_type == "delay":
        seconds = float(data.get("seconds", 1))
        await asyncio.sleep(max(0.0, min(seconds, 300)))  # سقف ۵ دقیقه برای جلوگیری از بلاک طولانی درخواست
        return ok(f"⏳ تاخیر {seconds} ثانیه")

    if node_type == "log":
        message = render_template(data.get("message") or "{{input_text}}", context)
        save_history("لاگ", message, "موفق", "info")
        return ok("✅ لاگ ثبت شد")

    if node_type == "condition":
        haystack_raw = context.get("input_text") or (context.get("message") or {}).get("text") or ""
        operator = str(data.get("operator") or ("contains_any" if data.get("contains_any") else "contains")).lower()
        compare_value = data.get("value")
        passed = False
        try:
            if operator == "contains_any":
                haystack = _normalize_text(haystack_raw)
                options = [_normalize_text(x) for x in (data.get("contains_any") or [])]
                passed = any(x and x in haystack for x in options)
            elif operator == "contains":
                passed = _normalize_text(str(compare_value or "")) in _normalize_text(haystack_raw)
            elif operator == "not_contains":
                passed = _normalize_text(str(compare_value or "")) not in _normalize_text(haystack_raw)
            elif operator == "equals":
                passed = _normalize_text(haystack_raw) == _normalize_text(str(compare_value or ""))
            elif operator == "not_equals":
                passed = _normalize_text(haystack_raw) != _normalize_text(str(compare_value or ""))
            elif operator == "regex":
                passed = bool(re.search(str(compare_value or ""), haystack_raw))
            elif operator in ("greater_than", "less_than"):
                left = float(re.sub(r"[^\d.\-]", "", haystack_raw) or 0)
                right = float(compare_value or 0)
                passed = left > right if operator == "greater_than" else left < right
            else:
                passed = True
        except Exception:
            passed = False
        context["condition_passed"] = passed  # سازگاری با کد قدیمی
        branch = "true" if passed else "false"
        return ok("✅ شرط برقرار شد (true)" if passed else "⛔ شرط رد شد (false)", branch=branch)

    if node_type == "switch":
        haystack = _normalize_text(context.get("input_text") or (context.get("message") or {}).get("text") or "")
        cases = data.get("cases") or []
        matched_label = None
        for case in cases:
            val = _normalize_text(str(case.get("value", "")))
            if val and val in haystack:
                matched_label = str(case.get("label") or case.get("value"))
                break
        branch = matched_label or "default"
        return ok(f"🔀 مسیر انتخاب‌شده: {branch}", branch=branch)

    if node_type == "merge":
        # گره اتصال/همگرایی شاخه‌ها؛ فقط یک نقطه‌ی بصری/منطقی برای یکی‌شدن چند مسیر است.
        return ok("🔗 مسیرها به هم متصل شدند")

    if node_type in ("set_value", "set_multi"):
        pairs = data.get("values") if node_type == "set_multi" else [{"key": data.get("key", "value"), "value": data.get("value", "")}]
        set_keys = []
        for pair in (pairs or []):
            key = str(pair.get("key") or "value")
            value = render_template(str(pair.get("value", "")), context)
            context["variables"][key] = value
            set_keys.append(key)
        return ok(f"✅ مقدار(های) {', '.join(set_keys)} تنظیم شد")

    if node_type == "http_request":
        method = str(data.get("method", "GET")).upper()
        url = render_template(data.get("url", ""), context)
        headers = data.get("headers") or {}
        payload = data.get("body")
        if not url:
            return fail("❌ آدرس URL خالی است")
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    method, url, headers=headers,
                    json=payload if isinstance(payload, dict) else None,
                    data=payload if isinstance(payload, str) else None,
                ) as resp:
                    text = await resp.text()
                    context["http_response"] = {"status": resp.status, "text": text[:4000]}
                    context["variables"]["http_response"] = text[:4000]
                    if resp.status >= 400:
                        return fail(f"❌ HTTP {resp.status}")
                    return ok(f"✅ HTTP {resp.status}")
        except Exception as e:
            return fail(f"❌ خطای HTTP: {str(e)[:150]}")

    if node_type == "email_trigger":
        emails = await receive_emails(settings, limit=5)
        context["emails"] = emails
        return ok(f"✅ {len(emails)} ایمیل دریافت شد")

    if node_type.startswith("send_"):
        platform = node_type.replace("send_", "", 1)
        result = await handle_send_node(platform, node, context, settings)
        success = result.startswith("✅")
        save_history("ارسال پیام", f"{platform}: {result}", "موفق" if success else "خطا", "send")
        return ok(result) if success else fail(result)

    return fail(f"⚠️ نوع گره پشتیبانی نشده: {node_type}")

async def run_workflow(workflow: Dict[str, Any], context: Dict[str, Any], settings: UserSettings) -> List[Dict[str, Any]]:
    """اجرای گراف ورک‌فلو با پشتیبانی از شاخه‌های true/false (IF) و سوییچ چندمسیره،
    ثبت نتیجه‌ی ساختاریافته به‌ازای هر گره (برای نمایش وضعیت موفق/خطا در رابط کاربری)
    و مدیریت خطا به‌ازای هر گره به‌جای خراب‌شدن کل اجرا (continue_on_fail)."""
    node_map, outgoing, incoming = build_graph(workflow)
    if not node_map:
        return [{"node_id": "-", "type": "-", "label": "-", "status": "error", "message": "❌ ورک‌فلو خالی است", "ms": 0}]

    roots = [node_id for node_id in node_map if not incoming.get(node_id)]
    if not roots:
        roots = [sorted(node_map.keys(), key=lambda x: int(x) if str(x).isdigit() else x)[0]]

    results: List[Dict[str, Any]] = []
    visited: set[str] = set()

    async def dfs(node_id: str):
        if node_id in visited or node_id not in node_map:
            return
        visited.add(node_id)
        node = node_map[node_id]
        node_type = node.get("type", "unknown")
        label = (node.get("data") or {}).get("label") or node_type
        started = time.time()
        branch = None
        try:
            outcome = await execute_node(node, context, settings)
            branch = outcome.get("branch")
            status = "success" if outcome.get("ok") else "error"
            message = outcome.get("message", "")
        except Exception as e:
            status = "error"
            message = f"❌ خطای اجرا: {str(e)[:200]}"
            save_history("خطای گره", f"{node_type}: {message}", "خطا", "error")

        results.append({
            "node_id": node_id,
            "type": node_type,
            "label": label,
            "status": status,
            "message": message,
            "ms": int((time.time() - started) * 1000),
        })

        continue_on_fail = bool((node.get("data") or {}).get("continue_on_fail", False))
        if status == "error" and not continue_on_fail:
            return

        for edge in outgoing.get(node_id, []):
            if _edge_allowed(edge.get("data") or {}, branch):
                await dfs(edge["target"])

    for root in roots:
        await dfs(root)

    return results

# ============================================================
# استخراج پیام از پلتفرم‌های مختلف
# ============================================================
def extract_message(message: Dict[str, Any], platform: str = "telegram") -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "type": "text",
        "text": "",
        "caption": "",
        "file_id": None,
        "file_name": "",
        "mime_type": "",
    }

    if platform in ["telegram", "bale"]:
        if "text" in message:
            payload["text"] = message.get("text", "")
            return payload
        if "photo" in message and message["photo"]:
            payload["type"] = "photo"
            payload["file_id"] = message["photo"][-1]["file_id"]
            payload["caption"] = message.get("caption", "")
            return payload
        if "video" in message and message["video"]:
            payload["type"] = "video"
            payload["file_id"] = message["video"]["file_id"]
            payload["caption"] = message.get("caption", "")
            payload["file_name"] = message["video"].get("file_name", "")
            payload["mime_type"] = message["video"].get("mime_type", "video/mp4")
            return payload
        if "voice" in message and message["voice"]:
            payload["type"] = "voice"
            payload["file_id"] = message["voice"]["file_id"]
            payload["mime_type"] = message["voice"].get("mime_type", "audio/ogg")
            payload["file_name"] = "voice.ogg"
            return payload
        if "audio" in message and message["audio"]:
            payload["type"] = "audio"
            payload["file_id"] = message["audio"]["file_id"]
            payload["mime_type"] = message["audio"].get("mime_type", "audio/mpeg")
            payload["file_name"] = message["audio"].get("file_name", "audio.mp3")
            return payload
        if "document" in message and message["document"]:
            payload["type"] = "document"
            payload["file_id"] = message["document"]["file_id"]
            payload["caption"] = message.get("caption", "")
            payload["file_name"] = message["document"].get("file_name", "")
            payload["mime_type"] = message["document"].get("mime_type", "application/octet-stream")
            return payload

    elif platform == "rubika":
        if message.get("new_message"):
            nm = message["new_message"]
            payload["text"] = nm.get("text", "")
            return payload
        payload["text"] = message.get("text", "")
        return payload

    elif platform == "soroush":
        payload["text"] = message.get("body", "") or message.get("text", "")
        return payload

    elif platform == "gap":
        payload["text"] = message.get("data", "") or message.get("text", "")
        return payload

    # Fallback
    payload["text"] = (message.get("text", "") or message.get("caption", "") or
                       message.get("body", "") or message.get("message", "") or
                       message.get("data", ""))
    return payload

# ============================================================
# Runtime state
# ============================================================
runtime: Dict[str, Any] = {
    "running": False,
    "listener_tasks": {},
    "workflow": None,
    "settings": None,
    "offsets": {},
    "last_error": None,
    "public_base_url": os.getenv("PUBLIC_BASE_URL", ""),
    "media_cleanup_task": None,
    # وب‌هوک‌های فعال: token -> {"workflow":..., "settings":..., "node_id":..., "created_at":...}
    # فقط در RAM نگه‌داری می‌شود و با /stop یا ری‌استارت سرور پاک می‌شود (سازگار با سیاست local-only).
    "webhooks": {},
}

# ============================================================
# شنونده‌های پلتفرم‌ها
# ============================================================
async def create_telegram_listener(platform: str, token: str, settings: UserSettings, workflow: Dict[str, Any]):
    """شنونده برای پلتفرم‌های Telegram-compatible (تلگرام و بله)"""
    runtime["running"] = True
    add_notification("سیستم روشن شد", f"شنونده {PLATFORM_LABELS.get(platform, platform)} فعال شد")

    offset = int(runtime["offsets"].get(platform, 0))
    
    base_url = "https://api.telegram.org" if platform == "telegram" else "https://tapi.bale.ai"
    get_updates_url = f"{base_url}/bot{token}/getUpdates"

    async with aiohttp.ClientSession() as session:
        while runtime["running"]:
            try:
                async with session.get(
                    get_updates_url,
                    params={"offset": offset, "timeout": 20, "allowed_updates": ["message"]},
                    timeout=aiohttp.ClientTimeout(total=25)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(2)
                        continue

                    data = await resp.json()
                    for update in data.get("result") or []:
                        offset = int(update["update_id"]) + 1
                        runtime["offsets"][platform] = offset

                        message = update.get("message")
                        if not message:
                            continue

                        msg = extract_message(message, platform)
                        file_bytes = None
                        if msg.get("file_id"):
                            file_bytes = await download_file(token, msg["file_id"], platform)

                        context = {
                            "input_text": msg.get("text") or msg.get("caption") or "",
                            "message": msg,
                            "file_bytes": file_bytes,
                            "variables": {},
                            "extra_data": {"platform": platform},
                            "public_base_url": runtime.get("public_base_url", ""),
                        }

                        try:
                            results = await run_workflow(workflow, context, settings)
                            save_history(
                                "اجرای خودکار",
                                f"{platform}: {msg.get('type')} -> {_summarize_results(results)}",
                                "موفق",
                                "trigger",
                            )
                            add_notification(
                                f"پیام از {PLATFORM_LABELS.get(platform, platform)}",
                                f"{msg.get('text', msg.get('caption', 'پیام دریافتی'))[:50]}..."
                            )
                        except Exception as exc:
                            runtime["last_error"] = str(exc)
                            save_history("اجرای خودکار", str(exc), "خطا", "error")

            except asyncio.CancelledError:
                break
            except Exception as exc:
                runtime["last_error"] = str(exc)
                save_history(f"شنونده {platform}", str(exc), "خطا", "error")
                await asyncio.sleep(3)

    runtime["running"] = False

async def create_rubika_listener(token: str, settings: UserSettings, workflow: Dict[str, Any]):
    """شنونده اختصاصی روبیکا"""
    runtime["running"] = True
    add_notification("سیستم روشن شد", "شنونده روبیکا فعال شد")

    start_id = int(runtime["offsets"].get("rubika", 0))

    async with aiohttp.ClientSession() as session:
        while runtime["running"]:
            try:
                async with session.post(
                    f"https://botapi.rubika.ir/v3/{token}/getUpdates",
                    json={"start_id": start_id, "limit": 10},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3)
                        continue

                    data = await resp.json()
                    updates = data.get("result") or data.get("updates") or []

                    for update in updates:
                        msg = update.get("update") or update
                        if not msg:
                            continue

                        update_id = msg.get("update_id") or msg.get("id")
                        if update_id:
                            start_id = int(update_id) + 1
                            runtime["offsets"]["rubika"] = start_id

                        m = extract_message(msg, "rubika")
                        context = {
                            "input_text": m.get("text") or "",
                            "message": m,
                            "file_bytes": None,
                            "variables": {},
                            "extra_data": {"platform": "rubika"},
                            "public_base_url": runtime.get("public_base_url", ""),
                        }

                        try:
                            results = await run_workflow(workflow, context, settings)
                            save_history("اجرای خودکار", f"rubika: {m.get('type')} -> {_summarize_results(results)}", "موفق", "trigger")
                            add_notification(
                                "پیام از روبیکا",
                                f"{m.get('text', 'پیام دریافتی')[:50]}..."
                            )
                        except Exception as e:
                            runtime["last_error"] = str(e)
                            save_history("اجرای خودکار", str(e), "خطا", "error")

            except asyncio.CancelledError:
                break
            except Exception as e:
                runtime["last_error"] = str(e)
                save_history("شنونده rubika", str(e), "خطا", "error")
                await asyncio.sleep(3)

    runtime["running"] = False

async def create_soroush_listener(token: str, settings: UserSettings, workflow: Dict[str, Any]):
    """شنونده اختصاصی سروش"""
    runtime["running"] = True
    add_notification("سیستم روشن شد", "شنونده سروش فعال شد")

    async with aiohttp.ClientSession() as session:
        while runtime["running"]:
            try:
                async with session.get(
                    f"https://bot.sapp.ir/{token}/getUpdates",
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue

                    data = await resp.json()
                    updates = data.get("result") or data.get("updates") or []

                    for update in updates:
                        msg = update.get("message") or update
                        if not msg:
                            continue

                        update_id = msg.get("update_id") or msg.get("id")
                        if update_id:
                            runtime["offsets"]["soroush"] = int(update_id) + 1

                        m = extract_message(msg, "soroush")
                        context = {
                            "input_text": m.get("text") or "",
                            "message": m,
                            "file_bytes": None,
                            "variables": {},
                            "extra_data": {"platform": "soroush"},
                            "public_base_url": runtime.get("public_base_url", ""),
                        }

                        try:
                            results = await run_workflow(workflow, context, settings)
                            save_history("اجرای خودکار", f"soroush: {m.get('type')} -> {_summarize_results(results)}", "موفق", "trigger")
                            add_notification(
                                "پیام از سروش",
                                f"{m.get('text', 'پیام دریافتی')[:50]}..."
                            )
                        except Exception as e:
                            runtime["last_error"] = str(e)
                            save_history("اجرای خودکار", str(e), "خطا", "error")

                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                runtime["last_error"] = str(e)
                save_history("شنونده soroush", str(e), "خطا", "error")
                await asyncio.sleep(5)

    runtime["running"] = False

async def create_gap_listener(token: str, settings: UserSettings, workflow: Dict[str, Any]):
    """شنونده اختصاصی گپ"""
    runtime["running"] = True
    add_notification("سیستم روشن شد", "شنونده گپ فعال شد")

    async with aiohttp.ClientSession() as session:
        while runtime["running"]:
            try:
                headers = {"token": token}
                async with session.get(
                    "https://api.gap.im/v1/getUpdates",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue

                    data = await resp.json()
                    updates = data.get("result") or data.get("updates") or []

                    for update in updates:
                        msg = update.get("message") or update
                        if not msg:
                            continue

                        update_id = msg.get("update_id") or msg.get("id")
                        if update_id:
                            runtime["offsets"]["gap"] = int(update_id) + 1

                        m = extract_message(msg, "gap")
                        context = {
                            "input_text": m.get("text") or "",
                            "message": m,
                            "file_bytes": None,
                            "variables": {},
                            "extra_data": {"platform": "gap"},
                            "public_base_url": runtime.get("public_base_url", ""),
                        }

                        try:
                            results = await run_workflow(workflow, context, settings)
                            save_history("اجرای خودکار", f"gap: {m.get('type')} -> {_summarize_results(results)}", "موفق", "trigger")
                            add_notification(
                                "پیام از گپ",
                                f"{m.get('text', 'پیام دریافتی')[:50]}..."
                            )
                        except Exception as e:
                            runtime["last_error"] = str(e)
                            save_history("اجرای خودکار", str(e), "خطا", "error")

                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                runtime["last_error"] = str(e)
                save_history("شنونده gap", str(e), "خطا", "error")
                await asyncio.sleep(5)

    runtime["running"] = False

def _summarize_results(results: List[Dict[str, Any]]) -> str:
    """خلاصه‌ی خوانا از نتایج ساختاریافته برای ثبت در تاریخچه/لاگ."""
    parts = []
    for r in results or []:
        if isinstance(r, dict):
            parts.append(f"{r.get('type')}:{r.get('status')}")
        else:
            parts.append(str(r))
    return ", ".join(parts) if parts else "بدون نتیجه"

async def create_timer_listener(node_id: str, interval: int, settings: UserSettings, workflow: Dict[str, Any]):
    """BUGFIX: تایمر قبلاً با asyncio.sleep داخل execute_node درخواست /execute را برای کل بازه زمانی
    (حتی ساعت‌ها) بلاک می‌کرد. حالا تایمر مثل بقیه پلتفرم‌ها یک تسک پس‌زمینه‌ی جداست که هر «interval»
    ثانیه کل ورک‌فلو را (بدون بلاک کردن HTTP request) اجرا می‌کند."""
    runtime["running"] = True
    interval = max(1, int(interval or 60))
    add_notification("سیستم روشن شد", f"تایمر هر {interval} ثانیه فعال شد")
    while runtime["running"]:
        try:
            await asyncio.sleep(interval)
            if not runtime["running"]:
                break
            context = {
                "input_text": "",
                "message": {"type": "text", "text": ""},
                "file_bytes": None,
                "variables": {},
                "extra_data": {"trigger": "timer"},
                "public_base_url": runtime.get("public_base_url", ""),
            }
            results = await run_workflow(workflow, context, settings)
            save_history("تایمر", _summarize_results(results), "موفق", "trigger")
        except asyncio.CancelledError:
            break
        except Exception as e:
            runtime["last_error"] = str(e)
            save_history("تایمر", str(e), "خطا", "error")
    runtime["running"] = False

async def create_email_listener(settings: UserSettings, workflow: Dict[str, Any], interval: int = 45):
    """شنونده‌ی ایمیل: هر «interval» ثانیه با IMAP بررسی می‌کند و ایمیل‌های خوانده‌نشده جدید را
    وارد ورک‌فلو می‌کند (قبلاً ایمیل فقط به‌صورت دستی/درخواستی قابل بررسی بود، نه به‌عنوان trigger زنده)."""
    runtime["running"] = True
    add_notification("سیستم روشن شد", "شنونده ایمیل فعال شد")
    while runtime["running"]:
        try:
            emails = await receive_emails(settings, limit=5)
            for mail in emails:
                context = {
                    "input_text": mail.get("body") or mail.get("subject") or "",
                    "message": {
                        "type": "text",
                        "text": mail.get("body") or "",
                        "caption": mail.get("subject") or "",
                    },
                    "file_bytes": None,
                    "variables": {},
                    "extra_data": {"trigger": "email", "from": mail.get("from"), "subject": mail.get("subject")},
                    "public_base_url": runtime.get("public_base_url", ""),
                }
                results = await run_workflow(workflow, context, settings)
                save_history("اجرای خودکار", f"ایمیل: {_summarize_results(results)}", "موفق", "trigger")
                add_notification("ایمیل جدید", (mail.get("subject") or "بدون موضوع")[:50])
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            runtime["last_error"] = str(e)
            save_history("شنونده ایمیل", str(e), "خطا", "error")
            await asyncio.sleep(interval)
    runtime["running"] = False

# ============================================================
# API
# ============================================================
@app.get("/health")
async def health():
    listeners_count = sum(1 for t in runtime["listener_tasks"].values() if t and not t.done())
    return {
        "ok": True,
        "running": runtime["running"],
        "listeners": listeners_count,
        "last_error": runtime["last_error"],
        "storage": "localStorage on device + RAM-only runtime",
        "temp_media": len(TEMP_MEDIA),
        "version": "4.0-pro",
        "platforms": list(PLATFORM_KEYWORDS.keys()),
        "webhooks": [
            {"token": tok, "url": f"{runtime.get('public_base_url','').rstrip('/')}/webhook/{tok}"}
            for tok in runtime["webhooks"].keys()
        ],
    }

@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse("<h1>index.html not found</h1>", status_code=404)
    return HTMLResponse(index_path.read_text(encoding="utf-8"))

@app.get("/download/{token}/{filename:path}")
async def download_temp_media(token: str, filename: str = ""):
    """لینک دانلود مستقیم و یک‌بارمصرف برای ویدئوی روبیکا.
    بعد از پاسخ دانلود، فایل و رکورد RAM حذف می‌شود.
    """
    info = TEMP_MEDIA.pop(token, None)
    if not info:
        raise HTTPException(status_code=404, detail="لینک نامعتبر است یا قبلاً دانلود/حذف شده")
    path = Path(info.get("path", ""))
    if info.get("expires_at", 0) < time.time():
        _delete_quietly(path)
        raise HTTPException(status_code=410, detail="مهلت لینک دانلود تمام شده و فایل حذف شد")
    if not path.exists():
        raise HTTPException(status_code=404, detail="فایل پیدا نشد")

    def _cleanup():
        _delete_quietly(path)

    return FileResponse(
        path,
        media_type=info.get("mime_type") or "application/octet-stream",
        filename=info.get("filename") or filename or "download.bin",
        headers={"Cache-Control": "no-store, max-age=0", "X-Content-Type-Options": "nosniff"},
        background=BackgroundTask(_cleanup),
    )

@app.post("/generate-workflow")
async def generate_workflow(request: Dict[str, Any]):
    text = str(request.get("text", ""))
    return generate_workflow_from_text(text)

@app.api_route("/webhook/{token}", methods=["GET", "POST"])
async def trigger_webhook(token: str, request: Request):
    """گیرنده‌ی وب‌هوک واقعی (مثل n8n): هر سرویس خارجی می‌تواند با یک درخواست GET/POST به این
    آدرس، ورک‌فلوی متصل را اجرا کند. آدرس با فعال‌سازی جریانی که گره «وب‌هوک» دارد ساخته می‌شود
    (بخش /execute) و فقط در RAM نگه‌داری می‌شود."""
    reg = runtime["webhooks"].get(token)
    if not reg:
        raise HTTPException(status_code=404, detail="این آدرس وب‌هوک فعال نیست یا منقضی شده")

    body: Dict[str, Any] = {}
    try:
        if request.method == "POST":
            body = await request.json()
    except Exception:
        body = {}
    query = dict(request.query_params)

    input_text = str(body.get("text") or body.get("input_text") or query.get("text") or "")
    context = {
        "input_text": input_text,
        "message": {"type": "text", "text": input_text},
        "file_bytes": None,
        "variables": {},
        "extra_data": {"trigger": "webhook", "body": body, "query": query},
        "public_base_url": runtime.get("public_base_url", ""),
    }
    results = await run_workflow(reg["workflow"], context, reg["settings"])
    save_history("وب‌هوک اجرا شد", _summarize_results(results), "موفق", "trigger")
    return {"success": True, "results": results}

@app.post("/execute")
async def execute_workflow(req: ExecuteRequest, request: Request):
    settings = req.settings
    runtime["public_base_url"] = (settings.public_base_url or os.getenv("PUBLIC_BASE_URL", "") or str(request.base_url).rstrip("/"))
    # Load saved settings if not provided
    setting_keys = [
        "tg_token", "tg_chat", 
        "bale_token", "bale_chat",
        "rubika_token", "rubika_chat",
        "soroush_token", "soroush_chat",
        "gap_token", "gap_chat",
        "ig_access_token", "ig_business_id",
        "email_host", "email_user", "email_pass", "email_imap"
    ]
    for key in setting_keys:
        if not getattr(settings, key, None):
            setattr(settings, key, get_setting(key))

    runtime["workflow"] = req.workflow
    runtime["settings"] = settings

    all_nodes = req.workflow.get("nodes", []) or []

    if req.test_mode:
        # اجرای تستی: هیچ شنونده/تایمر/وب‌هوکی فعال نمی‌شود؛ فقط همین یک‌بار با ورودی نمونه اجرا می‌شود.
        context = {
            "input_text": req.input_text or req.extra_data.get("input_text", ""),
            "message": {"type": "text", "text": req.input_text or ""},
            "file_bytes": None,
            "variables": {},
            "extra_data": req.extra_data or {},
            "public_base_url": runtime.get("public_base_url", ""),
        }
        results = await run_workflow(req.workflow, context, settings)
        overall_ok = all(r.get("status") != "error" for r in results) if results else False
        return {"success": overall_ok, "results": results, "mode": "test"}

    # Detect input platforms from workflow (وب‌هوک و تایمر جدا مدیریت می‌شوند)    input_platforms = []
    webhook_node = None
    timer_node = None
    for node in all_nodes:
        node_type = node.get("type", "")
        if node_type == "webhook_trigger":
            webhook_node = node
        elif node_type == "timer":
            timer_node = node
        elif node_type.endswith("_trigger") and node_type != "manual_trigger":
            platform = node_type.replace("_trigger", "")
            if platform not in input_platforms:
                input_platforms.append(platform)

    # Stop previous listeners
    for platform, task in list(runtime["listener_tasks"].items()):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except:
                pass
    runtime["listener_tasks"] = {}
    runtime["running"] = False

    # Start new listeners for each input platform
    listeners_started = []
    extra_messages = []

    # نگاشت نام پلتفرم به نام فیلد در UserSettings
    PLATFORM_FIELD_MAP = {
        "telegram": ("tg_token", "tg_chat"),
        "bale":     ("bale_token", "bale_chat"),
        "rubika":   ("rubika_token", "rubika_chat"),
        "soroush":  ("soroush_token", "soroush_chat"),
        "gap":      ("gap_token", "gap_chat"),
        "email":    ("email_user", "email_imap"),
    }

    for platform in input_platforms:
        token_field, chat_field = PLATFORM_FIELD_MAP.get(platform, (f"{platform}_token", f"{platform}_chat"))
        token = getattr(settings, token_field, "") or ""
        chat  = getattr(settings, chat_field, "") or ""

        if not token:
            save_history("execute", f"توکن پلتفرم {platform} ({token_field}) خالی است", "خطا", "error")
            continue

        if platform in ["telegram", "bale"]:
            runtime["offsets"][platform] = 0
            task = asyncio.create_task(
                create_telegram_listener(platform, token, settings, req.workflow)
            )
            runtime["listener_tasks"][platform] = task
            listeners_started.append(platform)

        elif platform == "rubika":
            runtime["offsets"]["rubika"] = 0
            task = asyncio.create_task(
                create_rubika_listener(token, settings, req.workflow)
            )
            runtime["listener_tasks"]["rubika"] = task
            listeners_started.append("rubika")

        elif platform == "soroush":
            runtime["offsets"]["soroush"] = 0
            task = asyncio.create_task(
                create_soroush_listener(token, settings, req.workflow)
            )
            runtime["listener_tasks"]["soroush"] = task
            listeners_started.append("soroush")

        elif platform == "gap":
            runtime["offsets"]["gap"] = 0
            task = asyncio.create_task(
                create_gap_listener(token, settings, req.workflow)
            )
            runtime["listener_tasks"]["gap"] = task
            listeners_started.append("gap")

        elif platform == "email":
            # BUGFIX: قبلاً ایمیل هیچ‌وقت به‌عنوان trigger زنده فعال نمی‌شد (فقط on-demand بود).
            # حالا مثل بقیه‌ی پلتفرم‌ها یک شنونده‌ی پس‌زمینه (poll هر ۴۵ ثانیه) دارد.
            task = asyncio.create_task(create_email_listener(settings, req.workflow))
            runtime["listener_tasks"]["email"] = task
            listeners_started.append("email")

    # BUGFIX: تایمر قبلاً «trigger واقعی» شناخته نمی‌شد و اگر تنها گره ورودی بود،
    # مستقیم وارد مسیر «اجرای دستی» با sleep طولانی داخل درخواست HTTP می‌شد.
    # حالا مثل بقیه پلتفرم‌ها یک تسک پس‌زمینه‌ی جدا می‌گیرد.
    if timer_node is not None:
        interval = int((timer_node.get("data") or {}).get("interval", 60))
        task = asyncio.create_task(create_timer_listener(timer_node.get("id", ""), interval, settings, req.workflow))
        runtime["listener_tasks"]["timer"] = task
        listeners_started.append("timer")
        extra_messages.append(f"⏱ تایمر هر {interval} ثانیه در پس‌زمینه فعال شد")

    # وب‌هوک: به‌جای نیاز به توکن پلتفرم، یک آدرس یک‌بار ساخته و در RAM ثبت می‌شود.
    if webhook_node is not None:
        token = secrets.token_urlsafe(16)
        runtime["webhooks"][token] = {
            "workflow": req.workflow,
            "settings": settings,
            "node_id": webhook_node.get("id", ""),
            "created_at": now_iso(),
        }
        base = (runtime.get("public_base_url") or "").rstrip("/")
        webhook_url = f"{base}/webhook/{token}"
        listeners_started.append("webhook")
        extra_messages.append(f"🌐 وب‌هوک فعال شد: {webhook_url}")
        save_history("وب‌هوک", f"وب‌هوک ثبت شد: {webhook_url}", "موفق", "trigger")

    if listeners_started:
        platform_names = [PLATFORM_LABELS.get(p, p) for p in listeners_started if p not in ("timer", "webhook")]
        msgs = []
        if platform_names:
            msgs.append(f"🟢 شنونده‌های {', '.join(platform_names)} فعال شدند")
        msgs.extend(extra_messages)
        return {
            "success": True,
            "results": msgs,
            "mode": "listener",
            "listeners": listeners_started,
            "webhook_url": webhook_url if webhook_node is not None else None,
        }

    # اگر trigger node داشت ولی token نداشت - خطا با راهنمایی دقیق‌تر
    if input_platforms:
        details = []
        for p in input_platforms:
            tf, _ = PLATFORM_FIELD_MAP.get(p, (f"{p}_token", f"{p}_chat"))
            val = getattr(settings, tf, "") or ""
            details.append(f"{PLATFORM_LABELS.get(p,p)} ({tf}={'✓' if val else '✗'})")
        return {
            "success": False,
            "results": [f"⚠️ توکن تنظیم نشده: {', '.join(details)}"],
            "mode": "error",
        }

    # اجرای دستی - فقط وقتی هیچ trigger node واقعی وجود نداشته باشد
    # BUGFIX: جلوگیری از ارسال دستور کاربر به عنوان پیام
    has_trigger = any(
        n.get("type", "").endswith("_trigger") and n.get("type") != "manual_trigger"
        for n in all_nodes
    )
    if has_trigger:
        return {
            "success": False,
            "results": ["⚠️ ورک‌فلو دارای trigger است اما توکن پلتفرم تنظیم نشده"],
            "mode": "error",
        }

    context = {
        "input_text": req.input_text or req.extra_data.get("input_text", ""),
        "message": {"type": "text", "text": req.input_text or ""},
        "file_bytes": None,
        "variables": {},
        "extra_data": req.extra_data or {},
        "public_base_url": runtime.get("public_base_url", ""),
    }
    results = await run_workflow(req.workflow, context, settings)
    overall_ok = all(r.get("status") != "error" for r in results) if results else False
    return {"success": overall_ok, "results": results, "mode": "once"}

@app.post("/stop")
async def stop_workflow():
    runtime["running"] = False

    for platform, task in list(runtime["listener_tasks"].items()):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except:
                pass

    runtime["listener_tasks"] = {}
    runtime["settings"] = None
    runtime["webhooks"] = {}
    return {"success": True, "message": "🔴 سیستم متوقف شد؛ داده‌های اجرایی از RAM پاک شد."}

@app.get("/api/history")
async def api_history(limit: int = 50):
    return {"history": list_history(limit=limit)}

@app.get("/api/notifications")
async def api_notifications(limit: int = 20):
    return {"notifications": list_notifications(limit=limit)}

@app.get("/api/stats")
async def api_stats():
    with db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        success = conn.execute("SELECT COUNT(*) FROM history WHERE status='موفق'").fetchone()[0]
        failed = conn.execute("SELECT COUNT(*) FROM history WHERE status='خطا'").fetchone()[0]
    
    listeners_count = sum(1 for t in runtime["listener_tasks"].values() if t and not t.done())
    return {
        "total": total,
        "success": success,
        "failed": failed,
        "running": runtime["running"],
        "listeners": listeners_count
    }

@app.get("/api/settings")
async def api_get_settings():
    keys = [
        "tg_token", "tg_chat", 
        "bale_token", "bale_chat",
        "rubika_token", "rubika_chat",
        "soroush_token", "soroush_chat",
        "gap_token", "gap_chat",
        "ig_access_token", "ig_business_id",
        "email_host", "email_user", "email_pass", "email_imap",
        "ai_api_key", "ai_base_url", "public_base_url"
    ]
    # برای حریم خصوصی همیشه خالی برمی‌گردد؛ UI از localStorage گوشی می‌خواند.
    return {"settings": {k: "" for k in keys}, "local_only": True}

@app.post("/api/settings")
async def api_save_settings(settings: UserSettings):
    # ذخیره سمت سرور غیرفعال است. این endpoint فقط برای سازگاری قدیمی پاسخ موفق می‌دهد.
    return {"success": True, "message": "✅ تنظیمات فقط روی گوشی شما ذخیره می‌شوند؛ سرور چیزی ذخیره نکرد."}

@app.get("/api/workflows")
async def api_list_workflows():
    # جریان‌ها در نسخه حریم‌خصوصی فقط در localStorage گوشی ذخیره می‌شوند.
    return {"workflows": [], "local_only": True}

@app.post("/api/workflows")
async def api_save_workflow(req: SaveWorkflowRequest):
    # ذخیره سمت سرور غیرفعال است تا هیچ اطلاعات کاربر خارج از گوشی ذخیره نشود.
    return {"success": False, "local_only": True, "message": "ذخیره جریان فقط روی گوشی انجام می‌شود."}

@app.get("/api/workflows/{workflow_id}")
async def api_get_workflow(workflow_id: int):
    raise HTTPException(status_code=404, detail="در نسخه local-only جریان‌ها روی سرور ذخیره نمی‌شوند")

@app.post("/api/telegram/test")
async def api_test_telegram(settings: UserSettings):
    token = settings.tg_token or get_setting("tg_token")
    chat_id = settings.tg_chat or get_setting("tg_chat")
    if not token or not chat_id:
        return {"success": False, "message": "توکن یا chat_id تلگرام تنظیم نشده"}
    msg = await send_telegram_message(token, chat_id, "تست اتصال از جریان ایرانی V2")
    return {"success": msg.startswith("✅"), "message": msg}

@app.post("/api/send/test-media")
async def api_test_send_media(payload: Dict[str, Any]):
    settings = UserSettings(**payload.get("settings", {}))
    platform = payload.get("platform")
    media_type = payload.get("media_type", "photo")
    file_path = payload.get("file_path")
    caption = payload.get("caption", "تست")
    chat_id = payload.get("chat_id", "")
    
    if not file_path or not os.path.exists(file_path):
        return {"success": False, "message": "file_path نامعتبر است"}
    
    with open(file_path, "rb") as f:
        file_bytes = f.read()
    
    msg = {"type": media_type, "caption": caption, "file_name": os.path.basename(file_path)}
    
    if platform == "bale":
        result = await send_bale(settings.bale_token, chat_id or settings.bale_chat, msg, file_bytes)
    elif platform == "rubika":
        result = await send_rubika(settings.rubika_token, chat_id or settings.rubika_chat, msg, file_bytes, public_base_url=settings.public_base_url)
    elif platform == "soroush":
        result = await send_soroush(settings.soroush_token, chat_id or settings.soroush_chat, msg, file_bytes)
    elif platform == "gap":
        result = await send_gap(settings.gap_token, chat_id or settings.gap_chat, msg, file_bytes)
    elif platform == "instagram":
        result = await send_instagram(settings.ig_access_token, chat_id or settings.ig_business_id, msg, file_bytes)
    else:
        result = await send_telegram_media(settings.tg_token, chat_id or settings.tg_chat, media_type, file_bytes, caption=caption, file_name=os.path.basename(file_path))
    
    return {"success": result.startswith("✅"), "message": result}

@app.get("/api/email/receive")
async def api_receive_emails(settings: UserSettings = None, limit: int = 5):
    if not settings:
        settings = UserSettings()
        settings.email_imap = get_setting("email_imap")
        settings.email_user = get_setting("email_user")
        settings.email_pass = get_setting("email_pass")
    emails = await receive_emails(settings, limit)
    return {"emails": emails}

@app.get("/api/debug/settings")
async def api_debug_settings():
    """در نسخه حریم‌خصوصی، هیچ تنظیماتی روی سرور ذخیره نمی‌شود."""
    return {"settings_in_db": {"privacy": "local-only؛ هیچ توکن یا اطلاعات کاربر روی سرور ذخیره نشده است"}}

@app.post("/api/stt")
async def api_stt():
    """میکروفون از طریق Web Speech API در مرورگر کار می‌کند - این endpoint فقط برای اطلاع‌رسانی است"""
    return {"success": True, "browser_stt": True, "message": "از Web Speech API در مرورگر استفاده کنید"}

@app.post("/api/send-manual")
async def api_send_manual(body: Dict[str, Any]):
    """ارسال دستی پیام به یک پلتفرم"""
    platform = body.get("platform", "")
    text = body.get("text", "")
    settings_data = body.get("settings", {})

    s = UserSettings(**settings_data) if settings_data else UserSettings()
    for key in ["tg_token", "tg_chat", "bale_token", "bale_chat",
                "rubika_token", "rubika_chat", "soroush_token", "soroush_chat",
                "gap_token", "gap_chat"]:
        if not getattr(s, key, None):
            setattr(s, key, get_setting(key))

    if not text:
        return {"success": False, "message": "متن پیام خالی است"}

    msg = {"type": "text", "text": text}

    if platform == "telegram":
        result = await send_telegram_message(s.tg_token, s.tg_chat, text)
    elif platform == "bale":
        result = await send_bale(s.bale_token, s.bale_chat, msg)
    elif platform == "rubika":
        result = await send_rubika(s.rubika_token, s.rubika_chat, msg)
    elif platform == "soroush":
        result = await send_soroush(s.soroush_token, s.soroush_chat, msg)
    elif platform == "gap":
        result = await send_gap(s.gap_token, s.gap_chat, msg)
    else:
        return {"success": False, "message": f"پلتفرم '{platform}' پشتیبانی نمی‌شود"}

    return {"success": result.startswith("✅"), "message": result}

@app.exception_handler(Exception)
async def all_exception_handler(_, exc: Exception):
    save_history("خطای عمومی", str(exc), "خطا", "error")
    return JSONResponse({"success": False, "detail": str(exc)}, status_code=500)

# ============================================================
# Startup / Shutdown
# ============================================================
@app.on_event("startup")
async def on_startup():
    for old_file in MEDIA_DIR.glob("*"):
        _delete_quietly(old_file)
    add_notification("سیستم", "جریان ایرانی Pro آماده شد")
    save_history("سیستم", "جریان ایرانی Pro راه‌اندازی شد (RAM-only)", "موفق", "system")
    runtime["media_cleanup_task"] = asyncio.create_task(cleanup_temp_media_loop())

@app.on_event("shutdown")
async def on_shutdown():
    runtime["running"] = False
    task = runtime.get("media_cleanup_task")
    if task and not task.done():
        task.cancel()
    for token, info in list(TEMP_MEDIA.items()):
        TEMP_MEDIA.pop(token, None)
        _delete_quietly(Path(info.get("path", "")))
    for platform, task in list(runtime["listener_tasks"].items()):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except:
                pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)