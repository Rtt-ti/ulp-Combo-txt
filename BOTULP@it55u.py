import os
import re
import io
import asyncio
import sqlite3
import zipfile
import tempfile
import shutil
from pathlib import Path
from telethon import TelegramClient, events, Button


API_ID = 12345678               
API_HASH = "API_HASH"      
BOT_TOKEN = "token_bot"    

WORK_DIR = Path(tempfile.gettempdir()) / "ulp_combo_bot"
WORK_DIR.mkdir(exist_ok=True)

MAX_FILE_MB = 2000              
CHUNK_LINES_PER_PART = 500_000  
TG_UPLOAD_LIMIT_MB = 49         


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}[:;|,][^\s:;|,]+")
CARD_RE  = re.compile(r"\b(?:\d[ -]?){13,19}\b\s*[:;|,]\s*\d{1,2}\s*[:;|,/]\s*\d{2,4}\s*[:;|,/]\s*\d{3,4}")
PHONE_RE = re.compile(r"^\+?\d{7,15}:")

def normalize_line(line: str) -> str:
    line = line.strip()
    return re.sub(r"\s*[|;,]\s*", ":", line)

def is_valid_combo(line: str) -> bool:
    parts = line.split(":")
    return len(parts) >= 2 and parts[0].strip() and parts[1].strip()

def classify(line: str) -> str:
    if EMAIL_RE.search(line): return "email"
    if CARD_RE.search(line): return "card"
    if PHONE_RE.match(line): return "phone"
    return "user"


def stream_zip_text(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as z:
        for info in z.infolist():
            if info.is_dir() or info.file_size == 0: continue
            try:
                with z.open(info, "r") as member:
                    text = io.TextIOWrapper(member, encoding="utf-8", errors="ignore")
                    for line in text:
                        line = line.strip()
                        if line and any(c in line for c in ":|,;"): yield line
            except Exception: continue

def stream_plain_text(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line and any(c in line for c in ":|,;"): yield line

def iter_source_lines(path: Path):
    if zipfile.is_zipfile(path): yield from stream_zip_text(path)
    else: yield from stream_plain_text(path)


def convert_engine(files: list, out_dir: Path) -> dict:
    out_dir.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(out_dir / "dedup.db"))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS seen (line TEXT PRIMARY KEY)")

    paths = {
        "all": out_dir / "combo_all.txt", "email": out_dir / "combo_email.txt",
        "user": out_dir / "combo_user.txt", "phone": out_dir / "combo_phone.txt",
        "card": out_dir / "combo_card.txt",
    }
    handles = {k: open(p, "w", encoding="utf-8") for k, p in paths.items()}
    counts = {"all": 0, "email": 0, "user": 0, "phone": 0, "card": 0}
    duplicates = 0

    try:
        for f in files:
            try:
                for raw in iter_source_lines(Path(f)):
                    line = normalize_line(raw)
                    if not is_valid_combo(line): continue
                    try: conn.execute("INSERT INTO seen (line) VALUES (?)", (line,))
                    except sqlite3.IntegrityError:
                        duplicates += 1
                        continue
                    kind = classify(line)
                    handles["all"].write(line + "\n")
                    handles[kind].write(line + "\n")
                    counts["all"] += 1
                    counts[kind] += 1
                    if counts["all"] % 100_000 == 0: conn.commit()
            except Exception: continue
        conn.commit()
    finally:
        for h in handles.values(): h.close()
        conn.close()
    return {"paths": paths, "counts": counts, "duplicates": duplicates}


def search_engine(files: list, keyword: str, format_type: str, out_dir: Path):
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"{keyword}_{format_type}.txt"
    keyword = keyword.lower()
    
    count = 0
    seen = set()
    
    with open(out_file, "w", encoding="utf-8") as f_out:
        for f in files:
            try:
                for raw in iter_source_lines(Path(f)):
                    line = normalize_line(raw)
                    if not is_valid_combo(line): continue
                    if keyword not in line.lower(): continue 
                    
                    # منع التكرار لو اختار المستخدم Dedup
                    if format_type == "dedup":
                        if line in seen: continue
                        seen.add(line)
                    
                    # تجهيز الصيغة المطلوبة
                    parts = line.split(":")
                    if format_type == "combo":
                        out_line = f"{parts[0]}:{parts[1]}"
                    elif format_type == "email":
                        if EMAIL_RE.search(line):
                            out_line = f"{parts[0]}:{parts[1]}"
                        else: continue
                    else: # full
                        out_line = line
                        
                    f_out.write(out_line + "\n")
                    count += 1
            except Exception: continue
            
    return out_file, count

# ==================== تقسيم الملفات ====================
def split_if_needed(path: Path, max_mb: int = TG_UPLOAD_LIMIT_MB) -> list:
    if path.stat().st_size / (1024 * 1024) <= max_mb: return [path]
    parts, part_num = [], 1
    current = path.parent / f"{path.stem}_part{part_num}{path.suffix}"
    fout = open(current, "w", encoding="utf-8")
    line_count = 0
    with path.open("r", encoding="utf-8") as fin:
        for line in fin:
            fout.write(line); line_count += 1
            if line_count >= CHUNK_LINES_PER_PART:
                fout.close(); parts.append(current)
                part_num += 1
                current = path.parent / f"{path.stem}_part{part_num}{path.suffix}"
                fout = open(current, "w", encoding="utf-8"); line_count = 0
    fout.close()
    if current.stat().st_size > 0: parts.append(current)
    return parts

# ==================== إدارة الحالات ====================
user_files = {}
user_states = {} # لتتبع هل المستخدم في وضع "البحث" أو لا
user_keywords = {} # لتخزين الكلمة المفتاحية مؤقتاً

# ==================== لوحات التحكم ====================
def get_main_keyboard():
    return [
        [Button.inline("📤 Upload ULP", data=b"upload_help")],
        [Button.inline("⚙️ بدء التحويل الشامل", data=b"convert")],
        [Button.inline("🔍 استخراج حسب الموقع", data=b"search_site")],
        [Button.inline("📊 حالة القائمة", data=b"status"), Button.inline("🗑 مسح القائمة", data=b"reset")]
    ]

def get_format_keyboard(keyword):
    return [
        [Button.inline("📥 Combo (user:pass)", data=f"fmt_combo_{keyword}")],
        [Button.inline("📧 Email Only (email:pass)", data=f"fmt_email_{keyword}")],
        [Button.inline("📄 Full Lines (with URL)", data=f"fmt_full_{keyword}")],
        [Button.inline("🚀 Combo + Dedup", data=f"fmt_dedup_{keyword}")],
        [Button.inline("❌ إلغاء", data=b"cancel")]
    ]

# ==================== الأحداث ====================
client = TelegramClient("ulp_combo_session", API_ID, API_HASH, connection_retries=5, timeout=300)

@client.on(events.NewMessage(pattern="/start"))
async def start(event):
    text = (
        "👋 **مرحباً بك في بوت تحويل واستخراج الكومبو!**\n\n"
        "يمكنك رفع ملفات ULP وتحويلها، أو استخراج بيانات موقع معين.\n\n"
        "👇 **استخدم لوحة التحكم:**"
    )
    await event.reply(text, buttons=get_main_keyboard(), parse_mode='md')

@client.on(events.CallbackQuery)
async def callback_handler(event):
    data = event.data.decode('utf-8')
    uid = event.sender_id

    if data == "upload_help":
        help_text = (
            "📤 **طريقة الاستعمال:**\n\n"
            "1️⃣ اضغط على زر الإرفاق (📎) في الشات.\n"
            "2️⃣ اختر ملفك بصيغة `.txt` أو `.zip` أو `.ulp`.\n"
            "3️⃣ أرسله للبوت.\n"
            "4️⃣ بعد التحميل، اضغط على (⚙️ بدء التحويل الشامل).\n\n"
            "⚠️ **الحد الأقصى:** 2 جيجابايت."
        )
        await event.answer("ارفع الملف من الشات الآن!", alert=True)
        await event.edit(help_text, buttons=[[Button.inline("🔙 رجوع", data=b"back")]], parse_mode='md')
    
    elif data == "back":
        await event.edit("👇 **لوحة التحكم:**", buttons=get_main_keyboard(), parse_mode='md')
    
    elif data == "convert":
        await event.answer("جاري التحويل...")
        await event.delete()
        await run_convert(uid, event.chat_id)
    
    elif data == "search_site":
        if not user_files.get(uid):
            await event.answer("❌ أرسل ملفات أولاً!", alert=True)
            return
        user_states[uid] = "awaiting_keyword"
        await event.edit(
            "🔍 **البحث والاستخراج:**\n\n"
            "أرسل اسم الموقع أو التطبيق الذي تريد استخراج بياناته.\n"
            "مثال: `netflix` أو `spotify` أو `google.com`",
            buttons=[[Button.inline("❌ إلغاء", data=b"cancel")]],
            parse_mode='md'
        )
    
    elif data == "status":
        files = user_files.get(uid, [])
        if not files:
            await event.answer("القائمة فاضية.", alert=True)
        else:
            total_mb = sum(Path(f).stat().st_size for f in files) / (1024 * 1024)
            await event.answer(f"الملفات: {len(files)}\nالحجم: {total_mb:.1f}MB", alert=True)
    
    elif data == "reset":
        files = user_files.get(uid, [])
        shutil.rmtree(WORK_DIR / str(uid), ignore_errors=True)
        user_files[uid] = []
        user_states[uid] = None
        await event.answer(f"تم مسح {len(files)} ملف.", alert=True)
        await event.edit("👇 **لوحة التحكم:**", buttons=get_main_keyboard())
        
    elif data == "cancel":
        user_states[uid] = None
        await event.edit("تم الإلغاء.", buttons=[[Button.inline("🔙 رجوع", data=b"back")]])
        
    # استقبال اختيار الصيغة
    elif data.startswith("fmt_"):
        parts = data.split("_")
        fmt_type = parts[1]
        keyword = parts[2]
        
        await event.answer("جاري الاستخراج...")
        await event.delete()
        await run_search(uid, event.chat_id, keyword, fmt_type)

@client.on(events.NewMessage(func=lambda e: e.document is not None))
async def handle_doc(event):
    doc = event.document
    size_mb = (doc.size or 0) / (1024 * 1024)
    
    if size_mb > MAX_FILE_MB:
        await event.reply(f"❌ الملف أكبر من الحد ({MAX_FILE_MB}MB).")
        return

    user_dir = WORK_DIR / str(event.sender_id)
    user_dir.mkdir(exist_ok=True)
    fname = next((a.file_name for a in doc.attributes if hasattr(a, 'file_name')), f"file_{doc.id}")
    local_path = user_dir / fname

    msg = await event.reply(f"⏳ جاري تحميل: `{fname}`\n(0%)", parse_mode='md')
    
    try:
        downloaded = 0
        total_size = doc.size
        
        with open(local_path, "wb") as f:
            async for chunk in client.iter_download(doc, request_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = int((downloaded / total_size) * 100)
                    if percent % 20 == 0:
                        try: await msg.edit(f"⏳ جاري تحميل: `{fname}`\n({percent}%)", parse_mode='md')
                        except: pass

        user_files.setdefault(event.sender_id, []).append(str(local_path))
        await msg.edit(f"✅ **تم التحميل بنجاح!**\n📁 `{fname}`\n📦 في القائمة: {len(user_files[event.sender_id])}", parse_mode='md')
        
    except Exception as e:
        await msg.edit(f"❌ فشل التحميل:\n`{e}`", parse_mode='md')

# استقبال الكلمة المفتاحية
@client.on(events.NewMessage(func=lambda e: e.is_private and e.text and not e.text.startswith('/')))
async def handle_keyword(event):
    uid = event.sender_id
    if user_states.get(uid) == "awaiting_keyword":
        keyword = event.text.strip()
        user_states[uid] = None # إلغاء الحالة
        
        await event.reply(
            f"🔍 **البحث عن:** `{keyword}`\n\n"
            "اختر صيغة الإخراج المطلوبة:",
            buttons=get_format_keyboard(keyword),
            parse_mode='md'
        )

# ==================== دوال التنفيذ ====================
async def run_convert(user_id, chat_id):
    files = user_files.get(user_id, [])
    if not files:
        await client.send_message(chat_id, "❌ لا توجد ملفات في القائمة.")
        return

    out_dir = WORK_DIR / f"{user_id}_out"
    shutil.rmtree(out_dir, ignore_errors=True)
    msg = await client.send_message(chat_id, "⚙️ جاري التحويل الشامل...")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, convert_engine, files, out_dir)
    counts, dup = result["counts"], result["duplicates"]

    if counts["all"] == 0:
        await msg.edit("❌ لم يتم استخراج أي كومبو."); return

    summary = (
        f"✅ **انتهى التحويل!**\n\n"
        f"📊 **الإحصائيات:**\n"
        f"• الإجمالي: `{counts['all']:,}`\n"
        f"• مكرر: `{dup:,}`\n"
        f"• إيميل: `{counts['email']:,}`\n"
        f"• يوزر: `{counts['user']:,}`\n"
        f"• هاتف: `{counts['phone']:,}`\n"
        f"• كارد: `{counts['card']:,}`"
    )
    await msg.edit(summary, parse_mode='md')

    labels = {"all": "الشامل", "email": "إيميل", "user": "يوزر", "phone": "هاتف", "card": "كارد"}
    for key, path in result["paths"].items():
        if path.stat().st_size == 0: continue
        parts = split_if_needed(path)
        for i, part in enumerate(parts, 1):
            cap = labels[key] + (f" جزء {i}/{len(parts)}" if len(parts) > 1 else "") + f" — {counts[key]:,} سطر"
            await client.send_file(chat_id, part, caption=cap)

    user_files[user_id] = []
    shutil.rmtree(out_dir, ignore_errors=True)
    shutil.rmtree(WORK_DIR / str(user_id), ignore_errors=True)

async def run_search(user_id, chat_id, keyword, fmt_type):
    files = user_files.get(user_id, [])
    out_dir = WORK_DIR / f"{user_id}_search"
    shutil.rmtree(out_dir, ignore_errors=True)
    
    msg = await client.send_message(chat_id, f"🔍 جاري استخراج بيانات `{keyword}`...", parse_mode='md')

    loop = asyncio.get_event_loop()
    out_file, count = await loop.run_in_executor(None, search_engine, files, keyword, fmt_type, out_dir)

    if count == 0:
        await msg.edit(f"❌ لم يتم العثور على أي نتائج لـ `{keyword}`.", parse_mode='md')
        return

    await msg.edit(f"✅ تم استخراج **{count:,}** سطر لـ `{keyword}`.\nجاري إرسال الملف...", parse_mode='md')

    parts = split_if_needed(out_file)
    for i, part in enumerate(parts, 1):
        cap = f"🔍 نتائج: {keyword} — {count:,} سطر"
        if len(parts) > 1: cap += f" (جزء {i}/{len(parts)})"
        await client.send_file(chat_id, part, caption=cap)

    shutil.rmtree(out_dir, ignore_errors=True)

async def main():
    await client.start(bot_token=BOT_TOKEN)
    print("Bot is running with Search Feature...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())