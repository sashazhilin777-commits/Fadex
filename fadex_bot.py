import sys
import asyncio
import sqlite3
import uuid
import re
import csv
import io
import logging
import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from contextlib import contextmanager
from collections import defaultdict

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.exceptions import TelegramBadRequest
import aiohttp
from dotenv import load_dotenv
import os
from aiohttp import web

load_dotenv()

# ========== ВЕБ-СЕРВЕР ДЛЯ RENDER ==========
async def health_check(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 10000)
    await site.start()
    print("✅ Веб-сервер запущен на порту 10000")

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
USDT_WALLET = os.getenv("USDT_WALLET")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
TON_API_KEY = os.getenv("TON_API_KEY", "")

MANUAL_URL = "https://telegra.ph/manual-po-aktivacii-04-28"
CLEANUP_DAYS = 7
RETRY_COUNT = 3
RETRY_DELAY = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRODUCTS = {
    "ton300":  {"name": "🔥 Промокод на 300 TON", "price_usd": 150, "desc": "300 TON", "old_price": None, "discount": None},
    "ton400":  {"name": "🔥 Промокод на 400 TON", "price_usd": 200, "desc": "400 TON", "old_price": None, "discount": None},
    "sol8":    {"name": "🚀 Промокод на 8 SOLANA", "price_usd": 280, "desc": "8 SOL", "old_price": None, "discount": None},
    "xrp900":  {"name": "💎 Промокод на 900 XRP",  "price_usd": 750, "desc": "900 XRP", "old_price": 825, "discount": 9},
    "xrp500":  {"name": "✨ Промокод на 500 XRP",  "price_usd": 425, "desc": "500 XRP", "old_price": 450, "discount": 6}
}

# ========== ШИФРОВАНИЕ ==========
ENCRYPTION_KEY = hashlib.sha256("fadex_secret_key_2024".encode()).digest()

def encrypt_data(data: str) -> str:
    """Простое шифрование данных"""
    encrypted = hashlib.sha256(f"{ENCRYPTION_KEY}{data}".encode()).hexdigest()[:16]
    return encrypted

def hash_password(password: str) -> str:
    """Хеширование пароля"""
    return hashlib.sha256(f"{ENCRYPTION_KEY}{password}".encode()).hexdigest()

# ========== RATE LIMITING ==========
user_commands = defaultdict(list)

def check_rate_limit(user_id: int, limit=30, window=60) -> bool:
    now = datetime.now()
    user_commands[user_id] = [t for t in user_commands[user_id] if now - t < timedelta(seconds=window)]
    if len(user_commands[user_id]) >= limit:
        return False
    user_commands[user_id].append(now)
    return True

# ========== БАЗА ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        joined_at TEXT,
        last_login TEXT,
        balance_usd REAL DEFAULT 0,
        encrypted_data TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        code TEXT UNIQUE,
        used BOOLEAN DEFAULT 0
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        type TEXT,
        amount_usd REAL,
        status TEXT,
        product_id TEXT,
        promo_code TEXT,
        created_at TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS manual_payments (
        payment_id TEXT PRIMARY KEY,
        user_id INTEGER,
        amount_usd REAL,
        purpose TEXT,
        product_id TEXT,
        quantity INTEGER,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        confirmed_at TEXT,
        locked BOOLEAN DEFAULT 0
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS withdrawal_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount_usd REAL,
        wallet_address TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        processed_at TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS admin_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER,
        action TEXT,
        target_user INTEGER,
        details TEXT,
        created_at TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS admin_sessions (
        admin_id INTEGER PRIMARY KEY,
        logged_until TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS products_config (
        product_id TEXT PRIMARY KEY,
        price_usd REAL,
        old_price REAL,
        description TEXT
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS coupons (
        id TEXT PRIMARY KEY,
        discount_percent INTEGER,
        valid_until TEXT,
        user_id INTEGER,
        used BOOLEAN DEFAULT 0,
        created_at TEXT
    )""")
    
    conn.commit()
    conn.close()

def save_products_to_db():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    for pid, prod in PRODUCTS.items():
        cur.execute("REPLACE INTO products_config (product_id, price_usd, old_price, description) VALUES (?, ?, ?, ?)", 
                    (pid, prod['price_usd'], prod.get('old_price'), prod['desc']))
    conn.commit()
    conn.close()

def load_products_from_db():
    global PRODUCTS
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT product_id, price_usd, old_price, description FROM products_config")
    rows = cur.fetchall()
    conn.close()
    if rows:
        for pid, price, old_price, desc in rows:
            if pid in PRODUCTS:
                PRODUCTS[pid]['price_usd'] = price
                PRODUCTS[pid]['old_price'] = old_price
                PRODUCTS[pid]['desc'] = desc
                if old_price:
                    discount = round((old_price - price) / old_price * 100)
                    PRODUCTS[pid]['discount'] = discount

def seed_promo_codes():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    codes = {
        "ton300": ["TONGFT400"],
        "ton400": ["TONGFT300"],
        "sol8":   ["SOLGFT8"],
        "xrp900": ["XRPGFT900"],
        "xrp500": ["XRPGFT500"]
    }
    for prod, c_list in codes.items():
        for c in c_list:
            cur.execute("INSERT OR IGNORE INTO promo_codes (product_id, code) VALUES (?, ?)", (prod, c))
    conn.commit()
    conn.close()

def register_or_update_user(uid, username, first_name):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    now = datetime.now().isoformat()
    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (uid,))
    exists = cur.fetchone()
    if not exists:
        cur.execute("INSERT INTO users (user_id, username, first_name, joined_at, last_login, balance_usd) VALUES (?, ?, ?, ?, ?, 0)",
                    (uid, username, first_name, now, now))
    else:
        cur.execute("UPDATE users SET username = ?, first_name = ?, last_login = ? WHERE user_id = ?",
                    (username, first_name, now, uid))
    conn.commit()
    conn.close()

def get_balance(user_id: int) -> float:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT balance_usd FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0.0

def update_balance(user_id: int, delta: float):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("UPDATE users SET balance_usd = balance_usd + ? WHERE user_id = ?", (delta, user_id))
    conn.commit()
    conn.close()

def add_transaction(user_id: int, typ: str, amount: float, status: str, product_id: str = None, promo_code: str = None):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("""INSERT INTO transactions (user_id, type, amount_usd, status, product_id, promo_code, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, typ, amount, status, product_id, promo_code, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_user_purchases(user_id: int, limit=20):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("""SELECT product_id, promo_code, amount_usd, created_at
                   FROM transactions WHERE user_id = ? AND type = 'purchase'
                   AND status = 'completed' AND promo_code IS NOT NULL
                   ORDER BY created_at DESC LIMIT ?""", (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_available_codes_count(product_id: str) -> int:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM promo_codes WHERE product_id = ? AND used = 0", (product_id,))
    cnt = cur.fetchone()[0]
    conn.close()
    return cnt

def get_unused_code(product_id: str) -> Optional[str]:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT code FROM promo_codes WHERE product_id = ? AND used = 0 LIMIT 1", (product_id,))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE promo_codes SET used = 1 WHERE code = ?", (row[0],))
        conn.commit()
        conn.close()
        return row[0]
    conn.close()
    return None

def add_promo_codes(product_id: str, codes: list):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    added = 0
    for code in codes:
        cur.execute("SELECT code FROM promo_codes WHERE code = ?", (code,))
        if cur.fetchone():
            continue
        cur.execute("INSERT INTO promo_codes (product_id, code, used) VALUES (?, ?, 0)", (product_id, code))
        added += 1
    conn.commit()
    conn.close()
    return added

def create_manual_payment(user_id: int, amount: float, purpose: str, product_id: str = None, quantity: int = 1) -> str:
    payment_id = str(uuid.uuid4())[:8]
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("""INSERT INTO manual_payments
                   (payment_id, user_id, amount_usd, purpose, product_id, quantity, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (payment_id, user_id, amount, purpose, product_id, quantity, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    return payment_id

def get_payment(payment_id: str):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount_usd, purpose, product_id, quantity, status FROM manual_payments WHERE payment_id = ?", (payment_id,))
    row = cur.fetchone()
    conn.close()
    return row

def update_payment_status(payment_id: str, status: str, confirmed_at: datetime = None):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    if confirmed_at:
        cur.execute("UPDATE manual_payments SET status = ?, confirmed_at = ? WHERE payment_id = ?",
                    (status, confirmed_at.isoformat(), payment_id))
    else:
        cur.execute("UPDATE manual_payments SET status = ? WHERE payment_id = ?", (status, payment_id))
    conn.commit()
    conn.close()

def create_withdrawal_request(user_id: int, amount: float, wallet: str) -> int:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, wallet_address, created_at) VALUES (?, ?, ?, ?)",
                (user_id, amount, wallet, datetime.now().isoformat()))
    req_id = cur.lastrowid
    conn.commit()
    conn.close()
    return req_id

def is_admin_logged_in(admin_id: int) -> bool:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT logged_until FROM admin_sessions WHERE admin_id = ?", (admin_id,))
    row = cur.fetchone()
    conn.close()
    if row and datetime.fromisoformat(row[0]) > datetime.now():
        return True
    return False

def admin_login(admin_id: int, password: str) -> bool:
    if password != ADMIN_PASSWORD:
        return False
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    until = (datetime.now() + timedelta(hours=1)).isoformat()
    cur.execute("REPLACE INTO admin_sessions (admin_id, logged_until) VALUES (?, ?)", (admin_id, until))
    conn.commit()
    conn.close()
    return True

def log_admin_action(admin_id, action, target_user, details):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO admin_logs (admin_id, action, target_user, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (admin_id, action, target_user, details, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, joined_at, last_login, balance_usd FROM users ORDER BY joined_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows

def get_stats():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users_cnt = cur.fetchone()[0]
    cur.execute("SELECT SUM(amount_usd) FROM transactions WHERE type='purchase' AND status='completed'")
    total_sales = cur.fetchone()[0] or 0
    cur.execute("SELECT SUM(amount_usd) FROM withdrawal_requests WHERE status='completed'")
    total_withdrawn = cur.fetchone()[0] or 0
    conn.close()
    return users_cnt, total_sales, total_withdrawn

def validate_trc20_address(address: str) -> bool:
    """Валидация TRC20 адреса (Tron)"""
    if not address.startswith('T'):
        return False
    if len(address) != 34:
        return False
    if not re.match(r'^[A-Za-z0-9]+$', address):
        return False
    return True

# ========== КЛАВИАТУРЫ ==========
def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍️ Товары"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="💰 Пополнить"), KeyboardButton(text="📦 Мои покупки")],
            [KeyboardButton(text="📜 Правила"), KeyboardButton(text="🆘 Поддержка")],
            [KeyboardButton(text="💸 Вывод средств"), KeyboardButton(text="🎫 Купон")],
            [KeyboardButton(text="📊 Наличие")],
        ],
        resize_keyboard=True
    )

def products_kb():
    btns = []
    for pid, prod in PRODUCTS.items():
        price_text = f"{prod['price_usd']}$"
        if prod.get("old_price"):
            price_text = f"~~{prod['old_price']}$~~ → {prod['price_usd']}$ (-{prod['discount']}%)"
        btns.append([InlineKeyboardButton(text=f"{prod['name']} — {price_text}", callback_data=f"prod_{pid}")])
    btns.append([InlineKeyboardButton(text="◀️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def purchase_kb(pid: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 С баланса", callback_data=f"balance_buy_{pid}"),
         InlineKeyboardButton(text="💸 Напрямую", callback_data=f"direct_pay_{pid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_products")]
    ])

def quantity_kb(pid: str, price: float):
    btns = []
    for q in [1, 2, 3, 4, 5]:
        btns.append([InlineKeyboardButton(text=f"{q} шт — {price * q}$", callback_data=f"qty_{pid}_{q}")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_purchase_{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def deposit_kb():
    amounts = [50, 100, 200, 500, 1000]
    btns = []
    for amt in amounts:
        btns.append([InlineKeyboardButton(text=f"{amt}$", callback_data=f"dep_{amt}")])
    btns.append([InlineKeyboardButton(text="✏️ Своя сумма", callback_data="dep_custom")])
    btns.append([InlineKeyboardButton(text="◀️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def back_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 В меню", callback_data="menu")]])

def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
         InlineKeyboardButton(text="➕ Добавить коды", callback_data="admin_add_codes")],
        [InlineKeyboardButton(text="📋 Все пользователи", callback_data="admin_users"),
         InlineKeyboardButton(text="💳 Платежи", callback_data="admin_payments")],
        [InlineKeyboardButton(text="💸 Выводы", callback_data="admin_withdrawals"),
         InlineKeyboardButton(text="📤 Экспорт", callback_data="admin_export")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
         InlineKeyboardButton(text="🎫 Купоны", callback_data="admin_coupons")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_logout"),
         InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
    ])

def admin_coupons_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать купон", callback_data="coupon_create")],
        [InlineKeyboardButton(text="📋 Список купонов", callback_data="coupon_list")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_stats")]
    ])

# ========== FSM СОСТОЯНИЯ ==========
class DepositStates(StatesGroup):
    waiting_custom = State()

class BalanceBuy(StatesGroup):
    quantity = State()
    confirm = State()

class DirectPay(StatesGroup):
    quantity = State()

class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_wallet = State()

class AdminAddCodes(StatesGroup):
    waiting_product = State()
    waiting_codes = State()

class AdminBroadcast(StatesGroup):
    waiting_message = State()

class AdminCoupon(StatesGroup):
    waiting_data = State()

# ========== СОЗДАНИЕ БОТА ==========
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@dp.message(Command("start"))
async def start_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Подождите немного...")
        return
    
    uid = msg.from_user.id
    uname = msg.from_user.username or ""
    fname = msg.from_user.first_name or "Пользователь"
    register_or_update_user(uid, uname, fname)
    
    welcome_text = f"""✨ Добро пожаловать в FadeX, {fname}! ✨

🎁 Промокоды на TON, SOL, XRP
💰 Пополнение USDT (TRC20)
⚡ Мгновенная выдача после оплаты

📌 Кнопка «🛍️ Товары» - выбрать товар
👤 Профиль - баланс и ID
💸 Вывод средств - до 24 часов

📘 Мануал по активации: {MANUAL_URL}

По всем вопросам: {ADMIN_USERNAME}"""
    
    await msg.answer(welcome_text, reply_markup=main_menu())

@dp.message(Command("admin"))
async def admin_cmd(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    
    args = msg.text.split()
    if len(args) >= 2 and args[1] == "login":
        pwd = args[2] if len(args) > 2 else ""
        if admin_login(ADMIN_ID, pwd):
            await msg.answer("✅ Вы вошли в админ-панель на 1 час!", reply_markup=admin_panel_kb())
            log_admin_action(ADMIN_ID, "login", 0, "Успешный вход")
        else:
            await msg.answer("❌ Неверный пароль!")
    else:
        if is_admin_logged_in(ADMIN_ID):
            await msg.answer("🔐 Админ-панель", reply_markup=admin_panel_kb())
        else:
            await msg.answer("⛔ Доступ запрещён! Используйте: /admin login <пароль>")

@dp.message(F.text == "🔐 Админ панель")
async def admin_button(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        await msg.answer("⛔ У вас нет доступа к админ-панели!")
        return
    
    if is_admin_logged_in(ADMIN_ID):
        await msg.answer("🔐 Админ-панель", reply_markup=admin_panel_kb())
    else:
        await msg.answer("⛔ Сначала войдите: /admin login <пароль>")

@dp.message(F.text == "🛍️ Товары")
async def show_products(msg: Message):
    await msg.answer("📦 Наши товары:", reply_markup=products_kb())

@dp.message(F.text == "👤 Профиль")
async def profile(msg: Message):
    bal = get_balance(msg.from_user.id)
    await msg.answer(f"👤 Ваш профиль\n\n🆔 ID: <code>{msg.from_user.id}</code>\n💰 Баланс: ${bal:.2f}\n\n📅 Дата регистрации: {datetime.now().strftime('%d.%m.%Y')}", parse_mode="HTML", reply_markup=main_menu())

@dp.message(F.text == "💰 Пополнить")
async def deposit_start(msg: Message):
    if not USDT_WALLET:
        await msg.answer("❌ Пополнение временно недоступно. Обратитесь к администратору.")
        return
    await msg.answer("💸 Выберите сумму пополнения USDT (TRC20):", reply_markup=deposit_kb())

@dp.message(F.text == "📦 Мои покупки")
async def my_purchases(msg: Message):
    purchases = get_user_purchases(msg.from_user.id)
    if not purchases:
        await msg.answer("📭 У вас пока нет покупок.", reply_markup=main_menu())
        return
    
    text = "📦 Ваши покупки:\n\n"
    for pid, codes, amt, dt in purchases:
        prod = PRODUCTS.get(pid, {"name": pid})
        text += f"🔹 {prod['name']}\n💰 ${amt:.2f}\n🎫 <code>{codes}</code>\n🕒 {dt[:16]}\n\n"
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())

@dp.message(F.text == "📜 Правила")
async def rules(msg: Message):
    text = """📜 ПРАВИЛА МАГАЗИНА FadeX

1️⃣ Возврат/замена промокода — 12 часов с момента покупки
2️⃣ После активации промокод возврату не подлежит
3️⃣ Вывод средств обрабатывается до 24 часов
4️⃣ Минимальная сумма вывода — 10 USDT
5️⃣ По всем вопросам — в поддержку

⚠️ Любые попытки мошенничества ведут к блокировке аккаунта!

Спасибо за доверие! 🙏"""
    await msg.answer(text, reply_markup=main_menu())

@dp.message(F.text == "🆘 Поддержка")
async def support(msg: Message):
    await msg.answer(f"📩 Связь с поддержкой:\n{ADMIN_USERNAME}\n\nОтветим в течение 12 часов.", reply_markup=main_menu())

@dp.message(F.text == "💸 Вывод средств")
async def withdraw_start(msg: Message, state: FSMContext):
    bal = get_balance(msg.from_user.id)
    if bal < 10:
        await msg.answer(f"❌ Минимальная сумма вывода — 10 USDT\n💰 Ваш баланс: ${bal:.2f}")
        return
    
    await state.set_state(WithdrawStates.waiting_amount)
    await msg.answer(f"💰 Ваш баланс: ${bal:.2f}\n\nВведите сумму вывода (мин 10, макс {bal:.2f}):")

@dp.message(DepositStates.waiting_custom, F.text.regex(r"^\d+(\.\d+)?$"))
async def deposit_custom_amount(msg: Message, state: FSMContext):
    amount = float(msg.text.strip())
    
    if amount < 10:
        await msg.answer("❌ Минимальная сумма пополнения — 10 USDT. Попробуйте ещё раз:", reply_markup=back_menu_kb())
        return
    
    if amount > 5000:
        await msg.answer("❌ Максимальная сумма пополнения — 5000 USDT. Попробуйте ещё раз:", reply_markup=back_menu_kb())
        return
    
    payment_id = create_manual_payment(msg.from_user.id, amount, "deposit")
    
    await msg.answer(
        f"🧾 **СЧЁТ НА ПОПОЛНЕНИЕ**\n\n"
        f"💰 Сумма: **{amount} USDT**\n\n"
        f"📤 **Кошелёк для перевода (TRC20):**\n"
        f"<code>{USDT_WALLET}</code>\n\n"
        f"📝 **ОБЯЗАТЕЛЬНО укажите ID в комментарии:**\n"
        f"<code>pay_{payment_id}</code>\n\n"
        f"✅ После перевода нажмите «Я ПЕРЕВЁЛ»\n\n"
        f"⚠️ Важно: переводы без указания ID не обрабатываются!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я ПЕРЕВЁЛ", callback_data=f"payment_done_{payment_id}")],
            [InlineKeyboardButton(text="❌ ОТМЕНИТЬ", callback_data=f"payment_cancel_{payment_id}")]
        ])
    )
    await state.clear()

@dp.message(WithdrawStates.waiting_wallet)
async def withdraw_wallet(msg: Message, state: FSMContext):
    wallet = msg.text.strip()
    
    if not validate_trc20_address(wallet):
        await msg.answer("❌ Неверный TRC20 адрес! Адрес должен:\n- Начинаться с буквы T\n- Содержать 34 символа\n- Состоять из латинских букв и цифр\n\nПопробуйте ещё раз:")
        return
    
    data = await state.get_data()
    amount = data['amount']
    
    # Создаём заявку
    rid = create_withdrawal_request(msg.from_user.id, amount, wallet)
    update_balance(msg.from_user.id, -amount)
    add_transaction(msg.from_user.id, "withdraw", amount, "pending")
    
    await msg.answer(f"✅ Заявка на вывод #{rid} создана!\n\n💰 Сумма: ${amount}\n📤 Адрес: `{wallet}`\n\n⏳ Обработка до 24 часов.", parse_mode="Markdown", reply_markup=main_menu())
    
    await bot.send_message(ADMIN_ID, f"💸 НОВАЯ ЗАЯВКА НА ВЫВОД #{rid}\n👤 Пользователь: {msg.from_user.id}\n💰 Сумма: ${amount}\n📤 Адрес: {wallet}")
    
    await state.clear()

@dp.message(F.text == "🎫 Купон")
async def coupon_info(msg: Message):
    await msg.answer("🎫 У вас нет активных купонов.\n\nКупоны можно получить:\n• За рефералов\n• По акциям\n• От администратора\n\nСледите за новостями!", reply_markup=main_menu())

@dp.message(F.text == "📊 Наличие")
async def availability(msg: Message):
    text = "📊 Наличие промокодов:\n\n"
    for pid, prod in PRODUCTS.items():
        cnt = get_available_codes_count(pid)
        text += f"🔹 {prod['name']}: {cnt} шт\n"
    await msg.answer(text, reply_markup=main_menu())

# ---------- ОБРАБОТЧИКИ ТОВАРОВ ----------
@dp.callback_query(F.data.startswith("prod_"))
async def product_chosen(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[1]
    prod = PRODUCTS[pid]
    
    price_text = f"${prod['price_usd']}"
    if prod.get("old_price"):
        price_text = f"~~${prod['old_price']}~~ → ${prod['price_usd']} (скидка -{prod['discount']}%)"
    
    text = f"🔹 {prod['name']}\n💰 Цена: {price_text}\n📦 {prod['desc']}\n\n✅ Выберите способ оплаты:"
    
    await state.update_data(pid=pid, price=prod["price_usd"])
    await cb.message.edit_text(text, reply_markup=purchase_kb(pid))
    await cb.answer()

@dp.callback_query(F.data.startswith("balance_buy_"))
async def balance_buy_start(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    prod = PRODUCTS[pid]
    price = prod["price_usd"]
    
    await state.update_data(pid=pid, price=price)
    await state.set_state(BalanceBuy.quantity)
    
    await cb.message.edit_text(f"🛒 {prod['name']}\n💰 ${price} за шт\n\nВыберите количество:", reply_markup=quantity_kb(pid, price))
    await cb.answer()

@dp.callback_query(F.data.startswith("direct_pay_"))
async def direct_pay_start(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    prod = PRODUCTS[pid]
    price = prod["price_usd"]
    
    await state.update_data(pid=pid, price=price)
    await state.set_state(DirectPay.quantity)
    
    await cb.message.edit_text(f"🛒 {prod['name']}\n💰 ${price} за шт\n\nВыберите количество:", reply_markup=quantity_kb(pid, price))
    await cb.answer()

@dp.callback_query(BalanceBuy.quantity, F.data.startswith("qty_"))
async def balance_select_qty(cb: CallbackQuery, state: FSMContext):
    _, pid, qty = cb.data.split("_")
    qty = int(qty)
    data = await state.get_data()
    price = data.get('price', PRODUCTS[pid]['price_usd'])
    total = price * qty
    bal = get_balance(cb.from_user.id)
    
    if bal < total:
        await cb.answer(f"❌ Недостаточно средств! Нужно ${total:.2f}, у вас ${bal:.2f}")
        return
    
    if get_available_codes_count(pid) < qty:
        await cb.answer(f"❌ Извините, товар временно отсутствует. Осталось {get_available_codes_count(pid)} шт")
        return
    
    await state.update_data(qty=qty, total=total)
    await state.set_state(BalanceBuy.confirm)
    
    await cb.message.edit_text(f"✅ ПОДТВЕРЖДЕНИЕ ПОКУПКИ\n\n📦 {PRODUCTS[pid]['name']}\n🔢 Количество: {qty} шт\n💰 Сумма: ${total:.2f}\n💰 Ваш баланс: ${bal:.2f}\n\nПодтверждаете покупку?", 
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton(text="✅ ДА, КУПИТЬ", callback_data="balance_confirm_go")],
                                   [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="back_products")]
                               ]))
    await cb.answer()

@dp.callback_query(F.data == "balance_confirm_go")
async def balance_do_purchase(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pid = data.get("pid")
    qty = data.get("qty")
    total = data.get("total")
    
    if not pid or not qty:
        await cb.answer("Ошибка, попробуйте снова")
        return
    
    # Проверяем наличие кодов
    available = get_available_codes_count(pid)
    if available < qty:
        await cb.message.edit_text(f"❌ Извините, товар закончился. Осталось {available} шт.\nПопробуйте позже!", reply_markup=back_menu_kb())
        await cb.answer()
        return
    
    # Проверяем баланс
    bal = get_balance(cb.from_user.id)
    if bal < total:
        await cb.message.edit_text(f"❌ Недостаточно средств! Нужно ${total:.2f}, у вас ${bal:.2f}", reply_markup=back_menu_kb())
        await cb.answer()
        return
    
    # Получаем промокоды
    codes = []
    for _ in range(qty):
        code = get_unused_code(pid)
        if code:
            codes.append(code)
    
    if len(codes) < qty:
        await cb.message.edit_text("❌ Ошибка: промокоды закончились. Обратитесь к администратору.", reply_markup=back_menu_kb())
        await cb.answer()
        return
    
    # Списываем средства
    update_balance(cb.from_user.id, -total)
    add_transaction(cb.from_user.id, "purchase", total, "completed", pid, ",".join(codes))
    
    prod = PRODUCTS[pid]
    codes_text = "\n".join([f"{i+1}. <code>{code}</code>" for i, code in enumerate(codes)])
    
    result_text = f"""✅ ПОКУПКА УСПЕШНА!

📦 {prod['name']}
🔢 Количество: {qty} шт
💰 Списано: ${total:.2f}
💰 Остаток: ${bal - total:.2f}

🎫 ВАШИ ПРОМОКОДЫ:
{codes_text}

📘 Инструкция по активации: {MANUAL_URL}

💾 Промокоды сохранены в «Мои покупки»"""

    await cb.message.edit_text(result_text, parse_mode="HTML", reply_markup=back_menu_kb())
    
    await bot.send_message(ADMIN_ID, f"🛒 ПРОДАЖА!\n👤 {cb.from_user.id}\n📦 {prod['name']} x{qty}\n💰 ${total:.2f}")
    
    await state.clear()
    await cb.answer()

@dp.callback_query(DirectPay.quantity, F.data.startswith("qty_"))
async def direct_select_qty(cb: CallbackQuery, state: FSMContext):
    _, pid, qty = cb.data.split("_")
    qty = int(qty)
    data = await state.get_data()
    price = data.get('price', PRODUCTS[pid]['price_usd'])
    total = price * qty
    
    prod = PRODUCTS[pid]
    payment_id = create_manual_payment(cb.from_user.id, total, "direct_purchase", pid, qty)
    
    await cb.message.delete()
    await cb.message.answer(
        f"🧾 ЗАКАЗ НА ПРЯМУЮ ОПЛАТУ\n\n"
        f"📦 {prod['name']}\n"
        f"🔢 Количество: {qty} шт\n"
        f"💰 Сумма: ${total:.2f} USDT\n\n"
        f"📤 КОШЕЛЁК ДЛЯ ОПЛАТЫ (TRC20):\n"
        f"<code>{USDT_WALLET}</code>\n\n"
        f"📝 ОБЯЗАТЕЛЬНО укажите ID в комментарии:\n"
        f"<code>pay_{payment_id}</code>\n\n"
        f"✅ После перевода нажмите кнопку «Я ПЕРЕВЁЛ»",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я ПЕРЕВЁЛ", callback_data=f"payment_done_{payment_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"payment_cancel_{payment_id}")]
        ])
    )
    await state.clear()
    await cb.answer()

# ---------- ОБРАБОТЧИКИ ПОПОЛНЕНИЯ ----------
@dp.callback_query(F.data.startswith("dep_"))
async def deposit_amount(cb: CallbackQuery, state: FSMContext):
    if cb.data == "dep_custom":
        await state.set_state(DepositStates.waiting_custom)
        await cb.message.edit_text(
            "💸 Введите сумму в USDT (мин 10, макс 5000):\n\n"
            "Пример: 100\n\n"
            "/otmena - отмена",
            reply_markup=back_menu_kb()
        )
        await cb.answer()
        return
    
    amount = float(cb.data.split("_")[1])
    payment_id = create_manual_payment(cb.from_user.id, amount, "deposit")
    
    await cb.message.delete()
    await cb.message.answer(
        f"🧾 **СЧЁТ НА ПОПОЛНЕНИЕ**\n\n"
        f"💰 Сумма: **{amount} USDT**\n\n"
        f"📤 **Кошелёк для перевода (TRC20):**\n"
        f"<code>{USDT_WALLET}</code>\n\n"
        f"📝 **ОБЯЗАТЕЛЬНО укажите ID в комментарии:**\n"
        f"<code>pay_{payment_id}</code>\n\n"
        f"✅ После перевода нажмите «Я ПЕРЕВЁЛ»",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я ПЕРЕВЁЛ", callback_data=f"payment_done_{payment_id}")],
            [InlineKeyboardButton(text="❌ ОТМЕНИТЬ", callback_data=f"payment_cancel_{payment_id}")]
        ])
    )
    await state.clear()
    await cb.answer()

@dp.message(Command("otmena"))
async def cancel_input(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("❌ Действие отменено.", reply_markup=main_menu())

@dp.callback_query(F.data.startswith("payment_done_"))
async def payment_done(cb: CallbackQuery):
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    
    uid, amt, purpose, prd, qty, _ = pay
    
    text = f"💰 НОВЫЙ ПЛАТЁЖ!\n\n🆔 ID: {pid}\n👤 Пользователь: {uid}\n💰 Сумма: ${amt}\n📌 Назначение: {'Пополнение' if purpose == 'deposit' else 'Покупка'}"
    if purpose == "direct_purchase" and prd:
        text += f"\n📦 Товар: {PRODUCTS.get(prd, {}).get('name', prd)} x{qty}"
    
    await bot.send_message(ADMIN_ID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{pid}"),
         InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_decline_{pid}")]
    ]))
    
    await cb.message.edit_text("✅ Уведомление отправлено администратору!\n\n⏳ Ожидайте подтверждения (до 24 часов).", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(F.data.startswith("payment_cancel_"))
async def payment_cancel(cb: CallbackQuery):
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if pay and pay[5] == 'pending':
        update_payment_status(pid, "cancelled")
    await cb.message.edit_text("❌ Платёж отменён.", reply_markup=main_menu())
    await cb.answer()

# ---------- АДМИН ОБРАБОТЧИКИ ----------
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    users, sales, withdrawn = get_stats()
    
    text = f"📊 СТАТИСТИКА МАГАЗИНА\n\n"
    text += f"👥 Пользователей: {users}\n"
    text += f"💰 Продажи: ${sales:.2f}\n"
    text += f"💸 Выведено: ${withdrawn:.2f}\n"
    text += f"📈 В обороте: ${sales - withdrawn:.2f}\n\n"
    text += f"📦 Товары:\n"
    for pid, prod in PRODUCTS.items():
        cnt = get_available_codes_count(pid)
        text += f"• {prod['name']}: {cnt} шт\n"
    
    await cb.message.edit_text(text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_add_codes")
async def admin_add_codes_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=prod['name'], callback_data=f"add_codes_{pid}")] for pid, prod in PRODUCTS.items()
    ] + [[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_stats")]])
    
    await cb.message.edit_text("➕ Выберите товар для добавления промокодов:", reply_markup=kb)
    await state.set_state(AdminAddCodes.waiting_product)
    await cb.answer()

@dp.callback_query(AdminAddCodes.waiting_product, F.data.startswith("add_codes_"))
async def admin_add_codes_get(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    await state.update_data(product_id=pid)
    await state.set_state(AdminAddCodes.waiting_codes)
    await cb.message.edit_text(f"📝 Введите промокоды для {PRODUCTS[pid]['name']}\n\nКаждый код с новой строки:\n\nПример:\nCODE123\nCODE456\n\n/otmena - отмена")
    await cb.answer()

@dp.message(AdminAddCodes.waiting_codes)
async def admin_add_codes_save(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    data = await state.get_data()
    pid = data.get('product_id')
    if not pid:
        await msg.answer("Ошибка, начните заново")
        await state.clear()
        return
    
    codes = [line.strip().upper() for line in msg.text.split('\n') if line.strip()]
    added = add_promo_codes(pid, codes)
    
    await msg.answer(f"✅ Добавлено {added} промокодов для {PRODUCTS[pid]['name']}", reply_markup=admin_panel_kb())
    log_admin_action(ADMIN_ID, "add_codes", 0, f"{pid}: {added} кодов")
    await state.clear()

@dp.callback_query(F.data == "admin_users")
async def admin_users(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    users = get_all_users()
    text = "👥 ПОЛЬЗОВАТЕЛИ:\n\n"
    for uid, uname, fname, joined, last, bal in users[:20]:
        text += f"🆔 {uid}\n📝 @{uname if uname else 'нет'}\n👤 {fname}\n💰 ${bal:.2f}\n📅 {joined[:16] if joined else '?'}\n\n"
    
    if len(users) > 20:
        text += f"... и ещё {len(users) - 20} пользователей"
    
    await cb.message.edit_text(text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_payments")
async def admin_payments(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT payment_id, user_id, amount_usd, purpose, product_id, quantity, created_at FROM manual_payments WHERE status='pending'")
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        await cb.message.edit_text("💳 Нет ожидающих платежей.", reply_markup=admin_panel_kb())
        await cb.answer()
        return
    
    text = "💳 ОЖИДАЮТ ПОДТВЕРЖДЕНИЯ:\n\n"
    for pid, uid, amt, purpose, prd, qty, cr in rows:
        text += f"🆔 {pid}\n👤 {uid}\n💰 ${amt}\n📌 {purpose}\n"
        if prd:
            text += f"📦 {PRODUCTS.get(prd, {}).get('name', prd)} x{qty}\n"
        text += f"🕒 {cr[:19]}\n\n"
    
    text += "Для подтверждения: /confirm <id>\nДля отклонения: /decline <id>"
    
    await cb.message.edit_text(text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, amount_usd, wallet_address, created_at FROM withdrawal_requests WHERE status='pending'")
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        await cb.message.edit_text("💸 Нет заявок на вывод.", reply_markup=admin_panel_kb())
        await cb.answer()
        return
    
    text = "💸 ЗАЯВКИ НА ВЫВОД:\n\n"
    for rid, uid, amt, wallet, cr in rows:
        text += f"🆔 #{rid}\n👤 {uid}\n💰 ${amt}\n📤 {wallet[:20]}...\n🕒 {cr[:19]}\n\n"
    
    text += "Для подтверждения: /process_withdraw <id>"
    
    await cb.message.edit_text(text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_export")
async def admin_export(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    await cb.message.edit_text("📤 Выберите тип экспорта:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Пользователи", callback_data="export_users")],
        [InlineKeyboardButton(text="💰 Транзакции", callback_data="export_transactions")],
        [InlineKeyboardButton(text="💸 Выводы", callback_data="export_withdrawals")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_stats")]
    ]))
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    await state.set_state(AdminBroadcast.waiting_message)
    await cb.message.edit_text("📢 Введите текст для массовой рассылки:\n\nПоддерживается HTML-разметка\n\n/otmena - отмена")
    await cb.answer()

@dp.message(AdminBroadcast.waiting_message)
async def admin_broadcast_send(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    text = msg.text
    users = get_all_users()
    success = 0
    
    await msg.answer(f"📢 Начинаю рассылку {len(users)} пользователям...")
    
    for user in users:
        try:
            await bot.send_message(user[0], f"📢 МАССОВАЯ РАССЫЛКА\n\n{text}", parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except:
            pass
    
    await msg.answer(f"✅ Рассылка завершена!\n📨 Отправлено: {success} из {len(users)}")
    log_admin_action(ADMIN_ID, "broadcast", 0, f"Отправлено {success} пользователям")
    await state.clear()

@dp.callback_query(F.data == "admin_coupons")
async def admin_coupons_menu(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    await cb.message.edit_text("🎫 УПРАВЛЕНИЕ КУПОНАМИ", reply_markup=admin_coupons_kb())
    await cb.answer()

@dp.callback_query(F.data == "coupon_create")
async def admin_coupon_create_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    await state.set_state(AdminCoupon.waiting_data)
    await cb.message.edit_text("🎫 СОЗДАНИЕ КУПОНА\n\nВведите данные в формате:\n`<скидка%> [user_id] [дней]`\n\nПримеры:\n• `15` - скидка 15% для всех на 30 дней\n• `25 123456789` - скидка 25% для пользователя на 30 дней\n• `10 0 7` - скидка 10% для всех на 7 дней\n\n/otmena - отмена", parse_mode="Markdown")
    await cb.answer()

@dp.message(AdminCoupon.waiting_data)
async def admin_coupon_create_save(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    parts = msg.text.split()
    if len(parts) < 1:
        await msg.answer("❌ Неверный формат!")
        return
    
    try:
        discount = int(parts[0])
        if discount < 1 or discount > 90:
            await msg.answer("❌ Скидка должна быть от 1 до 90%")
            return
        
        user_id = int(parts[1]) if len(parts) > 1 else None
        days = int(parts[2]) if len(parts) > 2 else 30
        
        coupon_id = hashlib.md5(f"{uuid.uuid4()}{datetime.now()}".encode()).hexdigest()[:8].upper()
        valid_until = (datetime.now() + timedelta(days=days)).isoformat()
        
        conn = sqlite3.connect("fadex_bot.db")
        cur = conn.cursor()
        cur.execute("INSERT INTO coupons (id, discount_percent, valid_until, user_id, created_at) VALUES (?, ?, ?, ?, ?)",
                    (coupon_id, discount, valid_until, user_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await msg.answer(f"✅ КУПОН СОЗДАН!\n\nКод: `{coupon_id}`\nСкидка: {discount}%\nДействителен: {days} дней\nПользователь: {user_id if user_id else 'для всех'}", parse_mode="Markdown", reply_markup=admin_panel_kb())
        log_admin_action(ADMIN_ID, "create_coupon", user_id or 0, f"скидка {discount}%")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "coupon_list")
async def admin_coupon_list(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT id, discount_percent, valid_until, user_id, used FROM coupons ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        await cb.message.edit_text("🎫 Нет созданных купонов.", reply_markup=admin_coupons_kb())
        await cb.answer()
        return
    
    text = "🎫 СПИСОК КУПОНОВ:\n\n"
    for cid, disc, valid, uid, used in rows:
        status = "❌" if used or datetime.fromisoformat(valid) < datetime.now() else "✅"
        user = f"для {uid}" if uid else "для всех"
        text += f"{status} `{cid}` - {disc}% ({user})\nдо {valid[:16]}\n\n"
    
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_coupons_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("export_"))
async def admin_export_do(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    export_type = cb.data.split("_")[1]
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    
    if export_type == "users":
        cur.execute("SELECT user_id, username, first_name, joined_at, balance_usd FROM users")
        rows = cur.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "username", "first_name", "joined_at", "balance_usd"])
        writer.writerows(rows)
        await cb.message.answer_document(types.BufferedInputFile(output.getvalue().encode('utf-8'), filename="users.csv"))
    elif export_type == "transactions":
        cur.execute("SELECT id, user_id, type, amount_usd, status, created_at FROM transactions")
        rows = cur.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "user_id", "type", "amount_usd", "status", "created_at"])
        writer.writerows(rows)
        await cb.message.answer_document(types.BufferedInputFile(output.getvalue().encode('utf-8'), filename="transactions.csv"))
    elif export_type == "withdrawals":
        cur.execute("SELECT id, user_id, amount_usd, wallet_address, status, created_at FROM withdrawal_requests")
        rows = cur.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "user_id", "amount_usd", "wallet_address", "status", "created_at"])
        writer.writerows(rows)
        await cb.message.answer_document(types.BufferedInputFile(output.getvalue().encode('utf-8'), filename="withdrawals.csv"))
    
    conn.close()
    await cb.answer()

@dp.callback_query(F.data == "admin_logout")
async def admin_logout(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("Нет прав")
        return
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("DELETE FROM admin_sessions WHERE admin_id = ?", (ADMIN_ID,))
    conn.commit()
    conn.close()
    
    await cb.message.edit_text("🚪 Вы вышли из админ-панели.", reply_markup=main_menu())
    await cb.answer()

# ---------- АДМИН КОМАНДЫ ----------
@dp.message(Command("confirm"))
async def admin_confirm(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /confirm <payment_id>")
        return
    
    pid = args[1]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await msg.answer("❌ Платёж не найден или уже обработан")
        return
    
    uid, amt, purpose, prd, qty, _ = pay
    
    if purpose == "deposit":
        update_balance(uid, amt)
        add_transaction(uid, "deposit", amt, "completed")
        update_payment_status(pid, "confirmed", datetime.now())
        await bot.send_message(uid, f"✅ Ваш баланс пополнен на ${amt} USDT!\n\nБлагодарим за доверие!")
        await msg.answer(f"✅ Пополнение ${amt} для {uid} подтверждено")
        log_admin_action(ADMIN_ID, "confirm_deposit", uid, f"${amt}")
        
    elif purpose == "direct_purchase":
        available = get_available_codes_count(prd)
        if available < qty:
            await msg.answer(f"❌ Не хватает кодов! Осталось {available} шт")
            return
        
        codes = []
        for _ in range(qty):
            code = get_unused_code(prd)
            if code:
                codes.append(code)
        
        if len(codes) < qty:
            await msg.answer("❌ Ошибка получения кодов")
            return
        
        add_transaction(uid, "purchase", amt, "completed", prd, ",".join(codes))
        update_payment_status(pid, "confirmed", datetime.now())
        
        codes_text = "\n".join([f"{i+1}. <code>{code}</code>" for i, code in enumerate(codes)])
        prod = PRODUCTS[prd]
        
        await bot.send_message(uid, f"✅ ОПЛАТА ПОДТВЕРЖДЕНА!\n\n📦 {prod['name']}\n🔢 {qty} шт\n💰 ${amt}\n\n🎫 ПРОМОКОДЫ:\n{codes_text}\n\n📘 Инструкция: {MANUAL_URL}", parse_mode="HTML")
        await msg.answer(f"✅ Покупка {prod['name']} x{qty} для {uid} подтверждена")
        log_admin_action(ADMIN_ID, "confirm_purchase", uid, f"{prod['name']} x{qty} ${amt}")

@dp.message(Command("decline"))
async def admin_decline(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /decline <payment_id>")
        return
    
    pid = args[1]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await msg.answer("❌ Платёж не найден или уже обработан")
        return
    
    uid, amt, _, _, _, _ = pay
    update_payment_status(pid, "declined", datetime.now())
    await bot.send_message(uid, f"❌ Ваш платёж на ${amt} USDT отклонён.\n\nВозможные причины:\n• Неверная сумма\n• Не указан ID платежа\n• Технические проблемы\n\nСвяжитесь с поддержкой: {ADMIN_USERNAME}")
    await msg.answer(f"❌ Платёж {pid} отклонён")
    log_admin_action(ADMIN_ID, "decline_payment", uid, f"${amt}")

@dp.message(Command("process_withdraw"))
async def admin_process_withdraw(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /process_withdraw <id_заявки>")
        return
    
    rid = int(args[1])
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount_usd, wallet_address FROM withdrawal_requests WHERE id = ? AND status = 'pending'", (rid,))
    row = cur.fetchone()
    
    if not row:
        await msg.answer("❌ Заявка не найдена или уже обработана")
        conn.close()
        return
    
    uid, amt, wallet = row
    cur.execute("UPDATE withdrawal_requests SET status = 'completed', processed_at = ? WHERE id = ?", (datetime.now().isoformat(), rid))
    conn.commit()
    conn.close()
    
    await bot.send_message(uid, f"✅ ВЫВОД СРЕДСТВ ПОДТВЕРЖДЁН!\n\n💰 Сумма: ${amt}\n📤 Адрес: {wallet}\n\nСредства будут отправлены в ближайшее время.")
    await msg.answer(f"✅ Заявка #{rid} на вывод ${amt} подтверждена")
    log_admin_action(ADMIN_ID, "process_withdraw", uid, f"${amt}")

# ---------- НАВИГАЦИЯ ----------
@dp.callback_query(F.data == "menu")
async def to_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    await cb.message.answer("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(F.data == "back_products")
async def back_to_products(cb: CallbackQuery):
    await cb.message.edit_text("📦 Наши товары:", reply_markup=products_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("back_purchase_"))
async def back_to_purchase(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[-1]
    prod = PRODUCTS[pid]
    await cb.message.edit_text(f"🔹 {prod['name']}\n💰 ${prod['price_usd']}\n📦 {prod['desc']}\n\n🛒 Способ оплаты:", reply_markup=purchase_kb(pid))
    await state.clear()
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_confirm_"))
async def admin_confirm_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    
    uid, amt, purpose, prd, qty, _ = pay
    
    if purpose == "deposit":
        update_balance(uid, amt)
        add_transaction(uid, "deposit", amt, "completed")
        update_payment_status(pid, "confirmed", datetime.now())
        await bot.send_message(uid, f"✅ Баланс пополнен на ${amt} USDT!")
        await cb.message.edit_text(f"✅ Пополнение ${amt} для {uid} подтверждено", reply_markup=admin_panel_kb())
        log_admin_action(ADMIN_ID, "confirm_deposit", uid, f"${amt}")
        
    elif purpose == "direct_purchase":
        available = get_available_codes_count(prd)
        if available < qty:
            await cb.answer(f"❌ Осталось {available} шт", show_alert=True)
            return
        
        codes = []
        for _ in range(qty):
            code = get_unused_code(prd)
            if code:
                codes.append(code)
        
        if len(codes) < qty:
            await cb.answer("Ошибка получения кодов")
            return
        
        add_transaction(uid, "purchase", amt, "completed", prd, ",".join(codes))
        update_payment_status(pid, "confirmed", datetime.now())
        
        codes_text = "\n".join([f"{i+1}. <code>{code}</code>" for i, code in enumerate(codes)])
        prod = PRODUCTS[prd]
        
        await bot.send_message(uid, f"✅ ОПЛАТА ПОДТВЕРЖДЕНА!\n\n📦 {prod['name']}\n🔢 {qty} шт\n💰 ${amt}\n\n🎫 ПРОМОКОДЫ:\n{codes_text}\n\n📘 Инструкция: {MANUAL_URL}", parse_mode="HTML")
        await cb.message.edit_text(f"✅ Покупка {prod['name']} x{qty} для {uid} подтверждена", reply_markup=admin_panel_kb())
        log_admin_action(ADMIN_ID, "confirm_purchase", uid, f"{prod['name']} x{qty} ${amt}")
    
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_decline_"))
async def admin_decline_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    
    uid, amt, _, _, _, _ = pay
    update_payment_status(pid, "declined", datetime.now())
    await bot.send_message(uid, f"❌ Платёж ${amt} отклонён. Свяжитесь с поддержкой: {ADMIN_USERNAME}")
    await cb.message.edit_text(f"❌ Платёж {pid} отклонён", reply_markup=admin_panel_kb())
    log_admin_action(ADMIN_ID, "decline_payment", uid, f"${amt}")
    await cb.answer()

# ========== ЗАПУСК ==========
async def main():
    asyncio.create_task(start_web_server())
    
    init_db()
    load_products_from_db()
    seed_promo_codes()
    
    asyncio.create_task(cleanup_task())
    
    print("\n" + "="*60)
    print("✅ БОТ УСПЕШНО ЗАПУЩЕН!")
    print("="*60)
    print(f"👤 Админ: {ADMIN_ID} {ADMIN_USERNAME}")
    print(f"💰 Кошелёк USDT (TRC20): {USDT_WALLET}")
    print(f"🎫 Система купонов: активна")
    print(f"🛡️ Защита от двойной траты: активна")
    print(f"📊 Rate limiting: активен")
    print("="*60 + "\n")
    
    await dp.start_polling(bot)

async def cleanup_task():
    while True:
        try:
            cleanup_old_payments()
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")
        await asyncio.sleep(86400)

def cleanup_old_payments():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=CLEANUP_DAYS)).isoformat()
    cur.execute("DELETE FROM manual_payments WHERE status='pending' AND created_at < ?", (cutoff,))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
