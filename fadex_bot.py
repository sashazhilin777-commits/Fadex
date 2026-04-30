import sys
import asyncio
import sqlite3
import uuid
import re
import csv
import io
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from contextlib import contextmanager
from collections import defaultdict
import hashlib
import json

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

load_dotenv()

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

# ========== RATE LIMITING ==========
user_commands = defaultdict(list)

def check_rate_limit(user_id: int, limit=30, window=60) -> bool:
    """Проверка лимита команд (30 команд в минуту)"""
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
    
    # Основные таблицы
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        joined_at TEXT,
        last_login TEXT,
        balance_usd REAL DEFAULT 0
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
    
    # Новая таблица для товаров (сохранение цен)
    cur.execute("""CREATE TABLE IF NOT EXISTS products_config (
        product_id TEXT PRIMARY KEY,
        price_usd REAL,
        old_price REAL,
        description TEXT
    )""")
    
    # Новая таблица для купонов
    cur.execute("""CREATE TABLE IF NOT EXISTS coupons (
        id TEXT PRIMARY KEY,
        discount_percent INTEGER,
        valid_until TEXT,
        user_id INTEGER,
        used BOOLEAN DEFAULT 0,
        created_at TEXT
    )""")
    
    # Новая таблица для заявок на рефанд
    cur.execute("""CREATE TABLE IF NOT EXISTS refund_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount_usd REAL,
        product_id TEXT,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        processed_at TEXT
    )""")
    
    # Добавляем недостающие колонки
    for col in ["first_name", "last_login", "joined_at"]:
        try:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
        except: pass
    
    for col in ["locked"]:
        try:
            cur.execute(f"ALTER TABLE manual_payments ADD COLUMN {col} BOOLEAN DEFAULT 0")
        except: pass
    
    conn.commit()
    
    # Сохраняем товары в БД
    save_products_to_db()
    
    conn.close()

def save_products_to_db():
    """Сохранение конфигурации товаров в БД"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    for pid, prod in PRODUCTS.items():
        cur.execute("REPLACE INTO products_config (product_id, price_usd, old_price, description) VALUES (?, ?, ?, ?)", 
                    (pid, prod['price_usd'], prod.get('old_price'), prod['desc']))
    conn.commit()
    conn.close()

def load_products_from_db():
    """Загрузка конфигурации товаров из БД"""
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
                else:
                    PRODUCTS[pid]['discount'] = None

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
        is_new = True
    else:
        cur.execute("UPDATE users SET username = ?, first_name = ?, last_login = ? WHERE user_id = ?",
                    (username, first_name, now, uid))
        is_new = False
    conn.commit()
    conn.close()
    return is_new

def get_all_users():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, joined_at, last_login, balance_usd FROM users ORDER BY joined_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows

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

def get_user_history(user_id: int, limit=30):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT type, amount_usd, status, created_at, product_id FROM transactions WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

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
    """Добавление промокодов с проверкой на дубликаты"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    added = 0
    duplicates = 0
    
    for code in codes:
        # Проверяем существование
        cur.execute("SELECT code FROM promo_codes WHERE code = ?", (code,))
        if cur.fetchone():
            duplicates += 1
            logger.warning(f"Дубликат промокода: {code}")
            continue
        cur.execute("INSERT INTO promo_codes (product_id, code, used) VALUES (?, ?, 0)", (product_id, code))
        added += 1
    
    conn.commit()
    conn.close()
    check_low_stock(product_id)
    return added, duplicates

def check_low_stock(product_id: str, threshold=3):
    cnt = get_available_codes_count(product_id)
    if cnt <= threshold:
        asyncio.create_task(bot.send_message(ADMIN_ID, f"⚠️ Остаток {PRODUCTS[product_id]['name']} – {cnt} шт"))

@contextmanager
def get_db_connection():
    conn = sqlite3.connect("fadex_bot.db", isolation_level=None)
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def atomic_purchase(user_id: int, product_id: str, qty: int) -> Tuple[list, float]:
    """Атомарная покупка с проверкой наличия кодов"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT balance_usd FROM users WHERE user_id = ? FOR UPDATE", (user_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError("Пользователь не найден")
        balance = row[0]
        price = PRODUCTS[product_id]['price_usd']
        total = price * qty
        if balance < total:
            raise ValueError("Недостаточно средств")
        
        cur.execute("SELECT id, code FROM promo_codes WHERE product_id = ? AND used = 0 LIMIT ? FOR UPDATE", (product_id, qty))
        rows = cur.fetchall()
        if len(rows) < qty:
            # Создаём заявку на рефанд
            create_refund_request(user_id, total, product_id, "Недостаточно промокодов в наличии")
            raise ValueError(f"Промокоды закончились (осталось {len(rows)} шт). Создана заявка на возврат средств.")
        
        ids = [r[0] for r in rows]
        codes = [r[1] for r in rows]
        cur.executemany("UPDATE promo_codes SET used = 1 WHERE id = ?", [(i,) for i in ids])
        cur.execute("UPDATE users SET balance_usd = balance_usd - ? WHERE user_id = ?", (total, user_id))
        cur.execute("""INSERT INTO transactions (user_id, type, amount_usd, status, product_id, promo_code, created_at)
                       VALUES (?, 'purchase', ?, 'completed', ?, ?, ?)""",
                    (user_id, total, product_id, ",".join(codes), datetime.now().isoformat()))
        return codes, total

def create_refund_request(user_id: int, amount: float, product_id: str, reason: str) -> int:
    """Создание заявки на возврат средств"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("""INSERT INTO refund_requests (user_id, amount_usd, product_id, reason, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (user_id, amount, product_id, reason, datetime.now().isoformat()))
    req_id = cur.lastrowid
    conn.commit()
    conn.close()
    asyncio.create_task(bot.send_message(ADMIN_ID, f"🔄 Создана заявка на рефанд #{req_id}\nПользователь: {user_id}\nСумма: ${amount}\nПричина: {reason}"))
    return req_id

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

def is_payment_locked(payment_id: str) -> bool:
    """Проверка блокировки платежа (защита от двойной траты)"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT locked FROM manual_payments WHERE payment_id = ?", (payment_id,))
    row = cur.fetchone()
    conn.close()
    return row and row[0] == 1

def lock_payment(payment_id: str):
    """Блокировка платежа"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("UPDATE manual_payments SET locked = 1 WHERE payment_id = ?", (payment_id,))
    conn.commit()
    conn.close()

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

def get_pending_payments():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT payment_id, user_id, amount_usd, purpose, product_id, quantity, created_at FROM manual_payments WHERE status='pending'")
    rows = cur.fetchall()
    conn.close()
    return rows

def cleanup_old_payments():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=CLEANUP_DAYS)).isoformat()
    cur.execute("DELETE FROM manual_payments WHERE status='pending' AND created_at < ?", (cutoff,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"Очистка: удалено {deleted} старых платежей")

def create_withdrawal_request(user_id: int, amount: float, wallet: str) -> int:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO withdrawal_requests (user_id, amount_usd, wallet_address, created_at) VALUES (?, ?, ?, ?)",
                (user_id, amount, wallet, datetime.now().isoformat()))
    req_id = cur.lastrowid
    conn.commit()
    conn.close()
    return req_id

def get_withdrawal_requests(status='pending'):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, amount_usd, wallet_address, created_at FROM withdrawal_requests WHERE status = ? ORDER BY created_at", (status,))
    rows = cur.fetchall()
    conn.close()
    return rows

def update_withdrawal_request(req_id: int, status: str):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("UPDATE withdrawal_requests SET status = ?, processed_at = ? WHERE id = ?",
                (status, datetime.now().isoformat(), req_id))
    conn.commit()
    conn.close()

def cancel_withdrawal_request(req_id: int, user_id: int) -> bool:
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, amount_usd, status FROM withdrawal_requests WHERE id = ? AND user_id = ?", (req_id, user_id))
    row = cur.fetchone()
    if row and row[3] == 'pending':
        amount = row[2]
        cur.execute("UPDATE withdrawal_requests SET status = 'cancelled', processed_at = ? WHERE id = ?", (datetime.now().isoformat(), req_id))
        update_balance(user_id, amount)
        add_transaction(user_id, "withdraw_cancel", amount, "completed")
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False

def log_admin_action(admin_id, action, target_user, details):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO admin_logs (admin_id, action, target_user, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (admin_id, action, target_user, details, datetime.now().isoformat()))
    conn.commit()
    conn.close()

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

def get_stats():
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users_cnt = cur.fetchone()[0]
    cur.execute("SELECT SUM(amount_usd) FROM transactions WHERE type='purchase' AND status='completed'")
    total_sales = cur.fetchone()[0] or 0
    cur.execute("SELECT SUM(amount_usd) FROM withdrawal_requests WHERE status='completed'")
    total_withdrawn = cur.fetchone()[0] or 0
    cur.execute("SELECT COUNT(*) FROM withdrawal_requests WHERE status='pending'")
    pending_withdrawals = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM manual_payments WHERE status='pending'")
    pending_payments = cur.fetchone()[0]
    conn.close()
    return users_cnt, total_sales, total_withdrawn, pending_withdrawals, pending_payments

def get_sales_stats(days=7):
    """Аналитика продаж за период"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    cur.execute("""
        SELECT product_id, COUNT(*), SUM(amount_usd) 
        FROM transactions 
        WHERE type='purchase' AND status='completed' AND created_at > ?
        GROUP BY product_id
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()
    return rows

def export_to_csv(table: str):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    if table == "users":
        cur.execute("SELECT user_id, username, first_name, joined_at, last_login, balance_usd FROM users")
        headers = ["user_id","username","first_name","joined_at","last_login","balance_usd"]
    elif table == "transactions":
        cur.execute("SELECT id, user_id, type, amount_usd, status, product_id, promo_code, created_at FROM transactions")
        headers = ["id","user_id","type","amount_usd","status","product_id","promo_code","created_at"]
    elif table == "withdrawals":
        cur.execute("SELECT id, user_id, amount_usd, wallet_address, status, created_at, processed_at FROM withdrawal_requests")
        headers = ["id","user_id","amount_usd","wallet_address","status","created_at","processed_at"]
    else:
        return None
    rows = cur.fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue().encode('utf-8')

def validate_ton_address(address: str) -> bool:
    if not (address.startswith("UQ") or address.startswith("EQ")):
        return False
    if len(address) != 48:
        return False
    if not re.match(r'^[A-Za-z0-9_-]+$', address):
        return False
    return True

def validate_sol_address(address: str) -> bool:
    """Валидация Solana адреса"""
    if len(address) < 32 or len(address) > 44:
        return False
    # Solana адреса в base58 (без 0, O, I, l)
    if not re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', address):
        return False
    return True

def validate_xrp_address(address: str) -> bool:
    """Валидация XRP адреса"""
    if not address.startswith('r'):
        return False
    if len(address) < 25 or len(address) > 34:
        return False
    if not re.match(r'^r[1-9A-HJ-NP-Za-km-z]{24,34}$', address):
        return False
    return True

def create_coupon(discount_percent: int, user_id: int = None, valid_days: int = 30) -> str:
    """Создание скидочного купона"""
    coupon_id = hashlib.md5(f"{uuid.uuid4()}{datetime.now()}".encode()).hexdigest()[:8].upper()
    valid_until = (datetime.now() + timedelta(days=valid_days)).isoformat()
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("INSERT INTO coupons (id, discount_percent, valid_until, user_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (coupon_id, discount_percent, valid_until, user_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    return coupon_id

def apply_coupon(coupon_code: str, user_id: int, total: float) -> Tuple[float, int]:
    """Применение купона, возвращает (новая_сумма, скидка_процент)"""
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    
    cur.execute("SELECT discount_percent, valid_until, used, user_id FROM coupons WHERE id = ?", (coupon_code,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        raise ValueError("Купон не найден")
    
    discount, valid_until, used, coupon_user_id = row
    
    if used:
        raise ValueError("Купон уже использован")
    
    if datetime.fromisoformat(valid_until) < datetime.now():
        raise ValueError("Купон просрочен")
    
    if coupon_user_id and coupon_user_id != user_id:
        raise ValueError("Купон не принадлежит вам")
    
    new_total = total * (100 - discount) / 100
    return new_total, discount

def mark_coupon_used(coupon_code: str):
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("UPDATE coupons SET used = 1 WHERE id = ?", (coupon_code,))
    conn.commit()
    conn.close()

# ========== СОЗДАНИЕ БОТА И ДИСПЕТЧЕРА ==========
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ========== АВТОПРОВЕРКА ПЛАТЕЖЕЙ ==========
async def check_ton_payments():
    if not TON_API_KEY:
        return []
    for attempt in range(RETRY_COUNT):
        try:
            url = f"https://toncenter.com/api/v2/getTransactions?address={USDT_WALLET}&limit=50&api_key={TON_API_KEY}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        payments = []
                        for tx in data.get('result', []):
                            in_msg = tx.get('in_msg', {})
                            if in_msg.get('source') == USDT_WALLET:
                                continue
                            comment = in_msg.get('message', '')
                            match = re.search(r'pay_([a-f0-9]{8})', comment or '', re.IGNORECASE)
                            if match:
                                payment_id = match.group(1)
                                amount_nano = int(in_msg.get('value', '0'))
                                amount = amount_nano / 1e9
                                payments.append((payment_id, amount))
                        return payments
                    logger.warning(f"Toncenter статус {resp.status}, попытка {attempt+1}")
        except Exception as e:
            logger.error(f"Ошибка Toncenter: {e}, попытка {attempt+1}")
        await asyncio.sleep(RETRY_DELAY * (attempt+1))
    await bot.send_message(ADMIN_ID, "⚠️ Не удалось подключиться к Toncenter (автопроверка отключена)")
    return []

async def auto_confirm_payment(payment_id: str, amount: float):
    if is_payment_locked(payment_id):
        logger.info(f"Платёж {payment_id} уже обрабатывается")
        return
    
    lock_payment(payment_id)
    
    pay = get_payment(payment_id)
    if not pay or pay[5] != 'pending':
        return
    uid, pay_amount, purpose, pid, qty, _ = pay
    if abs(pay_amount - amount) > 0.01:
        await bot.send_message(ADMIN_ID, f"⚠️ Сумма платежа {payment_id} ({amount}) не совпадает с {pay_amount}. Ручное подтверждение.")
        return
    if purpose == "deposit":
        update_balance(uid, pay_amount)
        add_transaction(uid, "deposit", pay_amount, "completed")
        update_payment_status(payment_id, "confirmed", datetime.now())
        await bot.send_message(uid, f"✅ Баланс автоматически пополнен на ${pay_amount}!")
        await bot.send_message(ADMIN_ID, f"🤖 Автопополнение: {uid} на ${pay_amount}")
        log_admin_action(0, "auto_confirm", uid, f"платёж {payment_id}")
    elif purpose == "direct_purchase":
        prod = PRODUCTS[pid]
        available = get_available_codes_count(pid)
        if available < qty:
            await bot.send_message(ADMIN_ID, f"⚠️ Не хватает кодов для {pid}. Платёж {payment_id} требует ручного вмешательства.")
            return
        codes = []
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT code FROM promo_codes WHERE product_id = ? AND used = 0 LIMIT ? FOR UPDATE", (pid, qty))
            rows = cur.fetchall()
            if len(rows) < qty:
                return
            codes = [r[0] for r in rows]
            cur.executemany("UPDATE promo_codes SET used = 1 WHERE code = ?", [(c,) for c in codes])
            cur.execute("UPDATE manual_payments SET status = 'confirmed', confirmed_at = ? WHERE payment_id = ?", (datetime.now().isoformat(), payment_id))
            cur.execute("INSERT INTO transactions (user_id, type, amount_usd, status, product_id, promo_code, created_at) VALUES (?, 'purchase', ?, 'completed', ?, ?, ?)",
                        (uid, pay_amount, pid, ",".join(codes), datetime.now().isoformat()))
        msg_text = f"✅ Оплата автоматически получена!\n📦 {prod['name']}\n🔢 {qty} шт\n💲 ${pay_amount}\n🎫 Промокоды:\n" + "\n".join(f"{i}. <code>{c}</code>" for i,c in enumerate(codes,1)) + "\n\n<i>Сохранены в «Мои покупки»</i>"
        await bot.send_message(uid, msg_text, parse_mode="HTML")
        await send_manual(uid, bot)
        await bot.send_message(ADMIN_ID, f"🤖 Автопокупка: {uid} купил {prod['name']} x{qty} за ${pay_amount}")
        log_admin_action(0, "auto_confirm", uid, f"платёж {payment_id}")

async def ton_payment_monitor():
    if not TON_API_KEY:
        return
    while True:
        try:
            payments = await check_ton_payments()
            for pid, amt in payments:
                await auto_confirm_payment(pid, amt)
        except Exception as e:
            logger.error(f"Ошибка мониторинга TON: {e}")
        await asyncio.sleep(30)

async def check_ton_api_health():
    """Автоматическая проверка доступности TON API"""
    if not TON_API_KEY:
        return
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://toncenter.com/api/v2/getBlockHeader?api_key={TON_API_KEY}", timeout=5) as resp:
                    if resp.status != 200:
                        await bot.send_message(ADMIN_ID, "⚠️ TON API недоступен! Автопроверка платежей может не работать.")
                    else:
                        logger.info("TON API работает нормально")
        except Exception as e:
            logger.error(f"Ошибка проверки TON API: {e}")
            await bot.send_message(ADMIN_ID, f"⚠️ TON API ошибка подключения: {str(e)[:100]}")
        await asyncio.sleep(3600)  # проверять каждый час

async def cleanup_task():
    while True:
        try:
            cleanup_old_payments()
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")
        await asyncio.sleep(86400)

# ========== КЛАВИАТУРЫ ==========
def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍️ Товары"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="💰 Пополнить"), KeyboardButton(text="📦 Мои покупки")],
            [KeyboardButton(text="📜 Правила"), KeyboardButton(text="🆘 Поддержка")],
            [KeyboardButton(text="🌐 FadeX.cc"), KeyboardButton(text="📊 Наличие")],
            [KeyboardButton(text="💸 Вывод средств"), KeyboardButton(text="🎫 Купон")]
        ],
        resize_keyboard=True
    )

def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
         InlineKeyboardButton(text="➕ Коды", callback_data="admin_addcodes")],
        [InlineKeyboardButton(text="⏳ Выводы", callback_data="admin_withdrawals"),
         InlineKeyboardButton(text="💳 Платежи", callback_data="admin_payments")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
         InlineKeyboardButton(text="📤 Экспорт", callback_data="admin_export")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
         InlineKeyboardButton(text="🎫 Купоны", callback_data="admin_coupons")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_logout"),
         InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
    ])

def products_kb():
    btns = []
    for pid, prod in PRODUCTS.items():
        if prod.get("old_price"):
            price_text = f"~~{prod['old_price']}$~~ 🔥 -{prod['discount']}% SALE {prod['price_usd']}$"
        else:
            price_text = f"{prod['price_usd']}$"
        btns.append([InlineKeyboardButton(text=f"{prod['name']} — {price_text}", callback_data=f"prod_{pid}")])
    btns.append([InlineKeyboardButton(text="◀️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def purchase_kb(pid: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 С баланса", callback_data=f"balance_buy_{pid}"),
         InlineKeyboardButton(text="💸 Напрямую", callback_data=f"direct_pay_{pid}")],
        [InlineKeyboardButton(text="🎫 Применить купон", callback_data=f"coupon_{pid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_products")]
    ])

def quantity_kb(pid: str, price: float):
    btns = []
    for q in [1,2,3,4,5]:
        btns.append([InlineKeyboardButton(text=f"{q} шт — {price*q}$", callback_data=f"qty_{pid}_{q}")])
    btns.append([
        InlineKeyboardButton(text="➖", callback_data=f"dec_{pid}"),
        InlineKeyboardButton(text="➕", callback_data=f"inc_{pid}"),
        InlineKeyboardButton(text="✅", callback_data=f"confirm_{pid}")
    ])
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

async def send_manual(chat_id: int, bot: Bot):
    await bot.send_message(chat_id, f"📘 Мануал: {MANUAL_URL}", parse_mode="Markdown", reply_markup=back_menu_kb())

async def safe_edit_message(message: types.Message, text: str, parse_mode: str = None, reply_markup=None):
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

# ========== FSM ==========
class DepositStates(StatesGroup):
    waiting_custom = State()

class BalanceBuy(StatesGroup):
    quantity = State()
    confirm = State()
    coupon_applied = State()

class DirectPay(StatesGroup):
    quantity = State()
    coupon_applied = State()

class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_wallet = State()

class AdminAddCodes(StatesGroup):
    waiting_product = State()
    waiting_codes = State()

class AdminBroadcast(StatesGroup):
    waiting_message = State()

class AdminCoupon(StatesGroup):
    waiting_action = State()
    waiting_coupon_data = State()

# ========== ОБРАБОТЧИКИ ==========
@dp.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    
    uid = msg.from_user.id
    uname = msg.from_user.username or ""
    fname = msg.from_user.first_name or "Пользователь"
    is_new = register_or_update_user(uid, uname, fname)
    welcome_text = (
        f"✨ Добро пожаловать, {fname}! ✨\n\n"
        "Промокоды на TON, SOL, XRP. Мгновенная выдача.\n"
        "Пополнение USDT (TON) – ручное.\n\n"
        "🛍️ Товары – выбор промокода\n"
        "👤 Профиль – баланс\n"
        "📘 Мануал – инструкция по активации\n"
        "💸 Вывод средств\n"
        "🎫 Купон – активировать скидочный купон\n\n"
        "Все вопросы – в поддержку."
    )
    await msg.answer(welcome_text, parse_mode="HTML", reply_markup=main_menu())
    if is_new:
        await bot.send_message(ADMIN_ID, f"🆕 Новый пользователь: {uid} @{uname} ({fname})")

@dp.message(Command("help"))
async def help_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    
    help_text = """
🤖 Команды бота:

🛍️ /start - Главное меню
💰 /balance - Баланс
📜 /history - История операций
📘 /manual - Инструкция по активации
💸 /cancel_withdraw <id> - Отменить заявку на вывод
🎫 /coupon <код> - Активировать купон
🆘 /help - Эта справка

👑 Админ-команды:
/admin login <пароль> - Вход в админку
/edit_price, /edit_desc, /edit_oldprice - Управление товарами
/broadcast - Массовая рассылка
/stock <id> - Остаток кодов
/list_products - Список товаров
/create_coupon <скидка%> [user_id] - Создать купон
"""
    await msg.answer(help_text)

@dp.message(Command("manual"))
async def manual_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await send_manual(msg.chat.id, bot)

@dp.message(Command("balance"))
async def balance_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer(f"💰 Баланс: ${get_balance(msg.from_user.id):.2f}")

@dp.message(Command("history"))
async def history_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    rows = get_user_history(msg.from_user.id)
    if not rows:
        await msg.answer("История пуста.")
        return
    text = "📜 История операций:\n\n"
    for typ, amt, status, dt, pid in rows:
        text += f"{dt[:16]} | {typ} | {amt}$ | {status}\n"
    await msg.answer(text)

@dp.message(Command("cancel_withdraw"))
async def cancel_withdraw_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /cancel_withdraw <id_заявки>")
        return
    try:
        req_id = int(args[1])
    except:
        await msg.answer("ID должно быть числом")
        return
    if cancel_withdrawal_request(req_id, msg.from_user.id):
        await msg.answer(f"✅ Заявка #{req_id} отменена, средства возвращены.")
    else:
        await msg.answer("❌ Заявка не найдена или уже обработана.")

@dp.message(Command("coupon"))
async def apply_coupon_cmd(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /coupon <код_купона>")
        return
    
    coupon_code = args[1].upper()
    try:
        # Просто проверяем купон (без применения)
        conn = sqlite3.connect("fadex_bot.db")
        cur = conn.cursor()
        cur.execute("SELECT discount_percent, valid_until, used FROM coupons WHERE id = ?", (coupon_code,))
        row = cur.fetchone()
        conn.close()
        
        if not row:
            await msg.answer("❌ Купон не найден")
            return
        
        discount, valid_until, used = row
        
        if used:
            await msg.answer("❌ Купон уже использован")
            return
        
        if datetime.fromisoformat(valid_until) < datetime.now():
            await msg.answer("❌ Купон просрочен")
            return
        
        await msg.answer(f"✅ Купон на {discount}% действителен! Примените его при покупке товара.")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("admin"))
async def admin_cmd(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    args = msg.text.split()
    if len(args) >= 2 and args[1] == "login":
        pwd = args[2] if len(args) > 2 else ""
        if admin_login(ADMIN_ID, pwd):
            await msg.answer("✅ Вы вошли в админ-панель на 1 час.", reply_markup=admin_panel_kb())
        else:
            await msg.answer("❌ Неверный пароль.")
    else:
        if is_admin_logged_in(ADMIN_ID):
            await msg.answer("🔐 Админ-панель", reply_markup=admin_panel_kb())
        else:
            await msg.answer("⛔ Доступ запрещён. Используйте /admin login <пароль>")

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
    await safe_edit_message(cb.message, "🚪 Вы вышли из админ-панели.")
    await cb.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    u, s, w, pw, pp = get_stats()
    sales_stats = get_sales_stats(7)
    stats_text = f"📊 Статистика\n👤 {u}\n💰 Продажи ${s:.2f}\n💸 Выведено ${w:.2f}\n⏳ Выводов {pw}\n💳 Платежей {pp}\n\n📈 Продажи за 7 дней:\n"
    for pid, cnt, total in sales_stats:
        stats_text += f"• {PRODUCTS.get(pid, {}).get('name', pid)}: {cnt} шт (${total:.2f})\n"
    await safe_edit_message(cb.message, stats_text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_addcodes")
async def admin_addcodes_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=prod['name'], callback_data=f"addcodes_prod_{pid}")] for pid, prod in PRODUCTS.items()])
    await safe_edit_message(cb.message, "➕ Выберите товар для добавления кодов:", reply_markup=kb)
    await state.set_state(AdminAddCodes.waiting_product)
    await cb.answer()

@dp.callback_query(AdminAddCodes.waiting_product, F.data.startswith("addcodes_prod_"))
async def admin_addcodes_get_codes(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    await state.update_data(product_id=pid)
    await state.set_state(AdminAddCodes.waiting_codes)
    await safe_edit_message(cb.message, f"Введите промокоды для {PRODUCTS[pid]['name']} (каждый с новой строки):")
    await cb.answer()

@dp.message(AdminAddCodes.waiting_codes)
async def admin_addcodes_save(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    data = await state.get_data()
    pid = data['product_id']
    codes = [line.strip() for line in msg.text.split('\n') if line.strip()]
    added, duplicates = add_promo_codes(pid, codes)
    await msg.answer(f"✅ Добавлено {added} промокодов для {PRODUCTS[pid]['name']}\n⚠️ Пропущено дубликатов: {duplicates}", reply_markup=admin_panel_kb())
    log_admin_action(ADMIN_ID, "add_codes", 0, f"{pid}: {added} кодов, дубликатов {duplicates}")
    await state.clear()

@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals_list(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    rows = get_withdrawal_requests('pending')
    if not rows:
        await safe_edit_message(cb.message, "Нет заявок на вывод.", reply_markup=admin_panel_kb())
        return
    text = "⏳ ЗАЯВКИ НА ВЫВОД:\n\n"
    for rid, uid, amt, wal, cr in rows:
        text += f"#{rid} | {uid} | ${amt}\n   {wal[:20]}...\n   {cr[:16]}\n\n"
    text += "Команда: /process_withdraw <id>"
    await safe_edit_message(cb.message, text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_payments")
async def admin_payments_list(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    rows = get_pending_payments()
    if not rows:
        await safe_edit_message(cb.message, "Нет платежей на подтверждении.", reply_markup=admin_panel_kb())
        return
    text = "💳 ОЖИДАЮТ ПОДТВЕРЖДЕНИЯ:\n\n"
    for pid, uid, amt, pur, prd, qty, cr in rows:
        text += f"ID: {pid}\n👤 {uid}\n💰 ${amt}\n📌 {pur}\n"
        if prd:
            text += f"📦 {PRODUCTS.get(prd,{}).get('name',prd)} x{qty}\n"
        text += f"🕒 {cr[:16]}\n\n"
    text += "Команды: /confirm <id> или /decline <id>"
    await safe_edit_message(cb.message, text, reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_users")
async def admin_users_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    rows = get_all_users()
    if not rows:
        await safe_edit_message(cb.message, "Нет пользователей.", reply_markup=admin_panel_kb())
        return
    text = "👥 ПОЛЬЗОВАТЕЛИ:\n\n"
    for uid, uname, fname, joined, last, bal in rows:
        j_short = joined[:16] if joined else "—"
        l_short = last[:16] if last else "—"
        uname_disp = f"@{uname}" if uname else "❌"
        text += f"<code>{uid}</code> | {uname_disp} | {fname}\n📅 {j_short}\n🕒 {l_short}\n💰 ${bal:.2f}\n\n"
        if len(text) > 3800:
            await cb.message.answer(text, parse_mode="HTML")
            text = ""
    if text:
        await safe_edit_message(cb.message, text, parse_mode="HTML", reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_export")
async def admin_export_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Пользователи", callback_data="export_users")],
        [InlineKeyboardButton(text="💰 Транзакции", callback_data="export_transactions")],
        [InlineKeyboardButton(text="💸 Выводы", callback_data="export_withdrawals")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_stats")]
    ])
    await safe_edit_message(cb.message, "Выберите тип экспорта:", reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    await state.set_state(AdminBroadcast.waiting_message)
    await safe_edit_message(cb.message, "📢 Введите текст для массовой рассылки:\n\n(можно использовать HTML разметку)")
    await cb.answer()

@dp.message(AdminBroadcast.waiting_message)
async def admin_broadcast_send(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    text = msg.text
    users = get_all_users()
    success = 0
    fail = 0
    
    await msg.answer(f"📢 Начинаю рассылку для {len(users)} пользователей...")
    
    for user in users:
        try:
            await bot.send_message(user[0], f"📢 МАССОВАЯ РАССЫЛКА\n\n{text}", parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)  # защита от блокировки
        except Exception as e:
            fail += 1
            logger.error(f"Ошибка отправки {user[0]}: {e}")
    
    await msg.answer(f"✅ Рассылка завершена!\n📨 Отправлено: {success}\n❌ Ошибок: {fail}")
    log_admin_action(ADMIN_ID, "broadcast", 0, f"Отправлено {success} из {len(users)}")
    await state.clear()

@dp.callback_query(F.data == "admin_coupons")
async def admin_coupons_menu(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать купон", callback_data="coupon_create")],
        [InlineKeyboardButton(text="📋 Список купонов", callback_data="coupon_list")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_stats")]
    ])
    await safe_edit_message(cb.message, "🎫 Управление купонами:", reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data == "coupon_create")
async def admin_coupon_create_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    await state.set_state(AdminCoupon.waiting_coupon_data)
    await safe_edit_message(cb.message, "Введите данные купона в формате:\n`<скидка%> [user_id] [дней]`\n\nПример: `25 123456789 30` - скидка 25% для пользователя на 30 дней\nИли: `15` - скидка 15% для всех на 30 дней", parse_mode="Markdown")
    await cb.answer()

@dp.message(AdminCoupon.waiting_coupon_data)
async def admin_coupon_create_save(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    
    parts = msg.text.split()
    if len(parts) < 1:
        await msg.answer("❌ Неверный формат. Используйте: `<скидка%> [user_id] [дней]`")
        return
    
    try:
        discount = int(parts[0])
        if discount < 1 or discount > 90:
            await msg.answer("❌ Скидка должна быть от 1 до 90%")
            return
        
        user_id = int(parts[1]) if len(parts) > 1 else None
        days = int(parts[2]) if len(parts) > 2 else 30
        
        coupon_id = create_coupon(discount, user_id, days)
        await msg.answer(f"✅ Купон создан!\n\nКод: `{coupon_id}`\nСкидка: {discount}%\nДействителен: {days} дней\nПользователь: {user_id if user_id else 'для всех'}", parse_mode="Markdown")
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
        await safe_edit_message(cb.message, "Нет созданных купонов.")
        return
    
    text = "🎫 Последние купоны:\n\n"
    for cid, disc, valid_until, uid, used in rows:
        status = "✅ Активен" if not used and datetime.fromisoformat(valid_until) > datetime.now() else "❌ Использован/Просрочен"
        user_info = f"для {uid}" if uid else "для всех"
        text += f"• `{cid}` - {disc}% ({user_info})\n  {status}\n  до {valid_until[:16]}\n\n"
    
    await safe_edit_message(cb.message, text, parse_mode="Markdown", reply_markup=admin_panel_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("export_"))
async def admin_export_do(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет доступа")
        return
    typ = cb.data.split("_")[1]
    fname = {"users":"users.csv","transactions":"transactions.csv","withdrawals":"withdrawals.csv"}.get(typ)
    if not fname:
        await cb.answer("Неизвестный тип")
        return
    data = export_to_csv(typ)
    if data:
        await cb.message.answer_document(types.BufferedInputFile(data, filename=fname))
    else:
        await cb.message.answer("Нет данных для экспорта.")
    await cb.answer()

@dp.message(Command("process_withdraw"))
async def admin_process_withdraw(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /process_withdraw <id>")
        return
    rid = int(args[1])
    rows = get_withdrawal_requests('pending')
    if not any(r[0]==rid for r in rows):
        await msg.answer("Заявка не найдена")
        return
    
    # Проверяем адрес
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount_usd, wallet_address FROM withdrawal_requests WHERE id = ?", (rid,))
    uid, amt, wal = cur.fetchone()
    conn.close()
    
    # Валидация адреса в зависимости от сети (можно расширить)
    if not (validate_ton_address(wal) or validate_sol_address(wal) or validate_xrp_address(wal)):
        await msg.answer(f"⚠️ Адрес {wal} не проходит валидацию. Всё равно подтвердить?\n/force_process_withdraw {rid}")
        return
    
    update_withdrawal_request(rid, "completed")
    await bot.send_message(uid, f"✅ Вывод ${amt} выполнен на {wal}.")
    await msg.answer(f"✅ Заявка #{rid} подтверждена.")
    log_admin_action(ADMIN_ID, "process_withdraw", uid, f"сумма ${amt}")

@dp.message(Command("force_process_withdraw"))
async def admin_force_process_withdraw(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /force_process_withdraw <id>")
        return
    rid = int(args[1])
    rows = get_withdrawal_requests('pending')
    if not any(r[0]==rid for r in rows):
        await msg.answer("Заявка не найдена")
        return
    
    conn = sqlite3.connect("fadex_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount_usd, wallet_address FROM withdrawal_requests WHERE id = ?", (rid,))
    uid, amt, wal = cur.fetchone()
    conn.close()
    
    update_withdrawal_request(rid, "completed")
    await bot.send_message(uid, f"✅ Вывод ${amt} выполнен на {wal} (принудительно).")
    await msg.answer(f"✅ Заявка #{rid} принудительно подтверждена.")
    log_admin_action(ADMIN_ID, "force_process_withdraw", uid, f"сумма ${amt}")

@dp.message(Command("confirm"))
async def admin_confirm_payment(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /confirm <payment_id>")
        return
    
    pid = args[1]
    
    # Защита от двойной траты
    if is_payment_locked(pid):
        await msg.answer("⚠️ Платёж уже обрабатывается")
        return
    lock_payment(pid)
    
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await msg.answer("Платёж не найден")
        return
    uid, amt, purpose, prd, qty, _ = pay
    if purpose == "deposit":
        update_balance(uid, amt)
        add_transaction(uid, "deposit", amt, "completed")
        update_payment_status(pid, "confirmed", datetime.now())
        await bot.send_message(uid, f"✅ Баланс пополнен на ${amt}!")
        await msg.answer(f"✅ Пополнение ${amt} для {uid} подтверждено.")
        log_admin_action(ADMIN_ID, "confirm_deposit", uid, f"${amt}")
    elif purpose == "direct_purchase":
        prod = PRODUCTS[prd]
        available = get_available_codes_count(prd)
        if available < qty:
            await msg.answer(f"Не хватает кодов ({available})")
            return
        codes = []
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT code FROM promo_codes WHERE product_id = ? AND used = 0 LIMIT ? FOR UPDATE", (prd, qty))
            rows = cur.fetchall()
            if len(rows) < qty:
                await msg.answer("Ошибка получения кодов")
                return
            codes = [r[0] for r in rows]
            cur.executemany("UPDATE promo_codes SET used = 1 WHERE code = ?", [(c,) for c in codes])
            cur.execute("UPDATE manual_payments SET status = 'confirmed', confirmed_at = ? WHERE payment_id = ?", (datetime.now().isoformat(), pid))
            cur.execute("INSERT INTO transactions (user_id, type, amount_usd, status, product_id, promo_code, created_at) VALUES (?, 'purchase', ?, 'completed', ?, ?, ?)",
                        (uid, amt, prd, ",".join(codes), datetime.now().isoformat()))
        msg_text = f"✅ Оплата получена!\n📦 {prod['name']}\n🔢 {qty} шт\n💲 ${amt}\n🎫 Промокоды:\n" + "\n".join(f"{i}. <code>{c}</code>" for i,c in enumerate(codes,1)) + "\n\n<i>Сохранены в «Мои покупки»</i>"
        await bot.send_message(uid, msg_text, parse_mode="HTML")
        await send_manual(uid, bot)
        await msg.answer(f"✅ Покупка {prod['name']} x{qty} для {uid} подтверждена.")
        log_admin_action(ADMIN_ID, "confirm_purchase", uid, f"{prod['name']} x{qty} ${amt}")

@dp.message(Command("decline"))
async def admin_decline_payment(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        return
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /decline <payment_id>")
        return
    pid = args[1]
    
    if is_payment_locked(pid):
        await msg.answer("⚠️ Платёж уже обрабатывается")
        return
    lock_payment(pid)
    
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await msg.answer("Платёж не найден")
        return
    uid, amt, _, _, _, _ = pay
    update_payment_status(pid, "declined", datetime.now())
    await bot.send_message(uid, f"❌ Платёж ${amt} отклонён.")
    await msg.answer(f"❌ Платёж {pid} отклонён.")
    log_admin_action(ADMIN_ID, "decline_payment", uid, f"${amt}")

# ---------- ОСНОВНЫЕ КНОПКИ МЕНЮ ----------
@dp.message(F.text == "🛍️ Товары")
async def show_products(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer("📦 Наши товары:\nВыберите нужный промокод 👇", reply_markup=products_kb())

@dp.message(F.text == "👤 Профиль")
async def profile(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer(f"👤 Профиль\nID: {msg.from_user.id}\n💰 Баланс: ${get_balance(msg.from_user.id):.2f}", parse_mode="HTML", reply_markup=main_menu())

@dp.message(F.text == "💰 Пополнить")
async def deposit_start(msg: Message, state: FSMContext):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await state.clear()
    await msg.answer("💸 Выберите сумму пополнения в USDT:", reply_markup=deposit_kb())

@dp.message(F.text == "📦 Мои покупки")
async def my_purchases(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    purchases = get_user_purchases(msg.from_user.id)
    if not purchases:
        await msg.answer("📭 Нет покупок.")
        return
    text = "📦 Ваши покупки:\n\n"
    for pid, codes, amt, dt in purchases:
        prod = PRODUCTS.get(pid, {"name": pid})
        text += f"🔹 {prod['name']}\n💰 ${amt:.2f}\n🎫 <code>{codes}</code>\n🕒 {dt[:16]}\n\n"
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())

@dp.message(F.text == "📊 Наличие")
async def availability(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    text = "📊 Доступные промокоды:\n\n"
    for pid, prod in PRODUCTS.items():
        cnt = get_available_codes_count(pid)
        text += f"🔹 {prod['name']} — {cnt} шт\n"
    await msg.answer(text, reply_markup=main_menu())

@dp.message(F.text == "📜 Правила")
async def rules(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer("ПРАВИЛА МАГАЗИНА FadeX\n\n• Возврат/замена промокода — 12 часов.\n• По всем вопросам — в поддержку.\n• Любые попытки мошенничества повлекут блокировку.\n• После каждой покупки выдаётся инструкция по активации.\n• Купоны не суммируются со скидками.\n\nСпасибо за честность!", reply_markup=main_menu())

@dp.message(F.text == "🆘 Поддержка")
async def support(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer(f"📩 Связь: {ADMIN_USERNAME}", reply_markup=main_menu())

@dp.message(F.text == "🌐 FadeX.cc")
async def fadex_link(msg: Message):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await msg.answer("https://fadex.cc", reply_markup=main_menu())

@dp.message(F.text == "💸 Вывод средств")
async def withdraw_start(msg: Message, state: FSMContext):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    
    bal = get_balance(msg.from_user.id)
    if bal < 10:
        await msg.answer(f"❌ Минимум 10 USD. Баланс: ${bal:.2f}")
        return
    await state.set_state(WithdrawStates.waiting_amount)
    await msg.answer(f"💰 Баланс: ${bal:.2f}\nВведите сумму вывода (мин 10, макс {bal:.2f}):")

@dp.message(F.text == "🎫 Купон")
async def coupon_button(msg: Message, state: FSMContext):
    if not check_rate_limit(msg.from_user.id):
        await msg.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    await state.set_state("waiting_coupon_code")
    await msg.answer("🎫 Введите код купона:\n\n(можно использовать /coupon <код>)")

@dp.message(WithdrawStates.waiting_amount, F.text.regex(r"^\d+(\.\d+)?$"))
async def withdraw_amount(msg: Message, state: FSMContext):
    amount = float(msg.text)
    bal = get_balance(msg.from_user.id)
    if amount < 10 or amount > bal:
        await msg.answer(f"❌ Сумма от 10 до {bal:.2f}")
        return
    await state.update_data(amount=amount)
    await state.set_state(WithdrawStates.waiting_wallet)
    await msg.answer("Введите адрес USDT (TON):\n\nПоддерживаются сети: TON (UQ/EQ), SOLANA (32-44 символа), XRP (начинается с r)")

@dp.message(WithdrawStates.waiting_wallet)
async def withdraw_wallet(msg: Message, state: FSMContext):
    wallet = msg.text.strip()
    
    # Проверяем все поддерживаемые сети
    if validate_ton_address(wallet):
        network = "TON"
    elif validate_sol_address(wallet):
        network = "SOLANA"
    elif validate_xrp_address(wallet):
        network = "XRP"
    else:
        await msg.answer("❌ Неверный адрес.\n\nПоддерживаются:\n• TON: UQ/EQ, 48 символов\n• SOLANA: 32-44 символа, base58\n• XRP: начинается с r, 25-34 символа")
        return
    
    data = await state.get_data()
    amount = data['amount']
    rid = create_withdrawal_request(msg.from_user.id, amount, wallet)
    update_balance(msg.from_user.id, -amount)
    add_transaction(msg.from_user.id, "withdraw", amount, "pending")
    await msg.answer(f"✅ Заявка #{rid} на ${amount} создана (сеть: {network}).", reply_markup=main_menu())
    await bot.send_message(ADMIN_ID, f"💸 Новая заявка #{rid}\n{msg.from_user.id}\n${amount}\n{network}\n{wallet}\n/process_withdraw {rid}")
    await state.clear()

# ---------- ОБРАБОТЧИКИ ПОПОЛНЕНИЯ ----------
@dp.callback_query(F.data.startswith("dep_"))
async def deposit_amount(cb: CallbackQuery, state: FSMContext):
    if cb.data == "dep_custom":
        return
    try:
        amount = float(cb.data.split("_")[1])
    except:
        await cb.answer("Ошибка", show_alert=True)
        return
    payment_id = create_manual_payment(cb.from_user.id, amount, "deposit")
    await cb.message.delete()
    await cb.message.answer(
        f"🧾 **Счёт на пополнение**\n\n"
        f"💵 Сумма: **{amount} USDT**\n"
        f"📤 Кошелёк для перевода:\n<code>{USDT_WALLET}</code>\n\n"
        f"📝 **Укажите ID в комментарии:**\n<code>pay_{payment_id}</code>\n\n"
        f"✅ После перевода нажмите кнопку «Я перевёл».",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я перевёл", callback_data=f"payment_done_{payment_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"payment_cancel_{payment_id}")]
        ])
    )
    await cb.answer()
    await state.clear()

@dp.callback_query(F.data == "dep_custom")
async def deposit_custom(cb: CallbackQuery, state: FSMContext):
    await state.set_state(DepositStates.waiting_custom)
    await cb.message.edit_text(
        "💸 Введите сумму в USD (мин 10):\n\n/otmena - отменить",
        reply_markup=back_menu_kb()
    )
    await cb.answer()

@dp.message(DepositStates.waiting_custom, F.text.regex(r"^\d+(\.\d+)?$"))
async def deposit_custom_amount(msg: Message, state: FSMContext):
    amount = float(msg.text.strip())
    if amount < 10:
        await msg.answer("❌ Минимальная сумма 10 USD. Попробуйте ещё раз:", reply_markup=back_menu_kb())
        return
    payment_id = create_manual_payment(msg.from_user.id, amount, "deposit")
    await msg.answer(
        f"🧾 **Счёт на пополнение**\n\n"
        f"💵 Сумма: **{amount} USDT**\n"
        f"📤 Кошелёк для перевода:\n<code>{USDT_WALLET}</code>\n\n"
        f"📝 **Укажите ID в комментарии:**\n<code>pay_{payment_id}</code>\n\n"
        f"✅ После перевода нажмите кнопку «Я перевёл».",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я перевёл", callback_data=f"payment_done_{payment_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"payment_cancel_{payment_id}")]
        ])
    )
    await state.clear()

@dp.message(DepositStates.waiting_custom)
async def deposit_custom_invalid(msg: Message, state: FSMContext):
    await msg.answer("❌ Введите число (например, 150).\nИли /otmena для отмены.", reply_markup=back_menu_kb())

@dp.message(Command("otmena"))
async def cancel_input(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("❌ Действие отменено.", reply_markup=main_menu())

# ---------- ОБРАБОТЧИКИ ПЛАТЕЖЕЙ ----------
@dp.callback_query(F.data.startswith("payment_done_"))
async def payment_done(cb: CallbackQuery):
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    uid, amt, pur, prd, qty, _ = pay
    text = f"💰 НОВЫЙ ПЛАТЁЖ!\n💳 ID: {pid}\n👤 <a href='tg://user?id={uid}'>{uid}</a>\n💵 ${amt}\n📌 {'Пополнение' if pur=='deposit' else 'Покупка'}"
    if pur == "direct_purchase":
        text += f"\n📦 {PRODUCTS.get(prd,{}).get('name',prd)} x{qty}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_{pid}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_decline_{pid}")]
    ])
    await bot.send_message(ADMIN_ID, text, parse_mode="HTML", reply_markup=kb)
    await cb.message.edit_text("✅ Сообщение отправлено администратору. Ожидайте.", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(F.data.startswith("payment_cancel_"))
async def payment_cancel(cb: CallbackQuery):
    pid = cb.data.split("_")[2]
    pay = get_payment(pid)
    if pay and pay[5] == 'pending':
        update_payment_status(pid, "cancelled")
    await cb.message.edit_text("❌ Платёж отменён.", reply_markup=back_menu_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_confirm_"))
async def admin_confirm_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет прав")
        return
    pid = cb.data.split("_")[2]
    
    if is_payment_locked(pid):
        await cb.answer("⚠️ Платёж уже обрабатывается")
        return
    lock_payment(pid)
    
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    uid, amt, pur, prd, qty, _ = pay
    if pur == "deposit":
        update_balance(uid, amt)
        add_transaction(uid, "deposit", amt, "completed")
        update_payment_status(pid, "confirmed", datetime.now())
        await bot.send_message(uid, f"✅ Баланс пополнен на ${amt}!")
        await safe_edit_message(cb.message, f"✅ Пополнение ${amt} для {uid} подтверждено.")
        await bot.send_message(ADMIN_ID, f"💰 Пополнение: {uid} на ${amt}")
    elif pur == "direct_purchase":
        prod = PRODUCTS[prd]
        available = get_available_codes_count(prd)
        if available < qty:
            await cb.answer(f"Не хватает кодов ({available})")
            return
        codes = []
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT code FROM promo_codes WHERE product_id = ? AND used = 0 LIMIT ? FOR UPDATE", (prd, qty))
            rows = cur.fetchall()
            if len(rows) < qty:
                await cb.answer("Ошибка получения кодов")
                return
            codes = [r[0] for r in rows]
            cur.executemany("UPDATE promo_codes SET used = 1 WHERE code = ?", [(c,) for c in codes])
            cur.execute("UPDATE manual_payments SET status = 'confirmed', confirmed_at = ? WHERE payment_id = ?", (datetime.now().isoformat(), pid))
            cur.execute("INSERT INTO transactions (user_id, type, amount_usd, status, product_id, promo_code, created_at) VALUES (?, 'purchase', ?, 'completed', ?, ?, ?)",
                        (uid, amt, prd, ",".join(codes), datetime.now().isoformat()))
        msg_text = f"✅ Оплата получена!\n📦 {prod['name']}\n🔢 {qty} шт\n💲 ${amt}\n🎫 Промокоды:\n" + "\n".join(f"{i}. <code>{c}</code>" for i,c in enumerate(codes,1)) + "\n\n<i>Сохранены в «Мои покупки»</i>"
        await bot.send_message(uid, msg_text, parse_mode="HTML")
        await send_manual(uid, bot)
        await safe_edit_message(cb.message, f"✅ Покупка {prod['name']} x{qty} для {uid} подтверждена.")
        await bot.send_message(ADMIN_ID, f"🛒 Покупка: {uid} купил {prod['name']} x{qty} за ${amt}")
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_decline_"))
async def admin_decline_callback(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await cb.answer("Нет прав")
        return
    pid = cb.data.split("_")[2]
    
    if is_payment_locked(pid):
        await cb.answer("⚠️ Платёж уже обрабатывается")
        return
    lock_payment(pid)
    
    pay = get_payment(pid)
    if not pay or pay[5] != 'pending':
        await cb.answer("Платёж уже обработан")
        return
    uid, amt, _, _, _, _ = pay
    update_payment_status(pid, "declined", datetime.now())
    await bot.send_message(uid, f"❌ Платёж ${amt} отклонён.")
    await safe_edit_message(cb.message, f"❌ Платёж {pid} отклонён.")
    await bot.send_message(ADMIN_ID, f"❌ Отклонён платёж: {uid} ${amt}")
    await cb.answer()

# ---------- ВЫБОР ТОВАРА И ПОКУПКА С КУПОНАМИ ----------
@dp.callback_query(F.data.startswith("coupon_"))
async def apply_coupon_to_product(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[1]
    await state.update_data(pid=pid, waiting_coupon=True)
    await cb.message.answer("🎫 Введите код купона для применения скидки:")
    await cb.answer()

@dp.message(F.text, lambda msg: getattr(msg, 'state', None) and 'waiting_coupon' in str(msg.state))
async def process_coupon_for_product(msg: Message, state: FSMContext):
    coupon_code = msg.text.strip().upper()
    data = await state.get_data()
    pid = data.get('pid')
    
    try:
        # Проверяем купон без применения
        conn = sqlite3.connect("fadex_bot.db")
        cur = conn.cursor()
        cur.execute("SELECT discount_percent, valid_until, used, user_id FROM coupons WHERE id = ?", (coupon_code,))
        row = cur.fetchone()
        conn.close()
        
        if not row:
            await msg.answer("❌ Купон не найден")
            return
        
        discount, valid_until, used, coupon_user_id = row
        
        if used:
            await msg.answer("❌ Купон уже использован")
            return
        
        if datetime.fromisoformat(valid_until) < datetime.now():
            await msg.answer("❌ Купон просрочен")
            return
        
        if coupon_user_id and coupon_user_id != msg.from_user.id:
            await msg.answer("❌ Купон не принадлежит вам")
            return
        
        await state.update_data(coupon_code=coupon_code, coupon_discount=discount)
        prod = PRODUCTS[pid]
        price_desc = f"~~${prod['old_price']}~~ -{prod['discount']}% → ${prod['price_usd']}" if prod.get("old_price") else f"${prod['price_usd']}"
        await msg.answer(f"✅ Купон на {discount}% применён!\n\n🔹 {prod['name']}\n💰 {price_desc}\n📦 {prod['desc']}\n\n🛒 Способ оплаты со скидкой:", reply_markup=purchase_kb(pid))
        await state.clear()
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {str(e)}")

@dp.callback_query(F.data.startswith("prod_"))
async def product_chosen(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[1]
    prod = PRODUCTS[pid]
    await state.update_data(pid=pid, price=prod["price_usd"])
    price_desc = f"~~${prod['old_price']}~~ -{prod['discount']}% → ${prod['price_usd']}" if prod.get("old_price") else f"${prod['price_usd']}"
    await safe_edit_message(cb.message, f"🔹 {prod['name']}\n💰 {price_desc}\n📦 {prod['desc']}\n\n🎫 Есть купон? Нажмите «Применить купон»\n\n🛒 Способ оплаты:", reply_markup=purchase_kb(pid))
    await cb.answer()

@dp.callback_query(F.data.startswith("balance_buy_"))
async def balance_buy_start(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    prod = PRODUCTS[pid]
    data = await state.get_data()
    coupon_discount = data.get('coupon_discount', 0)
    coupon_code = data.get('coupon_code')
    
    price = prod["price_usd"]
    if coupon_discount > 0:
        price = price * (100 - coupon_discount) / 100
        await state.update_data(coupon_discount=coupon_discount, coupon_code=coupon_code, discounted_price=price)
    
    await state.update_data(pid=pid, price=price, original_price=prod["price_usd"], quantity=1)
    await state.set_state(BalanceBuy.quantity)
    
    price_text = f"{price}$" if coupon_discount == 0 else f"~~{prod['price_usd']}$~~ → {price}$ (скидка {coupon_discount}%)"
    await safe_edit_message(cb.message, f"🛒 {prod['name']}\n💰 {price_text} за шт\nВыберите количество:", reply_markup=quantity_kb(pid, price))
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
        await cb.answer(f"❌ Не хватает ${total:.2f}")
        return
    if get_available_codes_count(pid) < qty:
        await cb.answer(f"❌ Осталось {get_available_codes_count(pid)} шт")
        return
    await state.update_data(pid=pid, qty=qty, total=total)
    await state.set_state(BalanceBuy.confirm)
    
    coupon_text = ""
    if data.get('coupon_discount'):
        coupon_text = f"\n🎫 Скидка по купону: {data['coupon_discount']}%"
    
    await safe_edit_message(cb.message, f"Подтвердите покупку\n{PRODUCTS[pid]['name']} x{qty} = ${total:.2f}{coupon_text}\nБаланс: ${bal:.2f}\n\nПодтверждаете?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Да", callback_data="balance_confirm_go")],[InlineKeyboardButton(text="❌ Нет", callback_data="back_products")]]))
    await cb.answer()

@dp.callback_query(F.data == "balance_confirm_go")
async def balance_do_purchase(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pid = data.get("pid")
    qty = data.get("qty")
    total = data.get("total")
    coupon_code = data.get('coupon_code')
    
    if not pid:
        await cb.answer("Ошибка")
        return
    
    try:
        codes, total = atomic_purchase(cb.from_user.id, pid, qty)
        
        # Если был купон, отмечаем его использованным
        if coupon_code:
            mark_coupon_used(coupon_code)
        
        prod = PRODUCTS[pid]
        coupon_text = ""
        if data.get('coupon_discount'):
            coupon_text = f"\n🎫 Скидка по купону: {data['coupon_discount']}%"
        
        msg_text = f"✅ Покупка успешна!\n📦 {prod['name']}\n🔢 {qty} шт\n💰 Списано: ${total:.2f}{coupon_text}\n🎫 Промокоды:\n" + "\n".join(f"{i}. <code>{c}</code>" for i,c in enumerate(codes,1)) + "\n\n<i>Сохранены в «Мои покупки»</i>"
        await safe_edit_message(cb.message, msg_text, parse_mode="HTML", reply_markup=back_menu_kb())
        await send_manual(cb.from_user.id, bot)
        await bot.send_message(ADMIN_ID, f"🛒 Покупка с баланса: {cb.from_user.id} купил {prod['name']} x{qty} за ${total:.2f}")
        if coupon_code:
            await bot.send_message(ADMIN_ID, f"🎫 Использован купон {coupon_code} (скидка {data['coupon_discount']}%)")
    except ValueError as e:
        await safe_edit_message(cb.message, f"❌ {str(e)}", reply_markup=back_menu_kb())
    await state.clear()
    await cb.answer()

@dp.callback_query(F.data.startswith("direct_pay_"))
async def direct_pay_start(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[2]
    prod = PRODUCTS[pid]
    data = await state.get_data()
    coupon_discount = data.get('coupon_discount', 0)
    coupon_code = data.get('coupon_code')
    
    price = prod["price_usd"]
    if coupon_discount > 0:
        price = price * (100 - coupon_discount) / 100
        await state.update_data(coupon_discount=coupon_discount, coupon_code=coupon_code, discounted_price=price)
    
    await state.update_data(pid=pid, price=price, original_price=prod["price_usd"], quantity=1)
    await state.set_state(DirectPay.quantity)
    
    price_text = f"{price}$" if coupon_discount == 0 else f"~~{prod['price_usd']}$~~ → {price}$ (скидка {coupon_discount}%)"
    await safe_edit_message(cb.message, f"🛒 {prod['name']}\n💰 {price_text} за шт\nВыберите количество:", reply_markup=quantity_kb(pid, price))
    await cb.answer()

@dp.callback_query(DirectPay.quantity, F.data.startswith("qty_"))
async def direct_select_qty(cb: CallbackQuery, state: FSMContext):
    _, pid, qty = cb.data.split("_")
    qty = int(qty)
    data = await state.get_data()
    price = data.get('price', PRODUCTS[pid]['price_usd'])
    total = price * qty
    original_total = PRODUCTS[pid]['price_usd'] * qty
    
    coupon_text = ""
    if data.get('coupon_discount'):
        coupon_text = f"\n🎫 Скидка по купону: {data['coupon_discount']}% (было ${original_total:.2f})"
    
    pid2 = create_manual_payment(cb.from_user.id, total, "direct_purchase", pid, qty)
    
    # Сохраняем информацию о купоне для платежа
    if data.get('coupon_code'):
        conn = sqlite3.connect("fadex_bot.db")
        cur = conn.cursor()
        cur.execute("UPDATE manual_payments SET purpose = ? WHERE payment_id = ?", 
                    (f"direct_purchase_coupon_{data['coupon_code']}", pid2))
        conn.commit()
        conn.close()
    
    await cb.message.delete()
    await cb.message.answer(
        f"🧾 Заказ на прямую оплату\n{PRODUCTS[pid]['name']} x{qty} = ${total:.2f} USDT{coupon_text}\n\nКошелёк: <code>{USDT_WALLET}</code>\nУкажите ID: <code>pay_{pid2}</code>\nПосле перевода нажмите «✅ Я перевёл».",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я перевёл", callback_data=f"payment_done_{pid2}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"payment_cancel_{pid2}")]
        ])
    )
    await cb.answer()
    await state.clear()

# ---------- НАВИГАЦИЯ ----------
@dp.callback_query(F.data == "menu")
async def to_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    await cb.message.answer("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(F.data == "back_products")
async def back_to_products(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_message(cb.message, "📦 Наши товары:", reply_markup=products_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("back_purchase_"))
async def back_to_purchase(cb: CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[-1]
    prod = PRODUCTS[pid]
    price_desc = f"~~${prod['old_price']}~~ -{prod['discount']}% → ${prod['price_usd']}" if prod.get("old_price") else f"${prod['price_usd']}"
    await safe_edit_message(cb.message, f"🔹 {prod['name']}\n💰 {price_desc}\n📦 {prod['desc']}\n\n🛒 Способ оплаты:", reply_markup=purchase_kb(pid))
    await cb.answer()

@dp.message(Command("id"))
async def id_cmd(msg: Message):
    await msg.answer(f"🆔 Ваш ID: <code>{msg.from_user.id}</code>", parse_mode="HTML")

# ========== АДМИН-КОМАНДЫ ДЛЯ РЕДАКТИРОВАНИЯ ТОВАРОВ ==========
@dp.message(Command("edit_price"))
async def edit_price(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    args = msg.text.split()
    if len(args) != 3:
        await msg.answer("Использование: /edit_price <id_товара> <новая_цена>")
        return
    pid = args[1]
    if pid not in PRODUCTS:
        await msg.answer(f"Неверный ID. Доступные: {', '.join(PRODUCTS.keys())}")
        return
    try:
        new_price = float(args[2])
        if new_price <= 0:
            raise ValueError
    except:
        await msg.answer("Цена должна быть положительным числом.")
        return
    old_price = PRODUCTS[pid]['price_usd']
    PRODUCTS[pid]['price_usd'] = new_price
    if PRODUCTS[pid].get('old_price'):
        old = PRODUCTS[pid]['old_price']
        discount = round((old - new_price) / old * 100)
        PRODUCTS[pid]['discount'] = discount
    save_products_to_db()
    log_admin_action(ADMIN_ID, "edit_price", 0, f"{pid}: ${old_price} → ${new_price}")
    await msg.answer(f"✅ Цена для {PRODUCTS[pid]['name']} изменена: ${new_price}. Скидка пересчитана.")

@dp.message(Command("edit_oldprice"))
async def edit_oldprice(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    args = msg.text.split()
    if len(args) != 3:
        await msg.answer("Использование: /edit_oldprice <id_товара> <старая_цена> (0 - убрать скидку)")
        return
    pid = args[1]
    if pid not in PRODUCTS:
        await msg.answer(f"Неверный ID. Доступные: {', '.join(PRODUCTS.keys())}")
        return
    try:
        old_price = float(args[2]) if float(args[2]) > 0 else None
    except:
        old_price = None
    PRODUCTS[pid]['old_price'] = old_price
    if old_price:
        discount = round((old_price - PRODUCTS[pid]['price_usd']) / old_price * 100)
        PRODUCTS[pid]['discount'] = discount
    else:
        PRODUCTS[pid]['discount'] = None
    save_products_to_db()
    log_admin_action(ADMIN_ID, "edit_oldprice", 0, f"{pid}: old_price={old_price}")
    await msg.answer(f"✅ Старая цена для {PRODUCTS[pid]['name']} установлена: {old_price if old_price else 'без скидки'}")

@dp.message(Command("edit_desc"))
async def edit_desc(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    args = msg.text.split(maxsplit=2)
    if len(args) != 3:
        await msg.answer("Использование: /edit_desc <id_товара> <новое_описание>")
        return
    pid = args[1]
    if pid not in PRODUCTS:
        await msg.answer(f"Неверный ID. Доступные: {', '.join(PRODUCTS.keys())}")
        return
    new_desc = args[2]
    PRODUCTS[pid]['desc'] = new_desc
    save_products_to_db()
    log_admin_action(ADMIN_ID, "edit_desc", 0, f"{pid}: {new_desc}")
    await msg.answer(f"✅ Описание для {PRODUCTS[pid]['name']} изменено: {new_desc}")

@dp.message(Command("stock"))
async def show_stock(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    args = msg.text.split()
    if len(args) != 2:
        await msg.answer("Использование: /stock <id_товара>")
        return
    pid = args[1]
    if pid not in PRODUCTS:
        await msg.answer(f"Неверный ID. Доступные: {', '.join(PRODUCTS.keys())}")
        return
    cnt = get_available_codes_count(pid)
    await msg.answer(f"📦 Остаток промокодов для {PRODUCTS[pid]['name']}: {cnt} шт.")

@dp.message(Command("list_products"))
async def list_products(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    text = "📋 Список товаров и их ID:\n\n"
    for pid, prod in PRODUCTS.items():
        text += f"🔹 `{pid}` – {prod['name']}\n   Цена: ${prod['price_usd']}\n"
        if prod.get('old_price'):
            text += f"   Скидка: -{prod['discount']}% (было ${prod['old_price']})\n"
        text += f"   Описание: {prod['desc']}\n"
        cnt = get_available_codes_count(pid)
        text += f"   Кодов в наличии: {cnt}\n\n"
    await msg.answer(text, parse_mode="Markdown")

@dp.message(Command("create_coupon"))
async def create_coupon_cmd(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("Использование: /create_coupon <скидка%> [user_id] [дней]\n\nПример: /create_coupon 25 123456789 30")
        return
    
    try:
        discount = int(args[1])
        if discount < 1 or discount > 90:
            await msg.answer("❌ Скидка должна быть от 1 до 90%")
            return
        
        user_id = int(args[2]) if len(args) > 2 else None
        days = int(args[3]) if len(args) > 3 else 30
        
        coupon_id = create_coupon(discount, user_id, days)
        await msg.answer(f"✅ Купон создан!\n\nКод: `{coupon_id}`\nСкидка: {discount}%\nДействителен: {days} дней\nПользователь: {user_id if user_id else 'для всех'}", parse_mode="Markdown")
        log_admin_action(ADMIN_ID, "create_coupon", user_id or 0, f"скидка {discount}%")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("broadcast"))
async def broadcast_cmd(msg: Message):
    if msg.from_user.id != ADMIN_ID or not is_admin_logged_in(ADMIN_ID):
        await msg.answer("⛔ Нет прав.")
        return
    
    text = msg.text.replace("/broadcast", "", 1).strip()
    if not text:
        await msg.answer("📢 Использование: /broadcast <текст для рассылки>\n\nМожно использовать HTML разметку")
        return
    
    users = get_all_users()
    success = 0
    fail = 0
    
    status_msg = await msg.answer(f"📢 Начинаю рассылку для {len(users)} пользователей...")
    
    for user in users:
        try:
            await bot.send_message(user[0], f"📢 МАССОВАЯ РАССЫЛКА\n\n{text}", parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            fail += 1
            logger.error(f"Ошибка отправки {user[0]}: {e}")
        
        # Обновляем статус каждые 100 сообщений
        if (success + fail) % 100 == 0:
            await status_msg.edit_text(f"📢 Рассылка в процессе...\n✅ Отправлено: {success}\n❌ Ошибок: {fail}")
    
    await status_msg.edit_text(f"✅ Рассылка завершена!\n📨 Отправлено: {success}\n❌ Ошибок: {fail}")
    log_admin_action(ADMIN_ID, "broadcast", 0, f"Отправлено {success} из {len(users)}")

# ========== ЛОГИРОВАНИЕ ВСЕХ СООБЩЕНИЙ ==========
@dp.message()
async def log_all_messages(msg: Message):
    """Логирование всех действий пользователей для аудита"""
    if msg.text and not msg.text.startswith('/') and msg.text not in ["🛍️ Товары", "👤 Профиль", "💰 Пополнить", "📦 Мои покупки", "📜 Правила", "🆘 Поддержка", "🌐 FadeX.cc", "📊 Наличие", "💸 Вывод средств", "🎫 Купон"]:
        logger.info(f"USER_ACTION: {msg.from_user.id} (@{msg.from_user.username}) -> {msg.text[:100]}")

# ========== ЗАПУСК ==========
async def main():
    init_db()
    load_products_from_db()  # Загружаем сохранённые цены
    seed_promo_codes()
    
    # Запускаем мониторинги
    if TON_API_KEY and USDT_WALLET and USDT_WALLET.startswith(('UQ', 'EQ')):
        asyncio.create_task(ton_payment_monitor())
        asyncio.create_task(check_ton_api_health())
        print("✅ Автоматическая проверка TON платежей включена")
    else:
        print("⚠️ Автоматическая проверка платежей отключена (TRC20 кошелек). Платежи будут подтверждаться вручную.")
    
    asyncio.create_task(cleanup_task())
    
    print("✅ Бот запущен (режим polling)")
    print(f"👤 Админ: {ADMIN_ID} {ADMIN_USERNAME}")
    print(f"💰 Кошелёк USDT (TRC20): {USDT_WALLET}")
    print("🎫 Система купонов активна")
    print("🛡️ Защита от двойной траты активна")
    print("📊 Rate limiting активен (30 команд/мин)")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
