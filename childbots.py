"""Mijoz (child) botlar.

Foydalanuvchi @BotFather'dan o'z botini yaratib, tokenini asosiy botga yuboradi. Shu jarayonning ichida
(bitta Dispatcher, alohida long-polling) ayni konkurs/battle funksiyalari bilan ishlaydigan nusxa bot ishga tushadi.

- Child botda faqat: Battle/Konkurs yaratish, "Mening battlelarim/konkurslarim", qo'llanma, egasi uchun statistika.
  Ball, do'kon, promokod, majburiy kanallar va admin panel faqat asosiy botda.
- Battle/konkurs yozuvlari `bot_id` bilan saqlanadi: post, tugatish va tugmalar o'sha bot orqali ishlaydi.
- Premium emoji child botlarda ishlatilmaydi (ID'lar asosiy botga tegishli): emojis.PLAIN yoqiladi.
"""
import asyncio
import logging
import os
import re
import time

from aiogram import Bot, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramConflictError, TelegramRetryAfter, TelegramUnauthorizedError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import games
from emojis import PLAIN
from games import esc, ex, ib, ibi, kb, now_ts, q1, qa

log = logging.getLogger("childbots")
router = Router()

CHILD_LIMIT = int(os.getenv("CHILD_LIMIT", "3"))      # bitta foydalanuvchi ochishi mumkin bo'lgan botlar soni (adminlar uchun cheksiz)
CHILD_MAX = int(os.getenv("CHILD_MAX", "100"))        # jami child botlar soni (server xotirasi uchun)
TOKEN_RE = re.compile(r"\d{6,12}:[A-Za-z0-9_-]{30,}")

games.db.executescript("""
CREATE TABLE IF NOT EXISTS child_bots(
  bot_id INTEGER PRIMARY KEY, token TEXT, username TEXT, name TEXT, owner INTEGER,
  active INTEGER DEFAULT 1, created INTEGER);
CREATE TABLE IF NOT EXISTS child_users(
  bot_id INTEGER, user_id INTEGER, ts INTEGER, PRIMARY KEY(bot_id, user_id));
""")
games.db.commit()

_dp = None
_allowed = None
_polls = {}            # bot_id -> polling task
_feeds = set()


# ====================== YORDAMCHILAR (bot.py ishlatadi) ======================
def owner_of(bot_id):
    r = q1("SELECT owner FROM child_bots WHERE bot_id=?", bot_id)
    return r["owner"] if r else None


def track_user(bot_id, uid):
    ex("INSERT OR IGNORE INTO child_users(bot_id,user_id,ts) VALUES(?,?,?)", bot_id, uid, now_ts())


def stats_text(bot_id) -> str:
    r = q1("SELECT * FROM child_bots WHERE bot_id=?", bot_id)
    if not r:
        return "Topilmadi."
    one = lambda sql, *a: q1(sql, *a)["c"]
    users = one("SELECT COUNT(*) c FROM child_users WHERE bot_id=?", bot_id)
    nb = one("SELECT COUNT(*) c FROM battles WHERE bot_id=?", bot_id)
    nba = one("SELECT COUNT(*) c FROM battles WHERE bot_id=? AND status='active'", bot_id)
    nc = one("SELECT COUNT(*) c FROM contests WHERE bot_id=?", bot_id)
    nca = one("SELECT COUNT(*) c FROM contests WHERE bot_id=? AND status='active'", bot_id)
    return (f"📊 <b>@{esc(r['username'])} statistikasi</b>\n\n👥 Foydalanuvchilar: <b>{users}</b>\n"
            f"⚔️ Battlelar: <b>{nb}</b> (faol: {nba})\n🏆 Konkurslar: <b>{nc}</b> (faol: {nca})\n"
            f"📅 Ochilgan: {games.fmt_ts(r['created'])}")


# ====================== ISHGA TUSHIRISH / TO'XTATISH ======================
def attach(dp):
    """Hamma router'lar ulangandan keyin chaqiriladi."""
    global _dp, _allowed
    _dp = dp
    _allowed = dp.resolve_used_update_types()


async def _feed(bot: Bot, upd):
    PLAIN.set(True)                       # bu task'ning o'z konteksti bor, boshqa so'rovlarga ta'sir qilmaydi
    try:
        await _dp.feed_update(bot, upd)
    except Exception:
        log.exception("child bot %s: update xatosi", bot.id)


async def _revoked(bot: Bot):
    """Token @BotFather'da bekor qilingan/o'zgartirilgan: botni o'chirib, egasiga xabar beramiz."""
    ex("UPDATE child_bots SET active=0 WHERE bot_id=?", bot.id)
    games.BOTS.pop(bot.id, None)
    _polls.pop(bot.id, None)
    r = q1("SELECT owner, username FROM child_bots WHERE bot_id=?", bot.id)
    main = games.BOTS.get(games.MAIN_ID)
    if r and main:
        try:
            await main.send_message(r["owner"], f"⚠️ @{esc(r['username'])} tokeni yaroqsiz bo'lib qoldi (BotFather'da o'zgartirilgan "
                                                "yoki bot o'chirilgan). Bot to'xtatildi. Yangi token bilan qayta ulang.")
        except Exception:
            pass
    try:
        await bot.session.close()
    except Exception:
        pass


async def _poll(bot: Bot):
    try:
        await bot.delete_webhook(drop_pending_updates=False)    # webhook qolib ketgan bo'lsa, polling ishlamaydi
    except TelegramUnauthorizedError:
        return await _revoked(bot)
    except Exception as e:
        log.warning("child %s delete_webhook: %s", bot.id, e)
    offset, delay = None, 1
    log.info("child bot ishga tushdi: %s", bot.id)
    while True:
        try:
            updates = await bot.get_updates(offset=offset, timeout=30, allowed_updates=_allowed, request_timeout=60)
            delay = 1
            for u in updates:
                offset = u.update_id + 1
                t = asyncio.create_task(_feed(bot, u))
                _feeds.add(t)
                t.add_done_callback(_feeds.discard)
        except asyncio.CancelledError:
            raise
        except TelegramUnauthorizedError:
            return await _revoked(bot)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except TelegramConflictError:
            log.warning("child %s: boshqa joyda ham polling/webhook ishlayapti", bot.id)
            await asyncio.sleep(10)
        except Exception as e:
            log.warning("child %s polling: %s", bot.id, e)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


def _new_bot(token: str) -> Bot:
    return Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))


def start(bot_id: int, token: str, bot: Bot = None) -> Bot:
    if bot_id in _polls:
        return games.BOTS[bot_id]
    bot = bot or _new_bot(token)
    games.BOTS[bot_id] = bot
    _polls[bot_id] = asyncio.create_task(_poll(bot))
    return bot


async def stop(bot_id: int):
    if bot_id == games.MAIN_ID:
        return
    t = _polls.pop(bot_id, None)
    if t:
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    bot = games.BOTS.pop(bot_id, None)
    if bot:
        try:
            await bot.session.close()
        except Exception:
            pass


def load_all():
    """Ishga tushganda barcha faol child botlarni qayta yoqadi."""
    n = 0
    for r in qa("SELECT bot_id, token FROM child_bots WHERE active=1"):
        start(r["bot_id"], r["token"])
        n += 1
    if n:
        log.info("%d ta child bot yoqildi", n)


async def shutdown():
    for bid in list(_polls):
        await stop(bid)


# ====================== ASOSIY BOTDA: BOT OCHISH ======================
class CB(StatesGroup):
    token = State()


INTRO = ("🤖 <b>O'z konkurs botingizni oching</b>\n\n"
         "Bu botdagi <b>Battle</b> va <b>Konkurs</b> funksiyalari bilan ishlaydigan o'zingizning botingiz bo'ladi: "
         "kanalingizda ovozli battle va konkurslar o'tkazasiz, postlarda <b>sizning bot nomingiz</b> chiqadi.\n\n"
         "<b>Qanday ochiladi:</b>\n"
         "1. @BotFather → /newbot → bot nomi va username bering\n"
         "2. BotFather bergan <b>tokenni</b> shu yerga yuboring\n"
         "3. Botni kanalingizga admin qiling va kanalga <code>#battil</code> deb yozing\n")


def _list_view(uid, admin_all=False):
    rows = qa("SELECT * FROM child_bots ORDER BY created DESC LIMIT 40") if admin_all else \
        qa("SELECT * FROM child_bots WHERE owner=? ORDER BY created DESC", uid)
    btns = [[ib(f"{'🟢' if r['active'] else '⛔'} @{r['username']}" + (f" · {r['owner']}" if admin_all else ""),
                cb=f"cbm:{r['bot_id']}")] for r in rows]
    if not admin_all:
        btns.append([ibi("add", "Yangi bot qo'shish", cb="cba", style="success")])
    return rows, kb(btns)


async def open_menu(m: Message, state: FSMContext):
    """«🤖 O'z botimni ochish» tugmasi."""
    rows, markup = _list_view(m.from_user.id)
    text = INTRO + (f"\n📋 <b>Sizning botlaringiz:</b> {len(rows)} ta" if rows else "")
    await m.answer(text, reply_markup=markup)


async def admin_list(m: Message):
    rows, markup = _list_view(m.from_user.id, admin_all=True)
    if not rows:
        return await m.answer("🤖 Hozircha mijoz botlar yo'q.")
    await m.answer(f"🤖 <b>Mijoz botlar:</b> {len(rows)} ta (🟢 faol · ⛔ to'xtagan · yonida egasining ID'si)", reply_markup=markup)


@router.callback_query(F.data == "cba")
async def add_cb(c: CallbackQuery, state: FSMContext):
    me = c.from_user.id
    n = q1("SELECT COUNT(*) c FROM child_bots WHERE owner=?", me)["c"]
    if n >= CHILD_LIMIT and me not in games.ADMIN_IDS:
        return await c.answer(f"Siz {CHILD_LIMIT} tagacha bot ocha olasiz.", show_alert=True)
    await c.answer()
    await state.set_state(CB.token)
    await c.message.answer("🔑 @BotFather bergan <b>bot tokenini</b> yuboring.\n"
                           "<i>Token xavfsizlik uchun xabaringizdan o'chiriladi. Tokenni hech kimga bermang.</i>",
                           reply_markup=games.CANCEL_KB)


@router.message(CB.token, F.text)
async def got_token(m: Message, state: FSMContext):
    tok = m.text.strip()
    try:
        await m.delete()                  # token chatda qolib ketmasin
    except Exception:
        pass
    if not TOKEN_RE.fullmatch(tok):
        return await m.answer("❌ Bu token emas. U <code>123456789:AAH...</code> ko'rinishida bo'ladi.\n"
                              "Qayta yuboring yoki /cancel.")
    me = m.from_user.id
    bot_id = int(tok.split(":")[0])
    if bot_id == games.MAIN_ID:
        return await m.answer("❌ Bu asosiy botning tokeni. O'zingiz yaratgan boshqa botning tokenini yuboring.")
    old = q1("SELECT * FROM child_bots WHERE bot_id=?", bot_id)
    if old and old["owner"] != me:
        return await m.answer("❌ Bu bot boshqa foydalanuvchiga ulangan.")
    if old and old["active"] and old["token"] == tok and bot_id in _polls:
        await state.clear()
        return await m.answer(f"✅ @{esc(old['username'])} allaqachon ulangan.")
    if not old:
        if q1("SELECT COUNT(*) c FROM child_bots WHERE owner=?", me)["c"] >= CHILD_LIMIT and me not in games.ADMIN_IDS:
            await state.clear()
            return await m.answer(f"❌ Siz {CHILD_LIMIT} tagacha bot ocha olasiz.")
        if q1("SELECT COUNT(*) c FROM child_bots WHERE active=1")["c"] >= CHILD_MAX:
            await state.clear()
            return await m.answer("❌ Hozircha yangi botlar qabul qilinmayapti. Keyinroq urinib ko'ring.")
    tb = _new_bot(tok)
    try:
        info = await tb.get_me()
    except TelegramUnauthorizedError:
        await tb.session.close()
        return await m.answer("❌ Token yaroqsiz. @BotFather'dan to'g'ri tokenni oling va qayta yuboring.")
    except Exception as e:
        await tb.session.close()
        log.warning("get_me: %s", e)
        return await m.answer("❌ Tokenni tekshirib bo'lmadi. Birozdan so'ng qayta urinib ko'ring.")
    if old and bot_id in _polls:
        await stop(bot_id)               # token almashtirilgan: eski polling to'xtatiladi
    ex("""INSERT INTO child_bots(bot_id,token,username,name,owner,active,created) VALUES(?,?,?,?,?,1,?)
          ON CONFLICT(bot_id) DO UPDATE SET token=excluded.token, username=excluded.username,
          name=excluded.name, active=1""", bot_id, tok, info.username, info.full_name, me, now_ts())
    start(bot_id, tok, tb)
    await state.clear()
    await m.answer(f"✅ <b>Botingiz tayyor!</b>\n\n🤖 @{esc(info.username)}\n\n"
                   "<b>Endi:</b>\n"
                   f"1. @{esc(info.username)} ga kirib /start bosing — u yerda Battle va Konkurs yaratish menyusi chiqadi\n"
                   "2. Botni kanalingizga <b>admin</b> qiling (xabar yozish, tahrirlash va foydalanuvchilarni boshqarish huquqi bilan)\n"
                   "3. Kanalga <code>#battil</code> deb yozing — battle posti chiqadi, <code>#tuga</code> — tugatadi\n\n"
                   "Ovoz berish uchun qurilma tasdiqlash va multi-akkaunt himoyasi ham ishlaydi.")


def _may_manage(c: CallbackQuery, r) -> bool:
    return bool(r) and (r["owner"] == c.from_user.id or c.from_user.id in games.ADMIN_IDS)


@router.callback_query(F.data.startswith("cbm:"))
async def bot_detail(c: CallbackQuery):
    r = q1("SELECT * FROM child_bots WHERE bot_id=?", int(c.data[4:]))
    if not _may_manage(c, r):
        return await c.answer("Topilmadi", show_alert=True)
    await c.answer()
    rows = [[ibi("del", "Botni o'chirish", cb=f"cbd:{r['bot_id']}", style="danger")],
            [ibi("w_back", "Orqaga", cb="cbl")]]
    state = "🟢 Ishlayapti" if r["active"] and r["bot_id"] in _polls else "⛔ To'xtagan (token yaroqsiz bo'lishi mumkin)"
    await c.message.edit_text(stats_text(r["bot_id"]) + f"\n📌 Holati: {state}", reply_markup=kb(rows))


@router.callback_query(F.data == "cbl")
async def back_list(c: CallbackQuery):
    rows, markup = _list_view(c.from_user.id, admin_all=c.from_user.id in games.ADMIN_IDS and not
                              q1("SELECT 1 FROM child_bots WHERE owner=?", c.from_user.id))
    await c.answer()
    await c.message.edit_text(INTRO + (f"\n📋 <b>Botlar:</b> {len(rows)} ta" if rows else ""), reply_markup=markup)


@router.callback_query(F.data.startswith("cbd:"))
async def bot_del_ask(c: CallbackQuery):
    r = q1("SELECT * FROM child_bots WHERE bot_id=?", int(c.data[4:]))
    if not _may_manage(c, r):
        return await c.answer("Topilmadi", show_alert=True)
    await c.answer()
    await c.message.edit_text(f"🗑 <b>@{esc(r['username'])}</b> o'chirilsinmi?\n\n"
                              "Bot to'xtaydi, token bazadan o'chadi. Faol battle/konkurslar natijalar bilan yakunlanadi.",
                              reply_markup=kb([[ibi("del", "Ha, o'chirish", cb=f"cby:{r['bot_id']}", style="danger")],
                                               [ibi("w_back", "Yo'q", cb=f"cbm:{r['bot_id']}")]]))


@router.callback_query(F.data.startswith("cby:"))
async def bot_del_do(c: CallbackQuery):
    bid = int(c.data[4:])
    r = q1("SELECT * FROM child_bots WHERE bot_id=?", bid)
    if not _may_manage(c, r):
        return await c.answer("Topilmadi", show_alert=True)
    await c.answer()
    cb = games.BOTS.get(bid)
    if cb:                                # faol yozuvlarni o'chirishdan oldin yakunlaymiz
        for x in qa("SELECT id FROM battles WHERE bot_id=? AND status='active'", bid):
            try:
                await games.as_bot(cb, games.battle_finish(cb, x["id"]))
            except Exception:
                log.exception("battle yakunlanmadi")
        for x in qa("SELECT id FROM contests WHERE bot_id=? AND status='active'", bid):
            try:
                await games.as_bot(cb, games.contest_finish(cb, x["id"]))
            except Exception:
                log.exception("konkurs yakunlanmadi")
    await stop(bid)
    ex("UPDATE battles SET status='done' WHERE bot_id=? AND status='active'", bid)
    ex("UPDATE contests SET status='done' WHERE bot_id=? AND status='active'", bid)
    ex("DELETE FROM child_bots WHERE bot_id=?", bid)
    ex("DELETE FROM child_users WHERE bot_id=?", bid)
    await c.message.edit_text(f"🗑 @{esc(r['username'])} o'chirildi.",
                              reply_markup=kb([[ibi("w_back", "Orqaga", cb="cbl")]]))
