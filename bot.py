import asyncio
import logging
import os
from datetime import datetime, timedelta
from enum import Enum
from zoneinfo import ZoneInfo
from html import escape

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")

GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")

if GROUP_CHAT_ID is None:
    raise Exception("GROUP_CHAT_ID is not specified")

GROUP_CHAT_ID = int(GROUP_CHAT_ID)

DB_PATH = os.getenv("DB_PATH", "./tasks.sqlite3")

DEVELOPMENT = os.getenv("DEVEL") is not None

TIMEZONE = ZoneInfo("Europe/Moscow")

if not API_TOKEN:
    raise Exception("API_TOKEN is not specified")

bot = Bot(token=API_TOKEN)

dp = Dispatcher()

scheduler = AsyncIOScheduler(timezone=TIMEZONE)

# =========================
# STATES
# =========================


class TaskStates(StatesGroup):
    waiting_for_task = State()
    waiting_for_edit = State()


# =========================
# ENUMS
# =========================


class TaskStatus(str, Enum):
    OPEN = "Открыта"
    IN_PROGRESS = "В процессе"
    DONE = "Готово"

    @property
    def icon(self):
        return {
            self.OPEN: "⏳",
            self.IN_PROGRESS: "🟧",
            self.DONE: "✅",
        }[self]

    def next(self):
        members = list(TaskStatus)
        return members[(members.index(self) + 1) % len(members)]


# =========================
# HELPERS
# =========================


def today():
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d")


def get_date_days_ago(days: int):
    return (
        datetime.now(TIMEZONE) - timedelta(days=days)
    ).strftime("%Y-%m-%d")


def split_message(text: str, limit=4000):

    chunks = []

    while len(text) > limit:

        split_index = text.rfind("\n", 0, limit)

        if split_index == -1:
            split_index = limit

        chunks.append(text[:split_index])

        text = text[split_index:]

    chunks.append(text)

    return chunks


# =========================
# DATABASE
# =========================


async def init_db():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                status TEXT NOT NULL,
                task_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        """)

        await db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_daily_task
            ON tasks(user_id, text, task_date)
        """)

        migrations = [
            ("task_date", "TEXT"),
            ("created_at", "TEXT DEFAULT CURRENT_TIMESTAMP"),
            ("updated_at", "TEXT DEFAULT CURRENT_TIMESTAMP"),
        ]

        for column_name, column_type in migrations:

            try:

                await db.execute(
                    f"""
                    ALTER TABLE tasks
                    ADD COLUMN {column_name} {column_type}
                    """
                )

            except aiosqlite.OperationalError as e:

                if "duplicate column name" not in str(e):
                    raise

        await db.commit()


# =========================
# USERS
# =========================


async def ensure_user(user: types.User):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT OR IGNORE INTO users (user_id, name)
            VALUES (?, ?)
            """,
            (
                user.id,
                user.full_name,
            ),
        )

        await db.commit()


# =========================
# TASKS
# =========================


async def add_task(user_id: int, text: str):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO tasks (
                user_id,
                text,
                status,
                task_date
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                user_id,
                text,
                TaskStatus.OPEN.value,
                today(),
            ),
        )

        await db.commit()

        return cursor.lastrowid


async def get_user_tasks(user_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            """
            SELECT
                id,
                text,
                status,
                created_at,
                updated_at
            FROM tasks
            WHERE user_id = ?
            AND task_date = ?
            ORDER BY
                CASE status
                    WHEN 'Открыта' THEN 1
                    WHEN 'В процессе' THEN 2
                    WHEN 'Готово' THEN 3
                END
            """,
            (
                user_id,
                today(),
            ),
        )

        rows = await cursor.fetchall()

        return [dict(row) for row in rows]


async def get_task_by_id(task_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            """
            SELECT
                id,
                text,
                status,
                created_at,
                updated_at
            FROM tasks
            WHERE id = ?
            """,
            (task_id,),
        )

        row = await cursor.fetchone()

        return dict(row) if row else None


async def get_tasks_between_dates(start_date, end_date):

    async with aiosqlite.connect(DB_PATH) as db:

        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            """
            SELECT
                u.name,
                u.user_id,
                t.id AS task_id,
                t.text,
                t.status,
                t.task_date
            FROM users u
            INNER JOIN tasks t
                ON u.user_id = t.user_id
            WHERE t.task_date BETWEEN ? AND ?
            ORDER BY
                t.task_date DESC,
                u.name
            """,
            (
                start_date,
                end_date,
            ),
        )

        rows = await cursor.fetchall()

    users = {}

    for row in rows:

        user_id = row["user_id"]

        if user_id not in users:

            users[user_id] = {
                "user_id": row["user_id"],
                "name": row["name"],
                "tasks": [],
            }

        users[user_id]["tasks"].append(
            {
                "id": row["task_id"],
                "text": row["text"],
                "status": row["status"],
                "date": row["task_date"],
            }
        )

    return list(users.values())


async def cycle_task_status(task_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            "SELECT status FROM tasks WHERE id = ?",
            (task_id,),
        )

        row = await cursor.fetchone()

        if not row:
            return

        new_status = TaskStatus(row[0]).next()

        await db.execute(
            """
            UPDATE tasks
            SET
                status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                new_status.value,
                task_id,
            ),
        )

        await db.commit()


async def update_task_text(task_id: int, text: str):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            UPDATE tasks
            SET
                text = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                text,
                task_id,
            ),
        )

        await db.commit()


async def delete_task(task_id: int):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            "DELETE FROM tasks WHERE id = ?",
            (task_id,),
        )

        await db.commit()


# =========================
# CARRY OVER TASKS
# =========================


async def carry_over_unfinished_tasks():

    yesterday = get_date_days_ago(1)

    async with aiosqlite.connect(DB_PATH) as db:

        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            """
            SELECT
                user_id,
                text
            FROM tasks
            WHERE task_date = ?
            AND status != ?
            """,
            (
                yesterday,
                TaskStatus.DONE.value,
            ),
        )

        rows = await cursor.fetchall()

        for row in rows:

            await db.execute(
                """
                INSERT OR IGNORE INTO tasks (
                    user_id,
                    text,
                    status,
                    task_date
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    row["user_id"],
                    row["text"],
                    TaskStatus.OPEN.value,
                    today(),
                ),
            )

        await db.commit()

    logging.info("unfinished tasks carried over")


# =========================
# CLEANUP
# =========================


async def cleanup_old_tasks():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            DELETE FROM tasks
            WHERE task_date < date('now', '-90 day')
        """)

        await db.commit()


# =========================
# KEYBOARDS
# =========================


def get_main_keyboard():

    builder = ReplyKeyboardBuilder()

    builder.button(text="➕ Добавить задачу")
    builder.button(text="📋 Мои задачи")
    builder.button(text="📊 Отправить отчет")
    builder.button(text="📅 Недельный отчет")
    builder.button(text="🗓 Месячный отчет")
    builder.button(text="🧹 Очистить мои задачи")

    builder.adjust(1)

    return builder.as_markup(resize_keyboard=True)


def build_task_keyboard(task: dict, user_id: int):

    builder = InlineKeyboardBuilder()

    icon = TaskStatus(task["status"]).icon

    builder.button(
        text=f"{icon} {task['status']}",
        callback_data=f"st_{task['id']}_{user_id}",
    )

    builder.button(
        text="✏️ Редактировать",
        callback_data=f"ed_{task['id']}_{user_id}",
    )

    builder.button(
        text="🗑 Удалить",
        callback_data=f"del_{task['id']}_{user_id}",
    )

    builder.adjust(2, 1)

    return builder.as_markup()


# =========================
# TASK MESSAGE
# =========================


async def send_task_message(message: types.Message, task: dict):

    await message.answer(
        f"📌 <b>Задача #{task['id']}</b>\n\n"
        f"{format_task_text(task['text'])}",
        parse_mode="HTML",
        reply_markup=build_task_keyboard(
            task,
            message.from_user.id,
        ),
    )


# =========================
# START
# =========================


@dp.message(Command("start"))
async def cmd_start(message: types.Message):

    if message.chat.type != "private":
        return

    await ensure_user(message.from_user)

    text = (
        "👋 Добро пожаловать в систему отчетов.\n\n"
        "➕ Добавляйте задачи\n"
        "📋 Меняйте статусы\n"
        "📊 Отчеты формируются автоматически"
    )

    await message.answer(
        text,
        reply_markup=get_main_keyboard(),
    )


# =========================
# ADD TASK
# =========================


@dp.message(F.text == "➕ Добавить задачу")
async def add_task_start(message: types.Message, state: FSMContext):

    await state.set_state(TaskStates.waiting_for_task)

    await message.answer("📝 Напишите задачу:")


@dp.message(TaskStates.waiting_for_task)
async def add_task_finish(message: types.Message, state: FSMContext):

    task_text = message.text.strip()

    if len(task_text) < 2:

        return await message.answer(
            "❌ Слишком короткая задача."
        )

    if len(task_text) > 300:

        return await message.answer(
            "❌ Максимальная длина задачи: 300 символов."
        )

    await ensure_user(message.from_user)

    task_id = await add_task(
        message.from_user.id,
        task_text,
    )

    await state.clear()

    await message.answer(
        "✅ Задача добавлена.",
        reply_markup=get_main_keyboard(),
    )

    await send_task_message(
        message,
        {
            "id": task_id,
            "text": task_text,
            "status": TaskStatus.OPEN.value,
        },
    )


# =========================
# MY TASKS
# =========================


@dp.message(Command("my_tasks"))
@dp.message(F.text == "📋 Мои задачи")
async def show_tasks(message: types.Message):

    tasks = await get_user_tasks(message.from_user.id)

    if not tasks:

        return await message.answer(
            "📭 У вас пока нет задач на сегодня.",
            reply_markup=get_main_keyboard(),
        )

    await message.answer(
        "📋 Ваши задачи на сегодня:"
    )

    for task in tasks:
        await send_task_message(message, task)


# =========================
# STATUS
# =========================


@dp.callback_query(F.data.startswith("st_"))
async def change_status(callback: types.CallbackQuery):

    _, task_id, user_id = callback.data.split("_")

    task_id = int(task_id)
    user_id = int(user_id)

    if callback.from_user.id != user_id:

        return await callback.answer(
            "❌ Нет доступа",
            show_alert=True,
        )

    await cycle_task_status(task_id)

    task = await get_task_by_id(task_id)

    if not task:
        return

    await callback.message.edit_reply_markup(
        reply_markup=build_task_keyboard(
            task,
            user_id,
        ),
    )

    await callback.answer("✅ Статус обновлен")


# =========================
# EDIT
# =========================


@dp.callback_query(F.data.startswith("ed_"))
async def edit_start(
    callback: types.CallbackQuery,
    state: FSMContext,
):

    _, task_id, user_id = callback.data.split("_")

    task_id = int(task_id)
    user_id = int(user_id)

    if callback.from_user.id != user_id:

        return await callback.answer(
            "❌ Нет доступа",
            show_alert=True,
        )

    await state.update_data(task_id=task_id)

    await state.set_state(TaskStates.waiting_for_edit)

    await callback.message.answer(
        "✏️ Введите новый текст задачи:"
    )

    await callback.answer()


@dp.message(TaskStates.waiting_for_edit)
async def edit_finish(message: types.Message, state: FSMContext):

    new_text = message.text.strip()

    if len(new_text) > 300:

        return await message.answer(
            "❌ Максимальная длина задачи: 300 символов."
        )

    data = await state.get_data()

    task_id = int(data["task_id"])

    await update_task_text(task_id, new_text)

    await state.clear()

    await message.answer(
        "✅ Задача обновлена.",
        reply_markup=get_main_keyboard(),
    )

    task = await get_task_by_id(task_id)

    if task:
        await send_task_message(message, task)


# =========================
# DELETE
# =========================


@dp.callback_query(F.data.startswith("del_"))
async def delete_task_handler(callback: types.CallbackQuery):

    _, task_id, user_id = callback.data.split("_")

    task_id = int(task_id)
    user_id = int(user_id)

    if callback.from_user.id != user_id:

        return await callback.answer(
            "❌ Нет доступа",
            show_alert=True,
        )

    await delete_task(task_id)

    await callback.message.delete()

    await callback.answer("🗑 Задача удалена")


# =========================
# CLEAR
# =========================


@dp.message(Command("clear"))
@dp.message(F.text == "🧹 Очистить мои задачи")
async def clear_tasks(message: types.Message):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            DELETE FROM tasks
            WHERE user_id = ?
            AND task_date = ?
            """,
            (
                message.from_user.id,
                today(),
            ),
        )

        await db.commit()

    await message.answer(
        "🧹 Все сегодняшние задачи удалены.",
        reply_markup=get_main_keyboard(),
    )


# =========================
# REPORT BUILDER
# =========================

def format_task_text(text: str):

    lines = text.split("\n")

    formatted = []

    for line in lines:

        line = escape(line.strip())

        if line.startswith(("-", "•", "—")):

            formatted.append(
                f"\n     ↳ {line[1:].strip()}"
            )

        else:
            formatted.append(line)

    return "\n".join(formatted)


async def build_report(
    start_date,
    end_date,
    report_title,
):

    users = await get_tasks_between_dates(
        start_date,
        end_date,
    )

    start_pretty = datetime.strptime(
        start_date,
        "%Y-%m-%d",
    ).strftime("%d.%m.%Y")

    end_pretty = datetime.strptime(
        end_date,
        "%Y-%m-%d",
    ).strftime("%d.%m.%Y")

    report = (
        f"📊 <b>{escape(report_title)}</b>\n"
        f"📅 {start_pretty} — {end_pretty}\n\n"
    )

    if not users:

        report += "🤷 Никто не заполнил задачи."

        return report

    for user in users:

        report += (
            f'👤 <a href="tg://user?id={user["user_id"]}">'
            f'{escape(user["name"])}</a>\n'
        )

        for task in user["tasks"]:

            icon = TaskStatus(task["status"]).icon

            task_date = datetime.strptime(
                task["date"],
                "%Y-%m-%d",
            ).strftime("%d.%m")

            report += (
                f"{icon} "
                f"{format_task_text(task['text'])} "
                f"<i>({escape(task['status'])})</i> "
                f"• {task_date}\n"
            )

        report += "\n"

    return report
# =========================
# DAILY REPORT
# =========================


async def send_daily_report(report_type="Отчет"):

    report = await build_report(
        today(),
        today(),
        report_type,
    )

    parts = split_message(report)

    for part in parts:

        await bot.send_message(
            GROUP_CHAT_ID,
            part,
            parse_mode="HTML",
        )


# =========================
# WEEKLY REPORT
# =========================


async def send_weekly_report():

    start_date = get_date_days_ago(7)

    report = await build_report(
        start_date,
        today(),
        "Недельный отчет",
    )

    parts = split_message(report)

    for part in parts:

        await bot.send_message(
            GROUP_CHAT_ID,
            part,
            parse_mode="HTML",
        )


# =========================
# MONTHLY REPORT
# =========================


async def send_monthly_report():

    start_date = get_date_days_ago(30)

    report = await build_report(
        start_date,
        today(),
        "Месячный отчет",
    )

    parts = split_message(report)

    for part in parts:

        await bot.send_message(
            GROUP_CHAT_ID,
            part,
            parse_mode="HTML",
        )


# =========================
# REPORT COMMANDS
# =========================


@dp.message(Command("report"))
@dp.message(F.text == "📊 Отправить отчет")
async def report_command(message: types.Message):

    await send_daily_report("Ручной отчет")

    await message.answer(
        "✅ Отчет отправлен.",
        reply_markup=get_main_keyboard(),
    )


@dp.message(F.text == "📅 Недельный отчет")
async def weekly_report_command(message: types.Message):

    await send_weekly_report()

    await message.answer(
        "✅ Недельный отчет отправлен.",
        reply_markup=get_main_keyboard(),
    )


@dp.message(F.text == "🗓 Месячный отчет")
async def monthly_report_command(message: types.Message):

    await send_monthly_report()

    await message.answer(
        "✅ Месячный отчет отправлен.",
        reply_markup=get_main_keyboard(),
    )


# =========================
# MORNING REMINDER
# =========================


async def send_morning_reminder():

    text = (
        "🌅 Доброе утро!\n\n"
        "Не забудьте заполнить план задач до 10:00."
    )

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            "SELECT user_id FROM users"
        )

        rows = await cursor.fetchall()

    for row in rows:

        try:

            await bot.send_message(
                row[0],
                text,
                reply_markup=get_main_keyboard(),
            )

        except Exception as e:
            logging.error(e)


# =========================
# MAIN
# =========================


async def main():

    logging.basicConfig(
        level=logging.DEBUG if DEVELOPMENT else logging.INFO
    )

    await init_db()

    scheduler.add_job(
        carry_over_unfinished_tasks,
        "cron",
        hour=9,
        minute=6,
    )

    scheduler.add_job(
        send_morning_reminder,
        "cron",
        hour=9,
        minute=5,
    )

    scheduler.add_job(
        send_daily_report,
        "cron",
        hour=10,
        minute=0,
        args=["Утренний План"],
    )

    scheduler.add_job(
        send_daily_report,
        "cron",
        hour=13,
        minute=0,
        args=["13:00"],
    )

    scheduler.add_job(
        send_daily_report,
        "cron",
        hour=18,
        minute=0,
        args=["18:00"],
    )

    scheduler.add_job(
        send_weekly_report,
        "cron",
        day_of_week="fri",
        hour=18,
        minute=0,
    )

    scheduler.add_job(
        send_monthly_report,
        "cron",
        day="last",
        hour=18,
        minute=00,
    )

    scheduler.add_job(
        cleanup_old_tasks,
        "cron",
        hour=0,
        minute=10,
    )

    scheduler.start()

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
