import logging
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import vk_api
from dotenv import load_dotenv
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.exceptions import ApiError
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("chudolesa-bot")

VK_TOKEN = (
    os.getenv("VK_TOKEN")
    or os.getenv("VK_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or os.getenv("API_TOKEN")
    or ""
).strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()
GROUP_SCREEN_NAME = os.getenv("GROUP_SCREEN_NAME", "chudolesa").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
WELCOME_DISCOUNT_PERCENT = os.getenv("WELCOME_DISCOUNT_PERCENT", "5").strip()
WELCOME_PROMO_CODE = os.getenv("WELCOME_PROMO_CODE", "ПОДАРОК").strip()
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "subscribers.db"

SUBSCRIBE_WORDS = {"начать", "старт", "start", "подписаться", "/start"}
UNSUBSCRIBE_WORDS = {"отписаться", "стоп", "stop", "/stop"}
HELP_WORDS = {"помощь", "help", "/help", "меню"}

SEND_DELAY_SECONDS = 0.4
RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_admin_ids(raw: str) -> set[int]:
    result: set[int] = set()
    for value in raw.split(","):
        value = value.strip()
        if value:
            result.add(int(value))
    return result


def validate_config() -> set[int]:
    missing = []
    if not VK_TOKEN:
        missing.append("VK_TOKEN/BOT_TOKEN")
    if missing:
        raise RuntimeError(
            "Не заданы обязательные переменные окружения: " + ", ".join(missing)
        )
    return parse_admin_ids(ADMIN_IDS_RAW)


def resolve_group_id(vk) -> int:
    if GROUP_ID_RAW:
        return int(GROUP_ID_RAW)
    result = vk.utils.resolveScreenName(screen_name=GROUP_SCREEN_NAME)
    if not result or result.get("type") != "group":
        raise RuntimeError(
            f"Не удалось определить сообщество по адресу {GROUP_SCREEN_NAME!r}"
        )
    return int(result["object_id"])


def db_connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def init_db() -> None:
    with db_connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                active INTEGER NOT NULL DEFAULT 1,
                subscribed_at TEXT NOT NULL,
                unsubscribed_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS drafts (
                admin_id INTEGER PRIMARY KEY,
                message TEXT NOT NULL,
                attachments TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                attachments TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                sent_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0
            );
            """
        )


def subscribe(user_id: int) -> None:
    now = utc_now()
    with db_connect() as connection:
        connection.execute(
            """
            INSERT INTO subscribers (user_id, active, subscribed_at, unsubscribed_at, last_error)
            VALUES (?, 1, ?, NULL, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                active = 1,
                subscribed_at = excluded.subscribed_at,
                unsubscribed_at = NULL,
                last_error = NULL
            """,
            (user_id, now),
        )


def unsubscribe(user_id: int) -> None:
    with db_connect() as connection:
        connection.execute(
            """
            UPDATE subscribers
            SET active = 0, unsubscribed_at = ?
            WHERE user_id = ?
            """,
            (utc_now(), user_id),
        )


def active_subscriber_ids() -> list[int]:
    with db_connect() as connection:
        rows = connection.execute(
            "SELECT user_id FROM subscribers WHERE active = 1 ORDER BY subscribed_at"
        ).fetchall()
    return [int(row["user_id"]) for row in rows]


def subscriber_counts() -> tuple[int, int]:
    with db_connect() as connection:
        row = connection.execute(
            """
            SELECT
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active_count,
                COUNT(*) AS total_count
            FROM subscribers
            """
        ).fetchone()
    return int(row["active_count"] or 0), int(row["total_count"] or 0)


def save_draft(admin_id: int, message: str, attachments: str) -> None:
    with db_connect() as connection:
        connection.execute(
            """
            INSERT INTO drafts (admin_id, message, attachments, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(admin_id) DO UPDATE SET
                message = excluded.message,
                attachments = excluded.attachments,
                created_at = excluded.created_at
            """,
            (admin_id, message, attachments, utc_now()),
        )


def get_draft(admin_id: int) -> sqlite3.Row | None:
    with db_connect() as connection:
        return connection.execute(
            "SELECT * FROM drafts WHERE admin_id = ?", (admin_id,)
        ).fetchone()


def delete_draft(admin_id: int) -> None:
    with db_connect() as connection:
        connection.execute("DELETE FROM drafts WHERE admin_id = ?", (admin_id,))


def start_campaign(admin_id: int, message: str, attachments: str) -> int:
    with db_connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO campaigns (admin_id, message, attachments, started_at)
            VALUES (?, ?, ?, ?)
            """,
            (admin_id, message, attachments, utc_now()),
        )
        return int(cursor.lastrowid)


def finish_campaign(campaign_id: int, sent_count: int, failed_count: int) -> None:
    with db_connect() as connection:
        connection.execute(
            """
            UPDATE campaigns
            SET finished_at = ?, sent_count = ?, failed_count = ?
            WHERE id = ?
            """,
            (utc_now(), sent_count, failed_count, campaign_id),
        )


def record_delivery_error(user_id: int, error: str) -> None:
    with db_connect() as connection:
        connection.execute(
            "UPDATE subscribers SET last_error = ? WHERE user_id = ?",
            (error[:500], user_id),
        )


def main_keyboard() -> str:
    keyboard = VkKeyboard(one_time=False)
    keyboard.add_button("Подписаться", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("Отписаться", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()


def send_message(vk, peer_id: int, message: str, attachment: str = "") -> None:
    params = {
        "peer_id": peer_id,
        "random_id": random.randint(1, 2_147_483_647),
        "message": message,
    }
    if attachment:
        params["attachment"] = attachment
    vk.messages.send(**params)


def send_with_retry(vk, peer_id: int, message: str, attachment: str = "") -> None:
    for attempt in range(len(RETRY_DELAYS_SECONDS) + 1):
        try:
            send_message(vk, peer_id, message, attachment)
            return
        except ApiError as error:
            if error.code != 6 or attempt == len(RETRY_DELAYS_SECONDS):
                raise
            time.sleep(RETRY_DELAYS_SECONDS[attempt])


def attachments_to_vk_string(attachments) -> str:
    result = []
    for attachment in attachments or []:
        kind = attachment.get("type")
        item = attachment.get(kind, {}) if kind else {}
        owner_id = item.get("owner_id")
        item_id = item.get("id")
        if not kind or owner_id is None or item_id is None:
            continue
        value = f"{kind}{owner_id}_{item_id}"
        access_key = item.get("access_key")
        if access_key:
            value += f"_{access_key}"
        result.append(value)
    return ",".join(result)


def user_help_text() -> str:
    return (
        "Я бот сообщества «Чудо леса» 🌿\n\n"
        "Нажмите «Подписаться», чтобы получать новости о свечах, декоре "
        "и мастер-классах. Отказаться можно в любой момент кнопкой «Отписаться»."
    )


def welcome_message() -> str:
    return (
        "Добро пожаловать в «Чудо леса» 🌿\n\n"
        "Теперь вы будете получать наши новости о свечах, декоре "
        "и мастер-классах.\n\n"
        f"🎁 Спасибо за подписку! Дарим вам скидку {WELCOME_DISCOUNT_PERCENT}% "
        "на первый заказ.\n"
        f"Промокод: {WELCOME_PROMO_CODE}\n"
        "Сообщите его нам при оформлении заказа.\n\n"
        "Скидка действует один раз и не суммируется с другими предложениями.\n"
        "Отписаться можно в любой момент."
    )


def admin_help_text() -> str:
    return (
        "Команды администратора:\n"
        "• /рассылка Текст — подготовить рассылку. Можно приложить фото.\n"
        "• /отправить — отправить подготовленную рассылку.\n"
        "• /отмена — удалить черновик.\n"
        "• /статистика — показать число подписчиков.\n\n"
        "Перед отправкой бот всегда показывает черновик."
    )


broadcast_lock = threading.Lock()


def run_broadcast(vk, admin_id: int, message: str, attachments: str) -> None:
    if not broadcast_lock.acquire(blocking=False):
        send_message(vk, admin_id, "Другая рассылка уже выполняется. Подождите её завершения.")
        return

    try:
        recipients = active_subscriber_ids()
        campaign_id = start_campaign(admin_id, message, attachments)
        sent_count = 0
        failed_count = 0

        for user_id in recipients:
            try:
                send_with_retry(vk, user_id, message, attachments)
                sent_count += 1
            except Exception as error:  # Ошибка одного адресата не останавливает рассылку.
                failed_count += 1
                record_delivery_error(user_id, str(error))
                logger.warning("Не удалось отправить сообщение user_id=%s: %s", user_id, error)
            time.sleep(SEND_DELAY_SECONDS)

        finish_campaign(campaign_id, sent_count, failed_count)
        send_message(
            vk,
            admin_id,
            "Рассылка завершена ✅\n"
            f"Отправлено: {sent_count}\n"
            f"Не доставлено: {failed_count}",
        )
    finally:
        broadcast_lock.release()


def handle_admin_command(vk, admin_id: int, text: str, attachments: str) -> bool:
    lowered = text.lower()

    if lowered == "/статистика":
        active_count, total_count = subscriber_counts()
        send_message(
            vk,
            admin_id,
            f"Активных подписчиков: {active_count}\nВсего записей: {total_count}",
        )
        return True

    if lowered == "/помощь":
        send_message(vk, admin_id, admin_help_text())
        return True

    if lowered == "/отмена":
        delete_draft(admin_id)
        send_message(vk, admin_id, "Черновик удалён.")
        return True

    if lowered == "/отправить":
        draft = get_draft(admin_id)
        if draft is None:
            send_message(vk, admin_id, "Черновика нет. Сначала используйте /рассылка Текст")
            return True
        delete_draft(admin_id)
        send_message(vk, admin_id, "Начинаю рассылку…")
        threading.Thread(
            target=run_broadcast,
            args=(vk, admin_id, draft["message"], draft["attachments"]),
            daemon=True,
        ).start()
        return True

    if lowered == "/рассылка" or lowered.startswith("/рассылка "):
        message = text[len("/рассылка") :].strip()
        if not message and not attachments:
            send_message(
                vk,
                admin_id,
                "Добавьте текст после команды или приложите фотографию.\n"
                "Пример: /рассылка В субботу состоится мастер-класс!",
            )
            return True
        save_draft(admin_id, message, attachments)
        preview_text = "Черновик рассылки:\n\n" + (message or "(только вложение)")
        send_message(vk, admin_id, preview_text, attachments)
        send_message(
            vk,
            admin_id,
            "Если всё верно, отправьте /отправить\nДля отмены — /отмена",
        )
        return True

    return False


def run() -> None:
    admin_ids = validate_config()
    init_db()

    session = vk_api.VkApi(token=VK_TOKEN, api_version="5.199")
    vk = session.get_api()
    group_id = resolve_group_id(vk)
    longpoll = VkBotLongPoll(session, group_id)
    logger.info("Бот запущен для сообщества %s", group_id)
    if not admin_ids:
        logger.warning(
            "ADMIN_IDS пока не задан. Отправьте боту команду /мойid, "
            "затем добавьте полученное число в переменную ADMIN_IDS."
        )

    for event in longpoll.listen():
        if event.type != VkBotEventType.MESSAGE_NEW:
            continue

        message = event.obj.message
        user_id = int(message.get("from_id", 0))
        peer_id = int(message.get("peer_id", 0))
        if user_id <= 0 or peer_id != user_id:
            continue

        text = str(message.get("text", "")).strip()
        lowered = text.lower()
        attachments = attachments_to_vk_string(message.get("attachments", []))

        try:
            if lowered in {"/мойid", "мой id", "мойid"}:
                send_message(
                    vk,
                    user_id,
                    f"Ваш числовой VK ID: {user_id}\n"
                    "Сохраните это число для настройки администратора.",
                )
                continue

            if user_id in admin_ids and handle_admin_command(
                vk, user_id, text, attachments
            ):
                continue

            if lowered in SUBSCRIBE_WORDS:
                subscribe(user_id)
                vk.messages.send(
                    peer_id=user_id,
                    random_id=random.randint(1, 2_147_483_647),
                    message=welcome_message(),
                    keyboard=main_keyboard(),
                )
            elif lowered in UNSUBSCRIBE_WORDS:
                unsubscribe(user_id)
                vk.messages.send(
                    peer_id=user_id,
                    random_id=random.randint(1, 2_147_483_647),
                    message="Вы отписались от рассылки. Возвращайтесь, когда захотите 🌿",
                    keyboard=main_keyboard(),
                )
            elif lowered in HELP_WORDS or not text:
                vk.messages.send(
                    peer_id=user_id,
                    random_id=random.randint(1, 2_147_483_647),
                    message=user_help_text(),
                    keyboard=main_keyboard(),
                )
            else:
                vk.messages.send(
                    peer_id=user_id,
                    random_id=random.randint(1, 2_147_483_647),
                    message=(
                        "Спасибо за сообщение! Выберите действие на клавиатуре ниже.\n\n"
                        + user_help_text()
                    ),
                    keyboard=main_keyboard(),
                )
        except Exception:
            logger.exception("Ошибка при обработке сообщения от user_id=%s", user_id)


if __name__ == "__main__":
    while True:
        try:
            run()
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Бот остановился с ошибкой; повторный запуск через 5 секунд")
            time.sleep(5)
