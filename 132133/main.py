import os
import sys
import logging
import tempfile
from dotenv import load_dotenv

# Загрузить переменные окружения из .env
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from handlers import (
    schedule_handler,
    lessons_handler,
    students_handler,
    attendance_handler,
    homework_check_handler,
    homework_submit_handler,
    ai_handler,
)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния разговора 
SCHEDULE = "schedule"
LESSONS = "lessons"
STUDENTS = "students"
ATTENDANCE = "attendance"
HOMEWORK_CHECK = "homework_check"
HOMEWORK_SUBMIT = "homework_submit"
AI = "ai"

# Главное меню 
def get_main_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📅 Отчет по расписанию", callback_data=SCHEDULE)],
            [InlineKeyboardButton("📚 Отчет по темам занятий", callback_data=LESSONS)],
            [InlineKeyboardButton("👥 Отчет по студентам", callback_data=STUDENTS)],
            [InlineKeyboardButton("📊 Отчет по посещаемости", callback_data=ATTENDANCE)],
            [InlineKeyboardButton("✅ Отчет по проверке ДЗ", callback_data=HOMEWORK_CHECK)],
            [InlineKeyboardButton("📝 Отчет по сдаче ДЗ", callback_data=HOMEWORK_SUBMIT)],
            [InlineKeyboardButton("🤖 AI-помощник", callback_data=AI)],
            [InlineKeyboardButton("❓ Справка", callback_data="help")],
            [InlineKeyboardButton("🔄 Начать заново", callback_data="restart")],
        ]
    )


def get_start_reply_keyboard():
    """Reply keyboard with a single /start button shown near the message bar."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton('/start')]], resize_keyboard=True, one_time_keyboard=False
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start и кнопки 'Начать заново'"""
    text = "👋 Привет! Я бот для анализа учебных отчётов.\n\nВыберите нужный отчёт:"
    reply_markup = get_main_keyboard()

    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
        # /start button near the input bar
        await update.message.reply_text(
            "Нажмите кнопку /start для быстрого открытия меню.",
            reply_markup=get_start_reply_keyboard(),
        )
    else:  # от callback
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        # send a small message with the reply keyboard so the client shows the button
        if update.callback_query.message:
            await update.callback_query.message.reply_text(
                "Нажмите кнопку /start для быстрого открытия меню.",
                reply_markup=get_start_reply_keyboard(),
            )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Справка по боту"""
    help_text = """
🤖 *Справка по боту*

*Доступные отчёты:*

📅 *Отчет по расписанию* — файл Расписание групп.xlsx
📚 *Отчет по темам занятий* — файл Темы уроков.xls
👥 *Отчет по студентам* — файл Отчет по студентам.xls
📊 *Отчет по посещаемости* — файл Посещаемость по преподавателям.xlsx
✅ *Отчет по проверке ДЗ* — файл Отчет по домашним заданиям.xlsx
📝 *Отчет по сдаче ДЗ* — файл Отчет по студентам.xls

*Как пользоваться:*
1. Нажмите на нужный отчёт
2. Загрузите соответствующий Excel-файл
3. Получите результат

Команды:
/start — главное меню
/help — эта справка
/cancel — отменить текущую операцию
"""

    if update.message:
        await update.message.reply_text(help_text, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(help_text, parse_mode="Markdown")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Обработка всех inline-кнопок"""
    query = update.callback_query
    await query.answer()

    choice = query.data

    # cправка
    if choice == "help":
        await help_command(update, context)
        return ConversationHandler.END

    # перезапуск 
    if choice == "restart":
        await start(update, context)
        return ConversationHandler.END

    # выбор периода по проверке ДЗ
    if choice in ("hw_check_month", "hw_check_week"):
        context.user_data["report_type"] = HOMEWORK_CHECK
        await homework_check_handler.handle_hw_check_period(update, context)
        return HOMEWORK_CHECK

    # сопоставление кнопки с функцией запуска отчёта
    start_handlers = {
        SCHEDULE: schedule_handler.start_schedule_report,
        LESSONS: lessons_handler.start_lessons_report,
        STUDENTS: students_handler.start_students_report,
        ATTENDANCE: attendance_handler.start_attendance_report,
        HOMEWORK_CHECK: homework_check_handler.start_homework_check_report,
        HOMEWORK_SUBMIT: homework_submit_handler.start_homework_submit_report,
        AI: ai_handler.start_ai_report,
    }

    handler_func = start_handlers.get(choice)
    if handler_func:
        context.user_data["report_type"] = choice
        
        await handler_func(update, context)
        return choice

    return ConversationHandler.END

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Единый обработчик всех загруженных файлов"""
    report_type = context.user_data.get("report_type")

    if not report_type:
        await update.message.reply_text("❌ Сначала выберите отчёт из меню.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    document = update.message.document
    if not document or not document.file_name.lower().endswith((".xls", ".xlsx")):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл Excel (.xls или .xlsx).")
        return report_type

    await update.message.reply_text("📥 Файл получен, обрабатываю...")

    tmp_path = None
    try:
        # Защита от двойной обработки одного и того же документа
        processed_key = f"processed_{document.file_id}"
        if context.user_data.get(processed_key):
            await update.message.reply_text("❗ Этот файл уже обрабатывается или был обработан.")
            return report_type

        file_obj = await document.get_file()
        # используем NamedTemporaryFile для безопасного управления временным файлом
        tmp = tempfile.NamedTemporaryFile(prefix="bot_", suffix=".xlsx", delete=False)
        tmp_path = tmp.name
        tmp.close()
        await file_obj.download_to_drive(tmp_path)
        # помечаем как обработанный (чтобы избежать повторной обработки при дублированных апдейтах)
        context.user_data[processed_key] = True

        # выбор процесса
        processors = {
            SCHEDULE: schedule_handler.process_schedule_file,
            LESSONS: lessons_handler.process_lessons_file,
            STUDENTS: students_handler.process_students_file,
            ATTENDANCE: attendance_handler.process_attendance_file,
            HOMEWORK_CHECK: homework_check_handler.process_homework_check_file,
            HOMEWORK_SUBMIT: homework_submit_handler.process_homework_submit_file,
        }

        processor = processors.get(report_type)
        if processor:
            await processor(update, context, tmp_path)

        # возврат в главное меню
        await update.message.reply_text("✅ Готово! Выберите следующий отчёт:", reply_markup=get_main_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        logger.exception("Ошибка при обработке файла")
        await update.message.reply_text("❌ Произошла ошибка при обработке файла.")
        return ConversationHandler.END

    finally:
        # удаление временного файла, если он был создан
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                logger.warning("Не удалось удалить временный файл: %s", tmp_path)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена текущей операции"""
    await update.message.reply_text("❌ Операция отменена.", reply_markup=get_main_keyboard())
    context.user_data.clear()
    return ConversationHandler.END

def main():
    # Загружаем переменные окружения из .env файла
    load_dotenv()
    
    # токен
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
        sys.exit(1)

    # создание приложения
    application = Application.builder().token(token).build()

    # store reusable objects in bot_data for handlers (e.g., main keyboard)
    application.bot_data["main_keyboard"] = get_main_keyboard()

    # ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(button_handler)],
        states={
            SCHEDULE: [MessageHandler(filters.Document.ALL, file_handler)],
            LESSONS: [MessageHandler(filters.Document.ALL, file_handler)],
            STUDENTS: [MessageHandler(filters.Document.ALL, file_handler)],
            ATTENDANCE: [MessageHandler(filters.Document.ALL, file_handler)],
            HOMEWORK_CHECK: [MessageHandler(filters.Document.ALL, file_handler)],
            HOMEWORK_SUBMIT: [MessageHandler(filters.Document.ALL, file_handler)],
            AI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ai_handler.process_ai_query),
                MessageHandler(filters.Document.ALL, ai_handler.process_ai_file),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Добавляем только этот хендлер и /help
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("help", help_command))

    # Allow asking the AI by replying to any message (no need to enter AI mode)
    # Reply with text -> routed to process_ai_query
    application.add_handler(MessageHandler(filters.TEXT & filters.REPLY, ai_handler.process_ai_query))
    # Reply with a document -> routed to process_ai_file
    application.add_handler(MessageHandler(filters.Document.ALL & filters.REPLY, ai_handler.process_ai_file))

    # Webhook конфиг для Render
    webhook_url = os.getenv("WEBHOOK_URL")
    port = int(os.getenv("PORT", "8080"))
    listen = "0.0.0.0"

    if webhook_url:
        print(f"🤖 Запускаю webhook на {listen}:{port}")
        print(f"   Webhook URL: {webhook_url}/{token}")
        application.run_webhook(
            listen=listen,
            port=port,
            url_path=f"/{token}",
            webhook_url=f"{webhook_url}/{token}",
        )
    else:
        print("⚠️  WEBHOOK_URL не задан — работаю в polling-режиме (может быть медленнее на Render)")
        application.run_polling()

if __name__ == "__main__":

    main()
