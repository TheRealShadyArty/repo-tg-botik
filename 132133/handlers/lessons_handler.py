"""Обработчик отчета по темам занятий"""
import logging
import pandas as pd
import re
from telegram import Update
from telegram.ext import ContextTypes
from telegram.helpers import escape_markdown
from .report_store import send_and_store

logger = logging.getLogger(__name__)

async def start_lessons_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "📚 *Отчет по темам занятий*\n\n"
            "Загрузите файл *Темы уроков.xls*\n\n"
            "Бот проверит формат тем:\n"
            "`Урок № X. Тема: ...`\n"
            "Некорректные темы будут перечислены.",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "📚 Загрузите файл с темами уроков (Excel).\n"
            "Проверяется формат: 'Урок № X. Тема: ...'"
        )

async def process_lessons_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    try:
        df = pd.read_excel(file_path, header=0)

        # Находим колонку с темами. По умолчанию 'Тема урока', иначе пытаемся угадать.
        topic_col = None
        if 'Тема урока' in df.columns:
            topic_col = 'Тема урока'
        else:
            # Ищем колонку, в имени которой есть 'тема' или похожее, либо первую текстовую колонку
            for col in df.columns:
                if isinstance(col, str) and 'тема' in col.lower():
                    topic_col = col
                    break
            if topic_col is None:
                # Найдём первую колонку с ненулевым количеством строк, которые выглядят как текст
                for col in df.columns:
                    sample = df[col].dropna().astype(str).str.strip()
                    if len(sample) > 0:
                        topic_col = col
                        break

        if topic_col is None:
            await update.message.reply_text("❌ Не удалось определить колонку с темами уроков.")
            return

        # Берём колонку тем — сохраняем все строки, не удаляя дубликаты, сохраняем исходный порядок
        topics_series = df[topic_col].astype(str).fillna('').str.strip()
        if topics_series.dropna().shape[0] == 0 and all(t == '' for t in topics_series):
            await update.message.reply_text("❌ Нет тем уроков в выбранной колонке.")
            return

        # Регулярное выражение: "Урок № [число]. Тема: [что угодно]"
        # Допускаем: опциональную точку после номера, пробелы вокруг "Тема" и двоеточия
        pattern = re.compile(r'^Урок\s*№\s*\d+\.?\s*Тема\s*:\s*.+', re.IGNORECASE)

        correct = []
        incorrect = []

        # Собираем все некорректные записи с указанием номера строки в файле
        for idx, topic in topics_series.items():
            topic_text = topic if isinstance(topic, str) else str(topic)
            if pattern.match(topic_text):
                correct.append(topic_text)
            else:
                # номер строки в Excel приблизительно idx + 2 (заголовок + 1)
                row_no = int(idx) + 2 if hasattr(idx, '__int__') else idx
                incorrect.append((row_no, topic_text))

        report_lines = []
        report_lines.append("📚 Отчет по темам занятий")
        report_lines.append("")
        report_lines.append(f"✅ Корректных тем: {len(correct)}")
        report_lines.append(f"❌ Некорректных тем: {len(incorrect)}")
        report_lines.append("")

        if incorrect:
            report_lines.append("Примеры некорректных тем (первые 100):")
            for row_no, topic_text in incorrect[:100]:
                report_lines.append(f"• [строка {row_no}] {topic_text}")
            if len(incorrect) > 100:
                report_lines.append(f"... и ещё {len(incorrect) - 100} некорректных.")
        else:
            report_lines.append("🎉 Все темы в правильном формате!")

        report = "\n".join(report_lines)

        # Всегда отправляем ответ в чат. Если список некорректных тем большой — разбиваем на части.
        # Telegram ограничивает длину сообщения ~4096 символов; используем безопасный порог 4000.
        MAX_LEN = 4000

        if not incorrect:
            # Ничего большого — просто отправляем итог
            escaped = escape_markdown(report, version=2)
            await update.message.reply_text(escaped, parse_mode='MarkdownV2')
            return

        # Формируем заголовок (первые строки отчёта)
        header_lines = report_lines[:5]  # заголовок, пустая строка, 2 строки с подсчётом и пустая
        header = "\n".join(header_lines) + "\n"

        # Создаём поток строк с некорректными темами (включая номера строк)
        item_lines = [f"• [строка {row_no}] {topic_text}" for row_no, topic_text in incorrect]

        # Собираем и отправляем чанки
        cur = header
        for line in item_lines:
            candidate = cur + line + "\n"
            if len(candidate) > MAX_LEN:
                # отправляем текущий буфер
                escaped = escape_markdown(cur, version=2)
                await send_and_store(update, context, escaped, parse_mode='MarkdownV2', metadata={'type': 'lessons'})
                # начать новый буфер with header removed
                cur = line + "\n"
            else:
                cur = candidate

        # отправляем остаток
        if cur.strip():
            escaped = escape_markdown(cur, version=2)
            await send_and_store(update, context, escaped, parse_mode='MarkdownV2', metadata={'type': 'lessons'})

    except Exception:
        logger.exception("Ошибка при обработке тем занятий")
        try:
            await update.message.reply_text("❌ Ошибка при чтении файла.")
        except Exception:
            pass