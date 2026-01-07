"""Обработчик отчета по студентам — ИЛИ условие"""
import logging
import pandas as pd
from telegram import Update
from telegram.helpers import escape_markdown
from telegram.ext import ContextTypes
from .report_store import send_and_store

logger = logging.getLogger(__name__)

async def start_students_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "👥 *Отчет по студентам*\n\n"
            "Загрузите файл:\n"
            "• Отчет по студентам.xls или .xlsx\n\n"
            "Бот найдёт студентов с:\n"
            "• ДЗ = 1 *или*\n"
            "• Классная работа < 3",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "👥 Загрузите файл с данными студентов.\n"
            "Бот покажет студентов с ДЗ = 1 ИЛИ классной работой < 3"
        )

async def process_students_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    try:
        df = pd.read_excel(file_path, header=0)

        print("\n=== ДАННЫЕ ИЗ ФАЙЛА (последние строки) ===")
        if all(col in df.columns for col in ['FIO', 'Homework', 'Classroom']):
            print(df[['FIO', 'Homework', 'Classroom']].tail(10).to_string())
        print("===========================================\n")

        if not all(col in df.columns for col in ['FIO', 'Homework', 'Classroom']):
            await update.message.reply_text("❌ Нет нужных колонок в файле")
            return

        df['Homework'] = pd.to_numeric(df['Homework'], errors='coerce')
        df['Classroom'] = pd.to_numeric(df['Classroom'], errors='coerce')

        # Проверяем наличие колонки 'Группа'
        has_group = 'Группа' in df.columns

        mask = (df['Homework'] == 1) | (df['Classroom'] < 3)
        cols_to_copy = ['FIO', 'Homework', 'Classroom']
        if has_group:
            cols_to_copy.append('Группа')
        problems = df[mask][cols_to_copy].copy()
        problems['FIO'] = problems['FIO'].str.strip()

        report = "👥 *Отчет по студентам с проблемами*\n\n"

        if len(problems) == 0:
            report += "✅ Проблемных студентов не найдено."
        else:
            count_text = "студент" if len(problems) == 1 else "студента" if 2 <= len(problems) % 10 <= 4 and len(problems) % 100 not in [12,13,14] else "студентов"
            report += f"⚠️ Найдено {len(problems)} {count_text}:\n\n"
            for _, row in problems.iterrows():
                hw = row['Homework']
                cw = row['Classroom']
                reason = []
                if pd.notna(hw) and hw == 1:
                    reason.append("ДЗ = 1 🔥")
                if pd.notna(cw) and cw < 3:
                    reason.append("Классная < 3 ⚠️")

                report += f"• *{row['FIO']}*"
                if has_group:
                    group = row['Группа'] if pd.notna(row['Группа']) else '-'
                    report += f" \({group}\)"
                report += "\n"
                report += f"  ДЗ: {int(hw) if pd.notna(hw) else '-'} | Класс: {cw if pd.notna(cw) else '-'}\n"
                if reason:
                    report += f"  Причина: {', '.join(reason)}\n"
                report += "\n"

        # Экранируем спецсимволы и отправляем безопасно в MarkdownV2
        escaped_report = escape_markdown(report, version=2)
        await send_and_store(update, context, escaped_report, parse_mode='MarkdownV2', metadata={'type': 'students'})

    except Exception as e:
        logger.exception("Ошибка в отчете по студентам")
        await update.message.reply_text("❌ Ошибка при обработке файла.")