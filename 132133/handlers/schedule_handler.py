"""Обработчик отчета по расписанию"""
import logging
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes
from .report_store import send_and_store
from collections import Counter

logger = logging.getLogger(__name__)

async def start_schedule_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сообщение перед загрузкой файла"""
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "📅 Загрузите файл с расписанием групп (Расписание групп.xlsx).\n"
            "Бот посчитает количество пар по каждой дисциплине для каждой группы."
        )
    else:
        await update.message.reply_text(
            "📅 Загрузите файл с расписанием групп (Расписание групп.xlsx).\n"
            "Бот посчитает количество пар по каждой дисциплине для каждой группы."
        )

async def process_schedule_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    """Обработка файла и генерация отчета — ОСТАВИТЬ ТОЛЬКО ОДНУ ЭТУ ФУНКЦИЮ"""
    try:
        logger.info("process_schedule_file called for %s", getattr(update.message, 'message_id', 'no-message-id'))
        # Дополнительная защита: помечаем, что обработка этого файла идёт
        doc_flag = context.user_data.get('processing_schedule')
        if doc_flag:
            logger.info("Schedule processing already in progress, skipping duplicate call")
            return
        context.user_data['processing_schedule'] = True

        df = pd.read_excel(file_path)

        if 'Группа' not in df.columns:
            await update.message.reply_text("❌ В файле не найдена колонка 'Группа'. Файл некорректный.")
            return

        content_columns = df.columns[3::2]
        if len(content_columns) == 0:
            await update.message.reply_text("❌ Не найдены колонки с расписанием по дням.")
            return

        groups = df['Группа'].dropna().unique()

        report = "📅 *Отчет по выставленному расписанию*\n\n"
        overall_total = 0

        for group in groups:
            if pd.isna(group) or str(group).strip() == '':
                continue

            group_df = df[df['Группа'] == group]
            disciplines = []

            for col in content_columns:
                for cell in group_df[col]:
                    if pd.notna(cell):
                        cell_str = str(cell)
                        for line in cell_str.split('\n'):
                            if 'Предмет:' in line:
                                discipline = line.split('Предмет:', 1)[1].strip()
                                if discipline:
                                    disciplines.append(discipline)

            if not disciplines:
                report += f"*Группа {group}*: Нет занятий в расписании.\n\n"
                continue

            counts = Counter(disciplines)

            report += f"*Группа {group}*:\n"
            group_total = 0
            for disc, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
                report += f"• {disc}: *{count} пар*\n"
                group_total += count
                overall_total += count

            report += f"Всего пар в группе: *{group_total}*\n\n"

        if overall_total == 0:
            report += "Нет данных о занятиях в загруженном файле.\n"

        report += f"*Общее количество пар по всем группам: {overall_total}*"

        await send_and_store(update, context, report, parse_mode='Markdown', metadata={'type': 'schedule'})

        # Очистка флага обработки
        context.user_data.pop('processing_schedule', None)

    except Exception as e:
        logger.exception("Ошибка при обработке файла расписания")
        await update.message.reply_text("❌ Произошла ошибка при обработке файла. Попробуйте снова.")