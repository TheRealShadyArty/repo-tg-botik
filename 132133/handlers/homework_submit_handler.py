"""Обработчик отчета по сданным домашним заданиям"""
import logging
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes
from .report_store import send_and_store

logger = logging.getLogger(__name__)

async def start_homework_submit_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск отчета по сдаче ДЗ"""
    await update.callback_query.edit_message_text(
        "📝 Загрузите файл сданных домашних заданий (Excel).\n"
        "Файл должен содержать информацию по студентам,\n"
        "группам и проценту выполненных заданий."
    )

async def process_homework_submit_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    """Обработка файла сданных ДЗ"""
    try:
        df = pd.read_excel(file_path)

        if df.shape[1] < 2:
            target = update.message if getattr(update, 'message', None) else update.callback_query
            await (target.reply_text("❌ Файл должен содержать минимум 2 колонки.") if hasattr(target, 'reply_text') else None)
            return

        columns = df.columns.tolist()

        # helper to normalize multiindex/tuple columns
        def col_to_str(c):
            if isinstance(c, tuple):
                return " ".join([str(x).strip() for x in c if str(x).strip()])
            return str(c).strip()

        cols_lower = [col_to_str(c).lower() for c in columns]

        # Log all columns for debugging
        logger.info(f"Total columns found: {len(columns)}")
        for i, col in enumerate(columns):
            logger.info(f"  Column {i}: '{col_to_str(col)}'")

        # detect ФИО/student column
        student_idx = None
        for i, c in enumerate(cols_lower):
            if any(k in c for k in ['фио', 'студент', 'имя', 'name']):
                student_idx = i
                break
        if student_idx is None:
            student_idx = 0
        logger.info(f"Student column: idx={student_idx}, name='{col_to_str(columns[student_idx])}'")

        # detect group column
        group_idx = None
        for i, c in enumerate(cols_lower):
            if any(k in c for k in ['группа', 'group']):
                group_idx = i
                break
        logger.info(f"Group column: idx={group_idx}, name='{col_to_str(columns[group_idx]) if group_idx is not None else 'N/A'}'")

        # detect percentage homework column - FIRST occurrence with both keywords
        percentage_idx = None
        for i, c in enumerate(cols_lower):
            # Look for columns containing both 'percentage' and 'homework'
            if 'percentage' in c and 'homework' in c:
                percentage_idx = i
                logger.info(f"Found 'Percentage Homework' at index {i}: '{col_to_str(columns[i])}'")
                break
        
        if percentage_idx is None:
            logger.warning("'Percentage Homework' not found with both keywords, searching for 'percentage' only")
            for i, c in enumerate(cols_lower):
                if 'percentage' in c:
                    percentage_idx = i
                    logger.info(f"Found 'percentage' at index {i}: '{col_to_str(columns[i])}'")
                    break

        if percentage_idx is None:
            logger.error("Could not find percentage column!")
            msg = "❌ Не удалось найти колонку 'Percentage Homework' в файле."
            if getattr(update, 'message', None) and update.message:
                await update.message.reply_text(msg)
            elif getattr(update, 'callback_query', None) and update.callback_query:
                await update.callback_query.edit_message_text(msg)
            return

        logger.info(f"Using percentage column: idx={percentage_idx}, name='{col_to_str(columns[percentage_idx])}'")


        problem_students = []

        for idx, row in df.iterrows():
            try:
                name = row[columns[student_idx]]
                if pd.isna(name):
                    continue
                name = str(name).strip()

                group = ""
                if group_idx is not None:
                    group_val = row[columns[group_idx]]
                    if pd.notna(group_val):
                        group = str(group_val).strip()

                # parse percentage
                if percentage_idx is not None:
                    pct_raw = row[columns[percentage_idx]]
                    if pd.isna(pct_raw):
                        continue
                    pct_str = str(pct_raw).strip().replace('\xa0', '').replace(',', '.').replace('%', '')
                    try:
                        pct = float(pct_str)
                        # handle 0-1 as fraction
                        if 0.0 <= pct <= 1.0:
                            pct *= 100.0

                        # Log for first few entries to verify parsing
                        if idx < 5:
                            logger.info(f"Row {idx}: name='{name}', group='{group}', pct_raw='{pct_raw}', pct_parsed={pct}")

                        if pct < 70.0:
                            problem_students.append({
                                'name': name,
                                'group': group,
                                'percentage': pct
                            })
                    except ValueError as e:
                        logger.warning(f"Row {idx}: Failed to parse percentage from '{pct_raw}': {e}")
                        continue

            except Exception as e:
                logger.warning(f"Row {idx}: Error processing row: {e}")
                continue

        logger.info(f"Found {len(problem_students)} students with <70% homework")

        # sort by percentage ascending
        problem_students.sort(key=lambda x: x['percentage'])

        # format report
        lines = ["📝 Отчет по сданным домашним заданиям:"]
        if problem_students:
            lines.append(f"⚠️ Студентов с выполнением < 70%: {len(problem_students)}")
            for s in problem_students:
                group_text = f" ({s['group']})" if s['group'] else ""
                lines.append(f"• {s['name']}{group_text}: {s['percentage']:.1f}%")
        else:
            lines.append("✅ Все студенты выполнили ≥ 70% заданий.")

        # join and split by 4000 char limit
        text = "\n".join(lines)
        
        # Telegram limit is 4096 chars, so we split at ~3500 to be safe
        max_len = 3500
        if len(text) <= max_len:
            messages = [text]
        else:
            messages = []
            current = ""
            for line in lines:
                if len(current) + len(line) + 1 > max_len:
                    if current:
                        messages.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                messages.append(current)

        # send response
        # send messages and store last sent one
        if getattr(update, 'message', None) and update.message:
            last = None
            for msg in messages:
                last = await send_and_store(update, context, msg, parse_mode=None, metadata={'type': 'homework_submit'})
        elif getattr(update, 'callback_query', None) and update.callback_query:
            last = await send_and_store(update, context, messages[0], parse_mode=None, metadata={'type': 'homework_submit'})

    except Exception:
        logger.exception("Ошибка при обработке файла сданных ДЗ")
        error_msg = "❌ Ошибка обработки файла. Подробности в логах."
        try:
            if getattr(update, 'message', None) and update.message:
                await update.message.reply_text(error_msg)
            elif getattr(update, 'callback_query', None) and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
        except Exception as send_error:
            logger.exception("Ошибка при отправке сообщения об ошибке")

