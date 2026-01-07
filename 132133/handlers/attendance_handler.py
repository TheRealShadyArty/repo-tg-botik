"""Обработчик отчета по посещаемости"""
import logging
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes
from .report_store import send_and_store

logger = logging.getLogger(__name__)

async def start_attendance_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск отчета по посещаемости"""
    text = (
        "📊 Загрузите файл посещаемости (Excel).\n"
        "Файл должен содержать информацию по преподавателям и их посещаемость."
    )
    # support both callback_query and normal messages
    if getattr(update, 'callback_query', None) and update.callback_query:
        await update.callback_query.edit_message_text(text)
    elif getattr(update, 'message', None) and update.message:
        await update.message.reply_text(text)

async def process_attendance_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    """Обработка файла посещаемости"""
    try:
        df = pd.read_excel(file_path)

        if df.shape[1] < 2:
            target = update.message if getattr(update, 'message', None) else update.callback_query
            await (target.reply_text("❌ Файл должен содержать минимум 2 колонки.") if hasattr(target, 'reply_text') else None)
            return

        columns = df.columns.tolist()

        # Ищем колонку с преподавателями и посещаемостью более надёжно
        teacher_col = None
        attendance_col = None

        attendance_keywords = ['посещ', 'сред', 'процент', '%', 'присут', 'avg']
        teacher_keywords = ['преподават', 'учител', 'фио', 'преподав']

        for col in columns:
            col_lower = str(col).lower()
            if any(k in col_lower for k in teacher_keywords):
                teacher_col = col
            if any(k in col_lower for k in attendance_keywords):
                attendance_col = col

        # fallback to first two columns
        if teacher_col is None:
            teacher_col = columns[0]
        if attendance_col is None:
            # try to find a numeric column further right
            if len(columns) > 1:
                attendance_col = columns[1]
            else:
                attendance_col = columns[0]

        # Попробуем привести колонку посещаемости к числам
        s = df[attendance_col].astype(str).fillna('').str.replace('\xa0', ' ')
        # удалить все кроме цифр, запятой, точек и минуса и процентного знака
        s_clean = s.str.replace(r"[^0-9,\.%-]", "", regex=True)
        # убрать % и привести запятые к точкам
        s_clean = s_clean.str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
        # привести к числу, невалидные -> NaN
        nums = pd.to_numeric(s_clean, errors='coerce')

        problem_teachers = []
        for idx, row in df.iterrows():
            try:
                name = row[teacher_col]
                if pd.isna(name):
                    continue
                name = str(name).strip()

                val = nums.iloc[idx]
                if pd.isna(val):
                    # skip rows without numeric attendance
                    continue
                attendance = float(val)
                # if value looks like fraction (0..1), treat as percent
                if 0.0 <= attendance <= 1.0:
                    attendance *= 100.0

                if attendance < 40.0:
                    problem_teachers.append((name, attendance))
            except Exception:
                continue

        # Сортировка по посещаемости (от меньшей к большей)
        problem_teachers.sort(key=lambda x: x[1])

        # Формирование простого текстового отчета
        lines = ["📊 Отчет по посещаемости преподавателей:"]
        if problem_teachers:
            lines.append(f"⚠️ Преподавателей с посещаемостью < 40%: {len(problem_teachers)}")
            for name, att in problem_teachers:
                lines.append(f"• {name}: {att:.1f}%")
        else:
            lines.append("✅ Все преподаватели имеют посещаемость ≥ 40%.")

        text = "\n".join(lines)

        # Ответить в том же месте, где пришло сообщение
        await send_and_store(update, context, text, parse_mode=None, metadata={'type': 'attendance'})

    except Exception:
        logger.exception("Ошибка при обработке файла посещаемости")
        if getattr(update, 'message', None) and update.message:
            await update.message.reply_text("❌ Ошибка обработки файла. Подробности в логах.")
        elif getattr(update, 'callback_query', None) and update.callback_query:
            await update.callback_query.edit_message_text("❌ Ошибка обработки файла. Подробности в логах.")
