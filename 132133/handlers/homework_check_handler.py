"""Обработчик отчета по проверке домашних заданий"""
import logging
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

async def start_homework_check_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск отчета по проверке ДЗ - выбор периода"""
    keyboard = [
        [
            InlineKeyboardButton("📅 За месяц", callback_data="hw_check_month"),
            InlineKeyboardButton("📆 За неделю", callback_data="hw_check_week"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        "✅ Выберите период для проверки домашних заданий:",
        reply_markup=reply_markup
    )

async def handle_hw_check_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка выбора периода (месяц/неделя)"""
    query = update.callback_query
    await query.answer()
    
    period = "month" if query.data == "hw_check_month" else "week"
    period_text = "месяц" if period == "month" else "неделю"
    
    # сохраняем выбор в контексте
    context.user_data['hw_check_period'] = period
    
    await query.edit_message_text(
        f"✅ Вы выбрали проверку за {period_text}.\n\n"
        "Теперь загрузите файл проверки домашних заданий (Excel).\n"
        "Файл должен содержать информацию по преподавателям и проверенным заданиям."
    )

async def process_homework_check_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str) -> None:
    """Обработка файла проверки ДЗ"""
    try:
        # Try several header parsing strategies to handle files with multi-row headers
        # Prefer MultiIndex header ([0,1]) that contains period labels like 'месяц' or 'недел'
        tried = []
        df = None
        columns = None
        # try MultiIndex header first
        for hdr in [[0, 1], None, 1]:
            try:
                if hdr is None:
                    tmp = pd.read_excel(file_path)
                else:
                    tmp = pd.read_excel(file_path, header=hdr)
                cols = tmp.columns.tolist()
                # flatten tuple columns to string for checking
                def col_to_str_check(c):
                    if isinstance(c, tuple):
                        return " ".join([str(x).strip() for x in c if str(x).strip()])
                    return str(c).strip()
                cols_lower = [col_to_str_check(c).lower() for c in cols]
                tried.append((hdr, cols_lower))
                # prefer parses that include explicit period labels
                has_keywords = any('получ' in c for c in cols_lower) and any('провер' in c for c in cols_lower)
                has_period = any('месяц' in c or 'недел' in c or 'неделя' in c for c in cols_lower)
                if has_keywords and has_period:
                    df = tmp
                    columns = cols
                    break
                # otherwise accept first parse that at least has both keywords
                if df is None and has_keywords:
                    df = tmp
                    columns = cols
                    # but keep searching for a parse with explicit periods
            except Exception:
                continue

        # if still not found, fallback to default read
        if df is None:
            df = pd.read_excel(file_path)
            columns = df.columns.tolist()

        # helper to normalize multiindex/tuple columns
        def col_to_str(c):
            if isinstance(c, tuple):
                return " ".join([str(x).strip() for x in c if str(x).strip()])
            return str(c).strip()

        cols_lower = [col_to_str(c).lower() for c in columns]

        # detect teacher column
        teacher_idx = None
        for i, c in enumerate(cols_lower):
            if any(k in c for k in ['преподават', 'учител', 'фио', 'преподав']):
                teacher_idx = i
                break
        if teacher_idx is None:
            teacher_idx = 0

        # detect issued (получено) and checked (проверено) columns and try to pair them by period
        issued_keywords = ['получ', 'получено']
        checked_keywords = ['провер', 'проверено']

        # attempt to detect period from column text (handles MultiIndex tuples and flattened headers)
        periods = {'month': {'issued': None, 'checked': None}, 'week': {'issued': None, 'checked': None}}
        other_issued = []
        other_checked = []
        for i, raw_col in enumerate(columns):
            text = col_to_str(raw_col).lower()
            is_issued = any(k in text for k in issued_keywords)
            is_checked = any(k in text for k in checked_keywords)
            # detect period if present in the same header cell
            period = None
            if 'месяц' in text:
                period = 'month'
            elif 'недел' in text or 'неделя' in text:
                period = 'week'

            if is_issued:
                if period:
                    periods[period]['issued'] = i
                else:
                    other_issued.append(i)
            if is_checked:
                if period:
                    periods[period]['checked'] = i
                else:
                    other_checked.append(i)

        # If some period parts are missing, try to pair by proximity: for each issued find nearest checked to the right
        def find_checked_for_issued(issued_idx, candidates):
            if not candidates:
                return None
            # prefer candidate to the right; pick nearest by absolute distance
            best = min(candidates, key=lambda x: abs(x - issued_idx))
            return best

        # ensure pairing for month and week using detected values or proximity from leftover lists
        if periods['month']['issued'] is None and other_issued:
            periods['month']['issued'] = other_issued[0]
        if periods['month']['checked'] is None and other_checked:
            # try to find checked near the month issued
            if periods['month']['issued'] is not None:
                periods['month']['checked'] = find_checked_for_issued(periods['month']['issued'], other_checked)
            else:
                periods['month']['checked'] = other_checked[0]

        if periods['week']['issued'] is None and len(other_issued) >= 2:
            periods['week']['issued'] = other_issued[1]
        elif periods['week']['issued'] is None and periods['month']['issued'] is not None and other_issued:
            # if only one other_issued remains, and month already used it, try to use next closest
            for idx in other_issued:
                if idx != periods['month']['issued']:
                    periods['week']['issued'] = idx
                    break

        if periods['week']['checked'] is None and len(other_checked) >= 2:
            periods['week']['checked'] = other_checked[1]
        elif periods['week']['checked'] is None and periods['month']['checked'] is not None and other_checked:
            for idx in other_checked:
                if idx != periods['month']['checked']:
                    periods['week']['checked'] = idx
                    break

        month_issued_idx = periods['month']['issued']
        month_checked_idx = periods['month']['checked']
        week_issued_idx = periods['week']['issued']
        week_checked_idx = periods['week']['checked']

        # if after all attempts we still don't have any 'получ' or 'провер' columns, inform user for debugging
        if not any(any(k in c for k in issued_keywords) for c in cols_lower) or not any(any(k in c for k in checked_keywords) for c in cols_lower):
            # prepare short diagnostics
            sample = cols_lower[:12]
            msg_lines = ["Не удалось автоматически определить колонки 'Получено' и/или 'Проверено'.", "Найденные заголовки:"]
            for i, c in enumerate(sample):
                msg_lines.append(f"{i}: {c}")
            msg_lines.append("Если хотите, пришлите первый лист xlsx или укажите номер строки заголовка.")
            text = "\n".join(msg_lines)
            if getattr(update, 'message', None) and update.message:
                await update.message.reply_text(text)
            elif getattr(update, 'callback_query', None) and update.callback_query:
                await update.callback_query.edit_message_text(text)
            return

        # Get selected period from context
        selected_period = context.user_data.get('hw_check_period', 'month')
        period_text = 'месяц' if selected_period == 'month' else 'неделю'
        
        # Choose which indices to use based on user selection
        if selected_period == 'month':
            issued_idx = month_issued_idx
            checked_idx = month_checked_idx
        else:
            issued_idx = week_issued_idx
            checked_idx = week_checked_idx

        problem_teachers = []

        for idx, row in df.iterrows():
            try:
                name = row[columns[teacher_idx]]
                if pd.isna(name):
                    continue
                name = str(name).strip()

                # Check only selected period
                if issued_idx is not None and checked_idx is not None:
                    issued_raw = row[columns[issued_idx]]
                    checked_raw = row[columns[checked_idx]]
                    issued = pd.to_numeric(str(issued_raw).strip().replace('\xa0', '').replace(',', '.'), errors='coerce')
                    checked = pd.to_numeric(str(checked_raw).strip().replace('\xa0', '').replace(',', '.'), errors='coerce')
                    if pd.notna(issued) and issued > 0 and pd.notna(checked):
                        pct = float(checked) / float(issued) * 100.0
                        if pct < 70.0:
                            problem_teachers.append({
                                'name': name,
                                'issued': int(issued),
                                'checked': int(checked),
                                'percentage': pct
                            })

            except Exception:
                continue

        # sort by percentage ascending
        problem_teachers.sort(key=lambda x: x['percentage'])

        # формируем сообщение
        lines = [f"✅ Отчет по проверке домашних заданий за {period_text}:"]
        if problem_teachers:
            lines.append(f"⚠️ Преподавателей с проверкой < 70%: {len(problem_teachers)}")
            for t in problem_teachers:
                lines.append(f"• {t['name']}: Получено {t['issued']} | Проверено {t['checked']} | {t['percentage']:.1f}%")
        else:
            lines.append(f"✅ Все преподаватели проверили ≥ 70% заданий за {period_text}.")

        text = "\n".join(lines)

        # отправляем ответ туда, откуда пришло сообщение
        if getattr(update, 'message', None) and update.message:
            await update.message.reply_text(text)
        elif getattr(update, 'callback_query', None) and update.callback_query:
            await update.callback_query.edit_message_text(text)

    except Exception:
        logger.exception("Ошибка при обработке файла проверки ДЗ")
        if getattr(update, 'message', None) and update.message:
            await update.message.reply_text("❌ Ошибка обработки файла. Подробности в логах.")
        elif getattr(update, 'callback_query', None) and update.callback_query:
            await update.callback_query.edit_message_text("❌ Ошибка обработки файла. Подробности в логах.")
