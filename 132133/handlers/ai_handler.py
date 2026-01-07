import os
import asyncio
import logging
import requests
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

logger = logging.getLogger(__name__)

# Mistral API конфиг
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY") or "r7JuVl8YKk8pfNPjCxnWzaPw6uNxYmdy"
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_ENDPOINT = os.getenv(
    "MISTRAL_ENDPOINT",
    "https://api.mistral.ai/v1/chat/completions",
)

# Кэш для сессии requests (переиспользуем HTTP connection)
_requests_session = None

def get_requests_session():
    """Возвращает кэшированную сессию requests для повторного использования соединений."""
    global _requests_session
    if _requests_session is None:
        _requests_session = requests.Session()
    return _requests_session

async def start_ai_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggered when user clicks the AI button. Ask for a question/prompt."""
    query = update.callback_query
    if query:
        await query.answer()
        await query.edit_message_text(
            "🤖 Выбран AI-помощник. Опишите задачу — кратко или подробно, а я постараюсь помочь."
        )
    else:
        await update.message.reply_text(
            "🤖 Выбран AI-помощник. Опишите задачу — кратко или подробно, а я постараюсь помочь."
        )

    # Mark the conversation state so file_handler or other handlers know we're in AI mode
    context.user_data["report_type"] = "ai"

async def process_ai_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Process the user's text prompt and forward it to Mistral API."""
    user_text = update.message.text.strip() if update.message and update.message.text else ""
    if not user_text:
        await update.message.reply_text("❗ Пожалуйста, напишите запрос текстом.")
        return "ai"

    # If the user replied to a message, try to parse the replied text on the fly
    reply_to = update.message.reply_to_message if update.message else None
    if reply_to:
        replied_text = ''
        if getattr(reply_to, 'text', None):
            replied_text = reply_to.text
        elif getattr(reply_to, 'caption', None):
            replied_text = reply_to.caption

        # Try to parse 'homework_check' style report from replied_text
        problems = []
        if replied_text:
            import re

            # Pattern to match lines like:
            # • Name: Получено 10 | Проверено 7 | 70.0%
            pattern = re.compile(
                r"^[\u2022\-\*\•]?\s*(?P<name>[^:\n]+):\s*[Пп]олучено\s*(?P<issued>[0-9]+)\s*\|\s*[Пп]роверено\s*(?P<checked>[0-9]+)\s*\|\s*(?P<pct>[0-9.,]+)%",
                re.MULTILINE,
            )

            for m in pattern.finditer(replied_text):
                try:
                    name = m.group('name').strip()
                    issued = int(m.group('issued'))
                    checked = int(m.group('checked'))
                    pct = float(m.group('pct').replace(',', '.'))
                    problems.append({'name': name, 'issued': issued, 'checked': checked, 'percentage': pct})
                except Exception:
                    continue

        # If we parsed problems from text, answer locally
        if problems:
            q = user_text.lower()
            # who checked the least
            if any(w in q for w in ['кто меньше', 'кто меньше всех', 'кто наименее', 'least', 'меньше всех провер']):
                worst = min(problems, key=lambda x: x.get('percentage', 100.0))
                await update.message.reply_text(
                    f"👎 Наименее проверял: {worst['name']} — {worst['checked']}/{worst['issued']} ({worst['percentage']:.1f}%)"
                )
                return 'ai'

            # top/bottom N
            if 'топ' in q or 'первые' in q or 'наиб' in q or 'лучше' in q:
                sorted_p = sorted(problems, key=lambda x: x.get('percentage', 0.0), reverse=True)
                n = 5
                lines = [f"Топ {n} преподавателей по % проверки:"]
                for t in sorted_p[:n]:
                    lines.append(f"• {t['name']}: {t['checked']}/{t['issued']} ({t['percentage']:.1f}%)")
                await update.message.reply_text('\n'.join(lines))
                return 'ai'

            # count of problem teachers
            if 'сколько' in q and ('преподав' in q or 'преподавателей' in q or 'сколько' in q):
                await update.message.reply_text(f"⚠️ Преподавателей с проблемой: {len(problems)}")
                return 'ai'

            # fallback: send parsed summary + user question to LLM
            sb = [f"Parsed report (from message):"]
            sb.append('Teachers with issues:')
            for t in problems[:50]:
                sb.append(f"{t['name']}: issued={t['issued']}, checked={t['checked']}, pct={t['percentage']:.1f}")
            sb.append('\nUser question: ' + user_text)
            prompt = '\n'.join(sb)

            await update.message.reply_text('🔎 Отправляю запрос в AI с контекстом отчёта...')
            try:
                loop = asyncio.get_event_loop()
                ai_reply = await loop.run_in_executor(None, _call_mistral, prompt)
            except Exception:
                logger.exception('Error calling Mistral API')
                await update.message.reply_text('❌ Ошибка при обращении к AI. Попробуйте позже.')
                return 'ai'

            if not ai_reply:
                await update.message.reply_text('❌ AI вернул пустой ответ.')
                return 'ai'

            max_len = 4000
            if len(ai_reply) > max_len:
                ai_reply = ai_reply[: max_len - 20] + '...'

            await update.message.reply_text(ai_reply)
            await update.message.reply_text(
                'Готово — выберите следующую опцию:', reply_markup=context.application.bot_data.get('main_keyboard')
            )
            context.user_data.clear()
            return ConversationHandler.END

        # if no structured parsing succeeded, but there is replied_text, forward it as context to LLM
        if replied_text:
            prompt = f"Контекст (сообщение):\n{replied_text}\n\nВопрос пользователя: {user_text}"
            await update.message.reply_text('🔎 Отправляю запрос в AI с контекстом сообщения...')
            try:
                loop = asyncio.get_event_loop()
                ai_reply = await loop.run_in_executor(None, _call_mistral, prompt)
            except Exception:
                logger.exception('Error calling Mistral API')
                await update.message.reply_text('❌ Ошибка при обращении к AI. Попробуйте позже.')
                return 'ai'

            if not ai_reply:
                await update.message.reply_text('❌ AI вернул пустой ответ.')
                return 'ai'

            max_len = 4000
            if len(ai_reply) > max_len:
                ai_reply = ai_reply[: max_len - 20] + '...'

            await update.message.reply_text(ai_reply)
            await update.message.reply_text(
                'Готово — выберите следующую опцию:', reply_markup=context.application.bot_data.get('main_keyboard')
            )
            context.user_data.clear()
            return ConversationHandler.END

    # General fallback: send the plain text user query to Mistral
    await update.message.reply_text('🔎 Отправляю запрос в AI, ожидайте...')

    try:
        # asyncio.to_thread is available in Python 3.9+; use run_in_executor for compatibility
        loop = asyncio.get_event_loop()
        ai_reply = await loop.run_in_executor(None, _call_mistral, user_text)
    except Exception as e:
        logger.exception('Error calling Mistral API')
        await update.message.reply_text('❌ Ошибка при обращении к AI. Попробуйте позже.')
        return 'ai'

    if not ai_reply:
        await update.message.reply_text('❌ AI вернул пустой ответ.')
        return 'ai'

    # Send the AI response (truncate if very long)
    max_len = 4000
    if len(ai_reply) > max_len:
        ai_reply = ai_reply[: max_len - 20] + '...'

    await update.message.reply_text(ai_reply)

    # Return to main menu
    await update.message.reply_text(
        "Готово — выберите следующую опцию:", reply_markup=context.application.bot_data.get("main_keyboard")
    )
    context.user_data.clear()
    return ConversationHandler.END


async def process_ai_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Handle uploaded Excel documents (.xls/.xlsx), extract tables and send to Mistral."""
    document = update.message.document if update.message else None
    if not document:
        await update.message.reply_text("❗ Пожалуйста, загрузите файл Excel (.xls или .xlsx).")
        return "ai"

    filename = document.file_name or "file"
    lower = filename.lower()
    if not lower.endswith((".xls", ".xlsx")):
        await update.message.reply_text("❗ Поддерживаются только файлы .xls или .xlsx для анализа.")
        return "ai"

    await update.message.reply_text("📥 Файл получен, скачиваю и анализирую...")

    # Use caption (if provided) as user's instruction/prompt for the analysis
    user_caption = update.message.caption.strip() if update.message and update.message.caption else ""

    try:
        file_obj = await document.get_file()
        temp_path = f"temp_{document.file_id}_{filename}"
        await file_obj.download_to_drive(temp_path)

        try:
            xls = pd.read_excel(temp_path, sheet_name=None)
        except Exception as e:
            raise RuntimeError(f"Не удалось прочитать Excel: {e}")

        parts = []
        for sheet_name, df in xls.items():
            parts.append(f"--- Sheet: {sheet_name} ---")
            try:
                csv = df.to_csv(index=False)
            except Exception:
                csv = df.astype(str).to_csv(index=False)
            parts.append(csv)

        content = "\n".join(parts)
        instruction = (
            "Пользователь загрузил Excel-файл. Проанализируй таблицы и дай краткое резюме, "
            "выдели ключевые столбцы/строки, возможные аномалии, агрегаты и рекомендации.\n\n"
        )
        doc_label = "Excel"

        # Truncate content if too large
        max_content = 15000
        if len(content) > max_content:
            content_snippet = content[: max_content - 200] + "\n... (truncated)"
        else:
            content_snippet = content

        # Prepend user caption to the prompt so the user can give instructions via file caption
        if user_caption:
            prompt = f"Задача от пользователя: {user_caption}\n\n" + instruction + f"{doc_label} START:\n" + content_snippet + f"\n{doc_label} END:\nОтвечай подробно, но лаконично."
        else:
            prompt = instruction + f"{doc_label} START:\n" + content_snippet + f"\n{doc_label} END:\nОтвечай подробно, но лаконично."

        loop = asyncio.get_event_loop()
        ai_reply = await loop.run_in_executor(None, _call_mistral, prompt)

        if not ai_reply:
            await update.message.reply_text("❌ AI вернул пустой ответ.")
            return "ai"

        await update.message.reply_text(ai_reply)

    except Exception as e:
        logger.exception("Error calling Mistral API for file")
        await update.message.reply_text(f"❌ Ошибка при анализе файла: {e}")
        return "ai"
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass

    await update.message.reply_text(
        "Готово — выберите следующую опцию:", reply_markup=context.application.bot_data.get("main_keyboard")
    )
    context.user_data.clear()
    return ConversationHandler.END

def _call_mistral(prompt: str) -> str:
    """Blocking call to Mistral HTTP API. Returns text or empty string on failure."""
    session = get_requests_session()
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": MISTRAL_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.6,
        "max_tokens": 512,
    }

    try:
        resp = session.post(MISTRAL_ENDPOINT, json=data, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error when calling Mistral API: {e}")

    # Handle 404 specifically
    if resp.status_code == 404:
        body = resp.text.strip()
        raise RuntimeError(
            f"Mistral API returned 404 Not Found for URL {MISTRAL_ENDPOINT}. "
            "This usually means the model name or endpoint is incorrect. "
            "Please verify `MISTRAL_MODEL` or set a correct `MISTRAL_ENDPOINT` environment variable." +
            (f" Response: {body}" if body else "")
        )

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        body = resp.text.strip()
        raise RuntimeError(f"Mistral API error {resp.status_code}: {body or str(e)}")

    try:
        j = resp.json()
    except Exception:
        return resp.text or ""

    # Parse standard Mistral response
    if isinstance(j, dict) and "choices" in j and isinstance(j["choices"], list) and j["choices"]:
        choice = j["choices"][0]
        if isinstance(choice, dict) and "message" in choice and isinstance(choice["message"], dict):
            return choice["message"].get("content", "")

    # Fallback
    return j.get("message") if isinstance(j, dict) and "message" in j else ""