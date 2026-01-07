import logging
from typing import Optional, Dict, Any
from telegram import Update, Message

logger = logging.getLogger(__name__)


async def send_and_store(
    update: Update,
    context,
    text: str,
    parse_mode: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Message]:
    """Send a message (reply or edit depending on update).

    NOTE: This helper no longer stores messages to `bot_data` to avoid memory growth.
    Returns the sent/edited Message when available.
    """
    sent_msg = None
    try:
        if getattr(update, 'callback_query', None) and update.callback_query:
            # Try to edit the existing message first
            try:
                sent = await update.callback_query.edit_message_text(text, parse_mode=parse_mode)
                # edit_message_text may return None in some PTB versions; fall back
                sent_msg = sent if sent is not None else update.callback_query.message
            except Exception:
                # fallback to sending a new message in the chat
                if update.callback_query.message:
                    sent_msg = await update.callback_query.message.reply_text(text, parse_mode=parse_mode)
        elif getattr(update, 'message', None) and update.message:
            sent_msg = await update.message.reply_text(text, parse_mode=parse_mode)

        # We intentionally do NOT store message text/metadata in bot_data here
        # to avoid unbounded memory growth on the host. If persistent storage
        # is needed later, use an external DB (Redis/SQLite/R2) with limits.

    except Exception:
        logger.exception('Failed to send or store message')

    return sent_msg
