"""
notifier.py — Gmail SMTP sender and IMAP reply poller.

Phase 7: Handles all Gmail communication for the approval gate.
  send_gmail()       — sends approval request email via SMTP TLS
  poll_gmail_reply() — polls IMAP inbox for APPROVE/REJECT reply
  send_cli_prompt()  — fallback when Gmail is not configured
"""

import os
import time
import imaplib
import smtplib
import email
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

GMAIL_ADDRESS      = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
GMAIL_SMTP_HOST    = os.getenv("GMAIL_SMTP_HOST", "smtp.gmail.com")
GMAIL_SMTP_PORT    = int(os.getenv("GMAIL_SMTP_PORT", "587"))
GMAIL_IMAP_HOST    = os.getenv("GMAIL_IMAP_HOST", "imap.gmail.com")
GMAIL_IMAP_PORT    = int(os.getenv("GMAIL_IMAP_PORT", "993"))


def send_gmail(subject: str, body: str) -> None:
    """Send an email from and to GMAIL_ADDRESS via SMTP TLS."""
    msg = MIMEMultipart()
    msg["From"] = GMAIL_ADDRESS
    msg["To"]   = GMAIL_ADDRESS
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, GMAIL_ADDRESS, msg.as_string())

    logger.info("Approval email sent: subject=%s", subject)


def poll_gmail_reply(subject_token: str, timeout_s: int, interval_s: int) -> str:
    """
    Poll Gmail IMAP inbox for a reply containing subject_token.

    Returns "APPROVED", "REJECTED", or "TIMED_OUT".

    LEARNING NOTE:
      We match on the unique token embedded in the subject line.
      Gmail reply subjects become "Re: [APPROVAL-XXXX] ..." so the token
      still appears in the subject — IMAP SUBJECT search finds it.
      We then scan the body for APPROVE or REJECT keywords.
    """
    deadline = time.time() + timeout_s
    seen_ids: set[bytes] = set()

    while time.time() < deadline:
        try:
            mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
            mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            mail.select("INBOX")

            _, data = mail.search(None, f'SUBJECT "{subject_token}"')
            msg_ids = data[0].split()

            for msg_id in msg_ids:
                if msg_id in seen_ids:
                    continue
                seen_ids.add(msg_id)

                _, msg_data = mail.fetch(msg_id, "(RFC822)")
                raw_msg = email.message_from_bytes(msg_data[0][1])

                # extract plain text body
                body_text = ""
                if raw_msg.is_multipart():
                    for part in raw_msg.walk():
                        if part.get_content_type() == "text/plain":
                            body_text = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                            break
                else:
                    body_text = raw_msg.get_payload(decode=True).decode("utf-8", errors="ignore")

                body_upper = body_text.upper()
                if "APPROVE" in body_upper:
                    mail.logout()
                    return "APPROVED"
                if "REJECT" in body_upper:
                    mail.logout()
                    return "REJECTED"

            mail.logout()

        except Exception as exc:
            logger.warning("IMAP poll error: %s", exc)

        time.sleep(interval_s)

    return "TIMED_OUT"


def send_cli_prompt(message: str) -> bool:
    """Fallback approval prompt — prints to terminal, reads y/n from stdin."""
    print("\n" + "=" * 60)
    print(message)
    print("=" * 60)
    try:
        answer = input("Approve? [y/N]: ").strip().lower()
        return answer in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False