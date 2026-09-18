"""Message-level parser for audited historical GNU mbox shards.

Direct evidence requires the message's own Date header and HTTP(S) URLs extracted only
from MIME text bodies. Monthly shard names are an independent consistency
check; they never substitute for a missing or malformed message timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
import re
from urllib.parse import urlsplit

from creeper.sources.non_snapshot import extract_http_urls, mailbox_year_from_locator


_MBOX_BOUNDARY_RE = re.compile(br"(?m)^From [^\r\n]*(?:\r?\n)")
_MAX_URLS_PER_MESSAGE = 64


@dataclass(frozen=True)
class MboxMessageRecord:
    urls: tuple[str, ...]
    source_time: str
    year: int
    message_index: int


def is_audited_gnu_mbox_locator(locator: str) -> bool:
    parsed = urlsplit(locator)
    if parsed.scheme.lower() != "https":
        return False
    if (parsed.hostname or "").lower() != "lists.gnu.org":
        return False
    if parsed.query or parsed.fragment:
        return False
    path = parsed.path.lower()
    if not path.startswith("/archive/mbox/"):
        return False
    name = path.rstrip("/").rsplit("/", 1)[-1]
    # Keep GNU authority forms frozen even if generic mailbox discovery later
    # learns additional institutional suffixes such as IETF .mail/.mail.gz.
    if name.endswith((".mail", ".mail.gz")):
        return False
    return mailbox_year_from_locator(locator) is not None



def _message_text_parts(message) -> tuple[str, ...]:
    parts: list[str] = []

    def visit(part) -> None:
        # Attachment authority is subtree-scoped. In particular, a
        # message/rfc822 attachment may contain ordinary text/* children whose
        # own Content-Disposition is empty; walking flat would accidentally
        # promote URLs from the attached/forwarded message.
        if (
            part.get_content_disposition() == "attachment"
            or part.get_filename() is not None
        ):
            return
        if part.is_multipart():
            for child in part.iter_parts():
                visit(child)
            return
        if part.get_content_maintype() != "text":
            return
        try:
            value = part.get_content()
        except (LookupError, UnicodeError, ValueError):
            raw = part.get_payload(decode=True)
            if raw is None:
                return
            charset = part.get_content_charset() or "utf-8"
            try:
                value = raw.decode(charset, errors="replace")
            except LookupError:
                value = raw.decode("utf-8", errors="replace")
        if isinstance(value, str):
            parts.append(value)

    visit(message)
    return tuple(parts)


def parse_mbox_messages(
    payload: bytes,
    *,
    locator: str,
    allow_truncated_tail: bool = False,
    max_urls_per_message: int = _MAX_URLS_PER_MESSAGE,
) -> tuple[MboxMessageRecord, ...]:
    """Parse dated URL observations from complete mbox messages.

    If allow_truncated_tail is true, only the final message is discarded;
    every earlier message is bounded by a following mbox envelope line.
    """
    if max_urls_per_message < 1 or max_urls_per_message > _MAX_URLS_PER_MESSAGE:
        raise ValueError("max_urls_per_message must be within [1, 64]")

    shard_year = mailbox_year_from_locator(locator)
    if shard_year is None:
        return ()

    matches = list(_MBOX_BOUNDARY_RE.finditer(payload))
    if not matches:
        return ()
    rows: list[MboxMessageRecord] = []
    parser = BytesParser(policy=policy.default)
    last_index = len(matches) - 1

    for message_index, match in enumerate(matches):
        if allow_truncated_tail and message_index == last_index:
            break
        start = match.end()
        end = (
            matches[message_index + 1].start()
            if message_index < last_index
            else len(payload)
        )
        if end <= start:
            continue
        try:
            message = parser.parsebytes(payload[start:end])
        except (ValueError, TypeError):
            continue
        raw_dates = message.get_all("Date", [])
        if len(raw_dates) != 1:
            continue
        date_text = str(raw_dates[0]).strip()
        try:
            parsed_date = parsedate_to_datetime(date_text)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed_date is None or parsed_date.year != shard_year:
            continue
        if not 1996 <= parsed_date.year <= 2001:
            continue

        urls: list[str] = []
        seen: set[str] = set()
        for body in _message_text_parts(message):
            for url in extract_http_urls(
                body,
                max_urls=max_urls_per_message,
            ):
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
                if len(urls) >= max_urls_per_message:
                    break
            if len(urls) >= max_urls_per_message:
                break
        if not urls:
            continue
        rows.append(
            MboxMessageRecord(
                urls=tuple(urls),
                source_time=date_text,
                year=parsed_date.year,
                message_index=message_index,
            )
        )
    return tuple(rows)
