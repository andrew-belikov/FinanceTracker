"""Recipient-level durable notification delivery lifecycle."""

from __future__ import annotations

import uuid

from telegram.error import BadRequest, NetworkError


async def send_tracked_notification(
    *,
    db_session,
    claim_delivery,
    complete_delivery,
    get_delivery_status,
    mark_uncertain,
    release_delivery,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    send,
    reclaim_stale: bool = True,
) -> bool:
    """Send once under a lease; network outcomes remain deliberately uncertain."""
    attempt_id = uuid.uuid4().hex
    with db_session() as session:
        claimed = claim_delivery(
            session, notification_kind=notification_kind, notification_key=notification_key,
            chat_id=chat_id, message_type=message_type, attempt_id=attempt_id,
            reclaim_stale=reclaim_stale,
        )
    if claimed is None:
        raise RuntimeError("Notification delivery ledger migration is unavailable")
    if not claimed:
        with db_session() as session:
            delivery_status = get_delivery_status(
                session, notification_kind=notification_kind, notification_key=notification_key,
                chat_id=chat_id, message_type=message_type,
            )
        if delivery_status == "sent":
            return False
        if delivery_status == "uncertain":
            if reclaim_stale:
                raise RuntimeError("Notification delivery has an ambiguous outcome")
            return False
        raise RuntimeError("Notification delivery is owned by another active worker")

    try:
        await send()
    except Exception as exc:
        with db_session() as session:
            if isinstance(exc, NetworkError) and not isinstance(exc, BadRequest):
                mark_uncertain(
                    session, notification_kind=notification_kind, notification_key=notification_key,
                    chat_id=chat_id, message_type=message_type, attempt_id=attempt_id,
                )
            else:
                release_delivery(
                    session, notification_kind=notification_kind, notification_key=notification_key,
                    chat_id=chat_id, message_type=message_type, attempt_id=attempt_id,
                )
        raise

    with db_session() as session:
        completed = complete_delivery(
            session, notification_kind=notification_kind, notification_key=notification_key,
            chat_id=chat_id, message_type=message_type, attempt_id=attempt_id,
        )
    if not completed:
        raise RuntimeError("Notification delivery lease was lost before completion")
    return True


def notification_deliveries_are_complete(
    *, db_session, deliveries_complete, target_chat_ids, notification_kind: str,
    notification_key: str, message_types: set[str]
) -> bool:
    with db_session() as session:
        completed = deliveries_complete(
            session, notification_kind=notification_kind, notification_key=notification_key,
            chat_ids=set(target_chat_ids), message_types=message_types,
        )
    return completed is True
