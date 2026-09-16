"""Persistence helpers for stable asset identities across broker payloads."""

from __future__ import annotations

from datetime import datetime

from financetracker.common.time_utils import utc_now
from financetracker.database.models import AssetAlias, Instrument


def upsert_asset_alias(
    session,
    *,
    asset_uid: str | None,
    instrument_uid: str | None,
    figi: str | None,
    name: str | None,
    seen_at: datetime | None,
) -> None:
    if not asset_uid:
        return

    seen_at = seen_at or utc_now()
    instrument = (
        session.query(Instrument).filter(Instrument.figi == figi).one_or_none()
        if figi
        else None
    )
    ticker = instrument.ticker if instrument is not None else None
    display_name = name or (instrument.name if instrument is not None else None) or figi or asset_uid
    alias = (
        session.query(AssetAlias)
        .filter(
            AssetAlias.asset_uid == asset_uid,
            AssetAlias.instrument_uid == instrument_uid,
            AssetAlias.figi == figi,
        )
        .one_or_none()
    )
    if alias is None:
        session.add(
            AssetAlias(
                asset_uid=asset_uid,
                instrument_uid=instrument_uid,
                figi=figi,
                ticker=ticker,
                name=display_name,
                first_seen_at=seen_at,
                last_seen_at=seen_at,
            )
        )
        return

    alias.ticker = ticker or alias.ticker
    alias.name = display_name or alias.name
    if seen_at < alias.first_seen_at:
        alias.first_seen_at = seen_at
    if seen_at > alias.last_seen_at:
        alias.last_seen_at = seen_at
    alias.updated_at = utc_now()


def resolve_asset_uid_for_position(
    session,
    *,
    asset_uid: str | None,
    instrument_uid: str | None,
    figi: str | None,
) -> str | None:
    if asset_uid:
        return asset_uid

    alias = None
    if instrument_uid:
        alias = (
            session.query(AssetAlias)
            .filter(AssetAlias.instrument_uid == instrument_uid)
            .order_by(AssetAlias.last_seen_at.desc(), AssetAlias.id.desc())
            .first()
        )
    if alias is None and figi:
        alias = (
            session.query(AssetAlias)
            .filter(AssetAlias.figi == figi)
            .order_by(AssetAlias.last_seen_at.desc(), AssetAlias.id.desc())
            .first()
        )
    return alias.asset_uid if alias is not None else None
