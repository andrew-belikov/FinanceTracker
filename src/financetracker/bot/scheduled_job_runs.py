"""Durable lease lifecycle for scheduled bot jobs.

The caller supplies runtime dependencies so the module stays independent from
the composition facade in :mod:`financetracker.bot.jobs`.
"""

from __future__ import annotations

import uuid


def claim_scheduled_job_run(
    *,
    db_session,
    claim_run,
    logger,
    job_name: str,
    run_date,
    trigger_source: str,
    scheduled_for: str,
) -> tuple[bool, bool, str | None]:
    """Claim a run, preserving migration-fallback and duplicate semantics."""
    attempt_id = uuid.uuid4().hex
    with db_session() as session:
        run_claimed = claim_run(
            session,
            job_name=job_name,
            run_date=run_date,
            attempt_id=attempt_id,
        )

    if run_claimed is False:
        logger.info(
            "daily_job_already_processed",
            "Daily job already processed for this date; skipping duplicate run.",
            {"today": run_date.isoformat(), "scheduled_for": scheduled_for,
             "trigger_source": trigger_source, "job_name": job_name},
        )
        return False, True, None
    if run_claimed is None:
        logger.warning(
            "daily_job_tracking_unavailable",
            "Daily job run tracking is unavailable because migration is not applied.",
            {"today": run_date.isoformat(), "trigger_source": trigger_source,
             "job_name": job_name},
        )
        return False, False, None
    return True, True, attempt_id


def heartbeat_scheduled_job_run(
    *, db_session, heartbeat_run, tracking_available: bool, job_name: str, run_date, attempt_id: str | None
) -> bool:
    if not tracking_available or attempt_id is None:
        return False
    with db_session() as session:
        return heartbeat_run(session, job_name=job_name, run_date=run_date, attempt_id=attempt_id) is True


def finalize_scheduled_job_run(
    *,
    db_session,
    complete_run,
    release_run,
    logger,
    tracking_available: bool,
    attempt_id: str | None,
    job_name: str,
    run_date,
    trigger_source: str,
    sent_total: int,
    failed_total: int,
) -> None:
    """Complete on full success, otherwise release the fenced claim for retry."""
    if not tracking_available or attempt_id is None:
        return
    with db_session() as session:
        if failed_total > 0:
            released = release_run(session, job_name=job_name, run_date=run_date, attempt_id=attempt_id)
            logger.warning(
                "daily_job_run_released_for_retry",
                "Released scheduled job claim because at least one intended delivery failed.",
                {"today": run_date.isoformat(), "trigger_source": trigger_source,
                 "sent_total": sent_total, "failed_total": failed_total,
                 "job_name": job_name, "released": bool(released)},
            )
        elif not complete_run(
            session, job_name=job_name, run_date=run_date, attempt_id=attempt_id,
            sent_total=sent_total, failed_total=failed_total,
        ):
            logger.warning(
                "daily_job_run_fence_rejected",
                "Scheduled job completion was rejected because the lease owner changed.",
                {"today": run_date.isoformat(), "job_name": job_name},
            )
