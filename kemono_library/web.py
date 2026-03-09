from __future__ import annotations

import html
import hashlib
import json
import re
import shutil
import sqlite3
import stat
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import bleach
from bs4 import BeautifulSoup
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, send_from_directory, url_for
from markupsafe import Markup

from .db import LibraryDB

LibraryDBLike = Any
from .kemono import (
    KemonoPostRef,
    creator_icon_url,
    download_attachment,
    download_creator_icon,
    extract_attachments,
    fetch_post_json,
    normalize_post_payload,
    parse_kemono_post_url,
    sanitize_filename,
    to_absolute_kemono_url,
)
from .rendering import render_post_content


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY="dev-local-secret",
        DATABASE=str(Path(app.root_path).parent / "data" / "library.db"),
        FILES_DIR=str(Path(app.root_path).parent / "data" / "files"),
        ICONS_DIR=str(Path(app.root_path).parent / "data" / "icons"),
    )
    if test_config:
        app.config.update(test_config)

    db = LibraryDB(app.config["DATABASE"])
    db.init_schema()
    Path(app.config["FILES_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["ICONS_DIR"]).mkdir(parents=True, exist_ok=True)
    app.db = db  # type: ignore[attr-defined]
    import_jobs: dict[str, dict[str, Any]] = {}
    import_jobs_lock = threading.Lock()
    import_job_queue: deque[str] = deque()
    import_job_queue_condition = threading.Condition(import_jobs_lock)
    attachment_retry_jobs: dict[str, dict[str, Any]] = {}
    attachment_retry_jobs_lock = threading.Lock()
    attachment_retry_job_queue: deque[str] = deque()
    attachment_retry_job_queue_condition = threading.Condition(attachment_retry_jobs_lock)

    def refresh_import_queue_positions_locked() -> None:
        for position, queued_job_id in enumerate(import_job_queue, start=1):
            queued_job = import_jobs.get(queued_job_id)
            if not queued_job:
                continue
            queued_job.update(
                {
                    "status": "queued",
                    "queue_position": position,
                    "message": f"Queued import job. Queue position: {position}.",
                }
            )

    def import_worker() -> None:
        while True:
            with import_job_queue_condition:
                while not import_job_queue:
                    import_job_queue_condition.wait()
                job_id = import_job_queue.popleft()
                job = import_jobs.get(job_id)
                if job:
                    job.update(
                        {
                            "status": "running",
                            "queue_position": 0,
                            "message": "Fetching post payload...",
                        }
                    )
                refresh_import_queue_positions_locked()

            if not job:
                continue
            job_payload = dict(job["payload"])

            def progress_callback(completed: int, total: int, current_file: str | None) -> None:
                if total <= 0:
                    message = "Saving post content and metadata..."
                elif completed >= total:
                    message = f"Finalizing import ({completed}/{total})..."
                elif current_file:
                    message = f"Downloading {completed}/{total}: {current_file}"
                else:
                    message = f"Downloading {completed}/{total} files..."
                with import_jobs_lock:
                    live_job = import_jobs.get(job_id)
                    if not live_job:
                        return
                    live_job.update(
                        {
                            "status": "running",
                            "queue_position": 0,
                            "message": message,
                            "completed": completed,
                            "total": total,
                            "current_file": current_file,
                        }
                    )

            try:
                local_post_id, imported_version_id = _import_post_into_library(
                    db,
                    files_base=Path(app.config["FILES_DIR"]),
                    icons_base=Path(app.config["ICONS_DIR"]),
                    creator_id=int(job_payload["creator_id"]),
                    series_id=job_payload["series_id"],
                    service=str(job_payload["service"]),
                    user_id=str(job_payload["user_id"]),
                    post_id=str(job_payload["post_id"]),
                    import_target_mode=str(job_payload["import_target_mode"]),
                    target_post_id=job_payload.get("target_post_id"),
                    overwrite_matching_version=bool(job_payload["overwrite_matching_version"]),
                    set_as_default=bool(job_payload["set_as_default"]),
                    version_label=job_payload.get("version_label"),
                    version_language=job_payload.get("version_language"),
                    requested_title=job_payload.get("requested_title"),
                    requested_content=job_payload.get("requested_content"),
                    requested_published_at=job_payload.get("requested_published_at"),
                    requested_edited_at=job_payload.get("requested_edited_at"),
                    requested_next_external_post_id=job_payload.get("requested_next_external_post_id"),
                    requested_prev_external_post_id=job_payload.get("requested_prev_external_post_id"),
                    tags_text=job_payload.get("tags_text"),
                    field_presence=dict(job_payload["field_presence"]),
                    selected_attachment_indices=set(job_payload["selected_attachment_indices"]),
                    progress_callback=progress_callback,
                )
                with import_jobs_lock:
                    live_job = import_jobs.get(job_id)
                    if live_job:
                        total = int(live_job.get("total") or 0)
                        completed = int(live_job.get("completed") or 0)
                        live_job.update(
                            {
                                "status": "completed",
                                "queue_position": 0,
                                "message": "Import complete.",
                                "completed": max(completed, total),
                                "redirect_url": (
                                    f"/posts/{local_post_id}?version_id={imported_version_id}"
                                    if not bool(job_payload["set_as_default"])
                                    else f"/posts/{local_post_id}"
                                ),
                            }
                        )
            except Exception as exc:  # noqa: BLE001
                with import_jobs_lock:
                    live_job = import_jobs.get(job_id)
                    if live_job:
                        live_job.update(
                            {
                                "status": "failed",
                                "queue_position": 0,
                                "message": "Import failed.",
                                "error": str(exc),
                            }
                        )

    threading.Thread(target=import_worker, daemon=True).start()

    def _collect_retry_scope_rows(scope: str, scope_id_raw: str) -> list[dict[str, Any]]:
        files_base = Path(app.config["FILES_DIR"])
        source_rows = db.list_attachment_inventory()
        local_file_status = _collect_local_file_status_by_path(files_base, source_rows)
        inventory_rows: list[dict[str, Any]] = []
        for row in source_rows:
            local_path = _optional_str(row["local_path"])
            local_available, _ = local_file_status.get(local_path or "", (False, None))
            inventory_row = {
                "id": int(row["id"]),
                "post_id": int(row["post_id"]),
                "creator_id": int(row["creator_id"]),
                "series_id": int(row["series_id"]) if row["series_id"] is not None else None,
                "name": str(row["name"]),
                "remote_url": str(row["remote_url"]),
                "local_path": local_path,
                "local_available": local_available,
                "creator_name": _optional_str(row["creator_name"]),
                "post_title": _optional_str(row["post_title"]),
                "version_label": _optional_str(row["version_label"]),
            }
            inventory_row["display_name"] = _build_attachment_retry_display_name(inventory_row)
            inventory_rows.append(
                inventory_row
            )
        scoped_rows = _filter_retry_scope_rows(inventory_rows, scope=scope, scope_id_raw=scope_id_raw)
        return [row for row in scoped_rows if not row["local_available"]]

    def refresh_attachment_retry_queue_positions_locked() -> None:
        for position, queued_job_id in enumerate(attachment_retry_job_queue, start=1):
            queued_job = attachment_retry_jobs.get(queued_job_id)
            if not queued_job:
                continue
            queued_job.update(
                {
                    "status": "queued",
                    "queue_position": position,
                    "message": f"Queued retry job. Queue position: {position}.",
                }
            )

    def attachment_retry_worker() -> None:
        while True:
            with attachment_retry_job_queue_condition:
                while not attachment_retry_job_queue:
                    attachment_retry_job_queue_condition.wait()
                job_id = attachment_retry_job_queue.popleft()
                job = attachment_retry_jobs.get(job_id)
                if job:
                    job.update(
                        {
                            "status": "running",
                            "queue_position": 0,
                            "message": "Preparing attachment retry...",
                        }
                    )
                refresh_attachment_retry_queue_positions_locked()

            if not job:
                continue

            job_payload = dict(job["payload"])
            scope = str(job_payload["scope"])
            scope_id_raw = str(job_payload["scope_id_raw"])
            return_to = str(job_payload["return_to"])
            missing_rows = _collect_retry_scope_rows(scope, scope_id_raw)

            if not missing_rows:
                with attachment_retry_jobs_lock:
                    live_job = attachment_retry_jobs.get(job_id)
                    if live_job:
                        live_job.update(
                            {
                                "status": "completed",
                                "queue_position": 0,
                                "message": "No missing attachments matched this retry scope.",
                                "completed": 0,
                                "total": 0,
                                "current_file": None,
                                "success_count": 0,
                                "failure_count": 0,
                                "failure_examples": [],
                                "results": [],
                                "redirect_url": return_to,
                            }
                        )
                continue

            success_count = 0
            failure_count = 0
            failure_samples: list[str] = []
            retry_results: list[dict[str, Any]] = []
            total = len(missing_rows)
            files_base = Path(app.config["FILES_DIR"])

            for idx, row in enumerate(missing_rows, start=1):
                display_name = str(row.get("display_name") or row["name"])
                with attachment_retry_jobs_lock:
                    live_job = attachment_retry_jobs.get(job_id)
                    if live_job:
                        live_job.update(
                            {
                                "status": "running",
                                "queue_position": 0,
                                "message": f"Retrying attachments ({idx}/{total})...",
                                "completed": idx - 1,
                                "total": total,
                                "current_file": display_name,
                                "success_count": success_count,
                                "failure_count": failure_count,
                            }
                        )

                result = _retry_attachment_row(
                    db,
                    files_base=files_base,
                    attachment_id=int(row["id"]),
                    post_id=int(row["post_id"]),
                    attachment_name=row["name"],
                    remote_url=str(row["remote_url"]),
                    existing_local_path=_optional_str(row["local_path"]),
                )
                retry_results.append(
                    {
                        "id": int(row["id"]),
                        "success": bool(result["success"]),
                        "error": result["error"],
                        "local_path": result["local_path"],
                        "file_size": result["file_size"],
                    }
                )
                if bool(result["success"]):
                    success_count += 1
                else:
                    failure_count += 1
                    error = _optional_str(result["error"])
                    if error and len(failure_samples) < 3:
                        failure_samples.append(f"{row['name']}: {error}")

                with attachment_retry_jobs_lock:
                    live_job = attachment_retry_jobs.get(job_id)
                    if live_job:
                        live_job.update(
                            {
                                "status": "running",
                                "queue_position": 0,
                                "message": f"Processed {idx}/{total} attachment(s)...",
                                "completed": idx,
                                "total": total,
                                "current_file": display_name,
                                "success_count": success_count,
                                "failure_count": failure_count,
                            }
                        )

            summary_message = f"Retried {success_count} missing attachment(s)."
            if failure_count:
                detail = "; ".join(failure_samples)
                suffix = f" Examples: {detail}" if detail else ""
                summary_message += f" {failure_count} failed.{suffix}"
            with attachment_retry_jobs_lock:
                live_job = attachment_retry_jobs.get(job_id)
                if live_job:
                    live_job.update(
                        {
                            "status": "completed",
                            "queue_position": 0,
                            "message": summary_message,
                            "completed": total,
                            "total": total,
                            "current_file": None,
                            "success_count": success_count,
                            "failure_count": failure_count,
                            "failure_examples": failure_samples,
                            "results": retry_results,
                            "redirect_url": return_to,
                        }
                    )

    threading.Thread(target=attachment_retry_worker, daemon=True).start()

    @app.template_filter("format_datetime")
    def format_datetime_filter(value: Any) -> str:
        return _format_datetime_for_display(value)

    @app.template_filter("render_markdown")
    def render_markdown_filter(value: Any) -> Markup:
        return _render_markdown_snippet(value)

    @app.template_filter("format_bytes")
    def format_bytes_filter(value: Any) -> str:
        return _format_bytes_for_display(value)

    @app.get("/")
    def index():
        creators = db.list_creators()
        recent_posts = db.list_recent_posts()
        return render_template("index.html", creators=creators, recent_posts=recent_posts)

    @app.get("/attachments")
    def attachment_manager():
        files_base = Path(app.config["FILES_DIR"])
        search_text = request.args.get("q", "").strip()
        state_filter = request.args.get("state", "all").strip().lower()
        if state_filter not in {"all", "missing", "local"}:
            state_filter = "all"
        media_filter = request.args.get("media", "all").strip().lower()
        if media_filter not in {"all", "images", "other"}:
            media_filter = "all"
        sort_key = request.args.get("sort", "creator").strip().lower()
        if sort_key not in {"creator", "size", "name", "recent"}:
            sort_key = "creator"

        source_rows = db.list_attachment_inventory()
        local_file_status = _collect_local_file_status_by_path(files_base, source_rows)
        inventory_rows: list[dict[str, Any]] = []
        for row in source_rows:
            local_path = _optional_str(row["local_path"])
            local_available, file_size = local_file_status.get(local_path or "", (False, None))
            remote_url = str(row["remote_url"])
            is_image = _is_likely_image_attachment(
                remote_url=remote_url,
                name=row["name"],
                local_path=local_path,
                kind=row["kind"],
            )
            preview_url = (
                url_for("serve_file", relative_path=local_path)
                if local_available and local_path
                else _preferred_remote_url_for_access(remote_url, row["name"])
            )
            post_title = str(row["post_title"]).strip() if row["post_title"] else f"Post {int(row['post_id'])}"
            creator_name = str(row["creator_name"]).strip() if row["creator_name"] else f"Creator {int(row['creator_id'])}"
            series_name = _optional_str(row["series_name"]) or "Unsorted"
            version_label = str(row["version_label"]).strip() if row["version_label"] else "Version"
            version_language = _optional_str(row["version_language"])
            search_blob = " ".join(
                part
                for part in (
                    creator_name,
                    series_name,
                    post_title,
                    str(row["name"]),
                    remote_url,
                    version_label,
                    version_language or "",
                )
                if part
            ).lower()
            inventory_rows.append(
                {
                    "id": int(row["id"]),
                    "version_id": int(row["version_id"]),
                    "post_id": int(row["post_id"]),
                    "creator_id": int(row["creator_id"]),
                    "series_id": int(row["series_id"]) if row["series_id"] is not None else None,
                    "creator_name": creator_name,
                    "series_name": series_name,
                    "post_title": post_title,
                    "post_published_at": row["post_published_at"],
                    "name": str(row["name"]),
                    "remote_url": remote_url,
                    "local_path": local_path,
                    "local_available": local_available,
                    "file_size": file_size,
                    "kind": str(row["kind"]),
                    "is_image": is_image,
                    "preview_url": preview_url,
                    "version_label": version_label,
                    "version_language": version_language,
                    "origin_kind": str(row["origin_kind"]),
                    "is_default_version": bool(row["is_default_version"]),
                    "search_blob": search_blob,
                }
            )

        filtered_rows = _filter_attachment_inventory_rows(
            inventory_rows,
            search_text=search_text,
            state_filter=state_filter,
            media_filter=media_filter,
        )
        sorted_rows = _sort_attachment_inventory_rows(filtered_rows, sort_key=sort_key)
        tree = _build_attachment_inventory_tree(sorted_rows, sort_key=sort_key)
        summary = _summarize_attachment_inventory(sorted_rows)

        return render_template(
            "attachment_manager.html",
            attachment_tree=tree,
            attachment_summary=summary,
            search_text=search_text,
            state_filter=state_filter,
            media_filter=media_filter,
            sort_key=sort_key,
            request_query=request.query_string.decode("utf-8"),
        )

    @app.post("/creators")
    def create_creator():
        name = request.form.get("name", "").strip()
        if not name:
            flash("Creator name is required.", "error")
            return redirect(url_for("index"))
        creator_id = db.create_creator(name)
        flash("Creator saved.", "success")
        return redirect(url_for("creator_detail", creator_id=creator_id))

    @app.route("/creators/<int:creator_id>/edit", methods=["GET", "POST"])
    def edit_creator(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)

        if request.method == "POST":
            name = request.form.get("name", "").strip()
            description = request.form.get("description", "")
            tags_text = request.form.get("tags_text", "")
            if not name:
                flash("Creator name is required.", "error")
                return redirect(url_for("edit_creator", creator_id=creator_id))
            try:
                db.update_creator(
                    creator_id,
                    name=name,
                    description=description,
                    tags_text=tags_text,
                )
            except sqlite3.IntegrityError:
                flash("Creator name already exists.", "error")
                return redirect(url_for("edit_creator", creator_id=creator_id))
            flash("Creator updated.", "success")
            return redirect(url_for("creator_detail", creator_id=creator_id))

        header_context = _build_creator_header_context(creator=creator, selected_series=None)
        return render_template("creator_edit.html", creator=creator, header_context=header_context)

    @app.get("/creators/<int:creator_id>")
    def creator_detail(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)
        series_list = [dict(row) for row in db.list_series(creator_id)]
        for series in series_list:
            focus_x, focus_y = _extract_thumbnail_focus_from_raw_metadata(series.get("cover_metadata_json"))
            series["cover_thumbnail_focus_x"] = focus_x
            series["cover_thumbnail_focus_y"] = focus_y
        series_by_id = {int(series["id"]): series for series in series_list}

        requested_series_id = request.args.get("series_id", type=int)
        folder = request.args.get("folder", "").strip().lower()

        selected_series_id: int | None = None
        unsorted_only = False
        active_folder = "all"
        if folder == "unsorted":
            unsorted_only = True
            active_folder = "unsorted"
        elif requested_series_id is not None and requested_series_id in series_by_id:
            selected_series_id = requested_series_id
            active_folder = "series"

        selected_series = series_by_id.get(selected_series_id) if selected_series_id is not None else None
        series_default_sort = (
            str(selected_series.get("default_sort_by", "")).strip().lower()
            if selected_series and isinstance(selected_series.get("default_sort_by"), str)
            else ""
        )
        if series_default_sort not in {"published", "title"}:
            series_default_sort = "published"
        series_default_direction = (
            str(selected_series.get("default_sort_direction", "")).strip().lower()
            if selected_series and isinstance(selected_series.get("default_sort_direction"), str)
            else ""
        )
        if series_default_direction not in {"asc", "desc"}:
            series_default_direction = "desc"

        sort_by = request.args.get("sort", series_default_sort if selected_series else "published").strip().lower()
        sort_direction = request.args.get(
            "direction",
            series_default_direction if selected_series else "desc",
        ).strip().lower()

        if sort_by not in {"published", "title"}:
            sort_by = series_default_sort if selected_series else "published"
        if sort_direction not in {"asc", "desc"}:
            sort_direction = series_default_direction if selected_series else "desc"

        posts_rows = db.list_posts_for_creator(
            creator_id,
            series_id=selected_series_id,
            unsorted_only=unsorted_only,
            sort_by=sort_by,
            sort_direction=sort_direction,
        )
        posts: list[dict[str, Any]] = []
        for row in posts_rows:
            item = dict(row)
            focus_x, focus_y = _extract_thumbnail_focus_from_raw_metadata(item.get("metadata_json"))
            item["thumbnail_focus_x"] = focus_x
            item["thumbnail_focus_y"] = focus_y
            posts.append(item)
        series_thumbnail_options = (
            [
                {
                    "id": int(item["id"]),
                    "title": str(item["title"]) if item["title"] else f"Post {item['id']}",
                    "published_at": str(item["published_at"]) if item["published_at"] else "",
                }
                for item in posts
            ]
            if selected_series
            else []
        )
        header_context = _build_creator_header_context(creator=creator, selected_series=selected_series)
        return render_template(
            "creator_detail.html",
            creator=creator,
            series_list=series_list,
            posts=posts,
            selected_series=selected_series,
            series_thumbnail_options=series_thumbnail_options,
            active_folder=active_folder,
            sort_by=sort_by,
            sort_direction=sort_direction,
            header_context=header_context if selected_series else None,
        )

    @app.post("/creators/<int:creator_id>/series")
    def create_series(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "")
        tags_text = request.form.get("tags_text", "")
        if not name:
            flash("Series/group name is required.", "error")
            return redirect(url_for("creator_detail", creator_id=creator_id))
        db.create_series(
            creator_id,
            name,
            description=description,
            tags_text=tags_text,
        )
        flash("Series saved.", "success")
        return redirect(url_for("creator_detail", creator_id=creator_id))

    @app.post("/creators/<int:creator_id>/series/<int:series_id>")
    def update_series(creator_id: int, series_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)

        series = next((row for row in db.list_series(creator_id) if int(row["id"]) == series_id), None)
        if not series:
            return ("Series not found", 404)

        name = request.form.get("name", "").strip()
        description = request.form.get("description", "")
        tags_text = request.form.get("tags_text", "")
        default_sort_by = request.form.get("default_sort_by", "published").strip().lower()
        default_sort_direction = request.form.get("default_sort_direction", "desc").strip().lower()
        raw_cover_post_id = request.form.get("cover_post_id", "").strip()
        try:
            cover_post_id = int(raw_cover_post_id) if raw_cover_post_id else None
        except ValueError:
            cover_post_id = None
        if not name:
            flash("Series/group name is required.", "error")
            return redirect(url_for("creator_detail", creator_id=creator_id, series_id=series_id))
        if default_sort_by not in {"published", "title"}:
            default_sort_by = "published"
        if default_sort_direction not in {"asc", "desc"}:
            default_sort_direction = "desc"
        if cover_post_id is not None:
            cover_post = db.get_post(cover_post_id)
            if (
                not cover_post
                or int(cover_post["creator_id"]) != creator_id
                or cover_post["series_id"] is None
                or int(cover_post["series_id"]) != series_id
            ):
                flash("Selected cover post is not part of this series.", "error")
                return redirect(url_for("creator_detail", creator_id=creator_id, series_id=series_id))

        db.update_series(
            series_id,
            name=name,
            description=description,
            tags_text=tags_text,
            default_sort_by=default_sort_by,
            default_sort_direction=default_sort_direction,
            cover_post_id=cover_post_id,
        )
        flash("Series updated.", "success")
        return redirect(url_for("creator_detail", creator_id=creator_id, series_id=series_id))

    @app.post("/creators/<int:creator_id>/delete")
    def delete_creator(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)

        post_ids = db.list_post_ids_for_creator(creator_id)
        icon_local_path = creator["icon_local_path"]
        deleted = db.delete_creator(creator_id)
        if not deleted:
            return ("Creator not found", 404)

        files_root = Path(app.config["FILES_DIR"])
        for post_id in post_ids:
            shutil.rmtree(files_root / f"post_{post_id}", ignore_errors=True)

        if isinstance(icon_local_path, str) and icon_local_path.strip():
            icon_path = Path(app.config["ICONS_DIR"]) / icon_local_path
            if icon_path.exists():
                icon_path.unlink()

        flash("Creator deleted.", "success")
        return redirect(url_for("index"))

    @app.get("/import")
    def import_form():
        creators = db.list_creators()
        selected_creator = request.args.get("creator_id", type=int)
        selected_series = request.args.get("series_id", type=int)
        prefill_url = request.args.get("url", "")
        prefill_force_target_post_version = _parse_boolish(
            request.args.get("force_target_post_version"),
            default=False,
        )
        prefill_force_overwrite_matching_version = _parse_boolish(
            request.args.get("force_overwrite_matching_version"),
            default=False,
        )
        prefill_import_target_mode = request.args.get("import_target_mode", "").strip().lower()
        prefill_target_post_id = request.args.get("target_post_id", type=int)
        prefill_overwrite_matching_version = _parse_boolish(
            request.args.get("overwrite_matching_version"),
            default=True,
        )
        if prefill_force_target_post_version:
            prefill_import_target_mode = "existing"
        if prefill_force_overwrite_matching_version:
            prefill_import_target_mode = "existing"
            prefill_overwrite_matching_version = True
        prefill_set_as_default = _parse_boolish(
            request.args.get("set_as_default"),
            default=True,
        )
        prefill_version_label = request.args.get("version_label", "")
        prefill_version_language = request.args.get("version_language", "")
        return render_template(
            "import_form.html",
            creators=creators,
            selected_creator=selected_creator,
            selected_series=selected_series,
            prefill_url=prefill_url,
            prefill_force_target_post_version=prefill_force_target_post_version,
            prefill_force_overwrite_matching_version=prefill_force_overwrite_matching_version,
            prefill_import_target_mode=prefill_import_target_mode,
            prefill_target_post_id=prefill_target_post_id,
            prefill_overwrite_matching_version=prefill_overwrite_matching_version,
            prefill_set_as_default=prefill_set_as_default,
            prefill_version_label=prefill_version_label,
            prefill_version_language=prefill_version_language,
            series_list=db.list_series(selected_creator) if selected_creator else [],
        )

    @app.post("/import/preview")
    def import_preview():
        raw_url = request.form.get("post_url", "").strip()
        creator_id = request.form.get("creator_id", type=int)
        series_id = request.form.get("series_id", type=int)

        if not creator_id:
            flash("Pick a creator first.", "error")
            return redirect(url_for("import_form", url=raw_url))

        creator = db.get_creator(creator_id)
        if not creator:
            flash("Creator not found.", "error")
            return redirect(url_for("import_form", url=raw_url))

        try:
            post_ref = parse_kemono_post_url(raw_url)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("import_form", url=raw_url, creator_id=creator_id))

        fallback_user = creator["external_user_id"] if not post_ref.user_id else None
        try:
            raw_payload = fetch_post_json(post_ref, fallback_user_id=fallback_user)
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to fetch post: {exc}", "error")
            return redirect(url_for("import_form", url=raw_url, creator_id=creator_id))
        payload = normalize_post_payload(raw_payload)

        resolved_user_id = post_ref.user_id or payload.get("user")
        if not resolved_user_id:
            flash("Could not infer user ID for this URL.", "error")
            return redirect(url_for("import_form", url=raw_url, creator_id=creator_id))

        try:
            series_id = _validate_import_series_selection(db, creator_id=creator_id, series_id=series_id)
            exact_match = _find_import_source_match(
                db,
                service=post_ref.service,
                user_id=str(resolved_user_id),
                post_id=post_ref.post_id,
                creator_id=creator_id,
            )
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("import_form", url=raw_url, creator_id=creator_id, series_id=series_id))

        prefill_import_target_mode = request.form.get("import_target_mode", "").strip().lower()
        prefill_target_post_id = request.form.get("target_post_id", type=int)
        force_target_post_version = _form_checkbox_enabled(
            request.form,
            "force_target_post_version",
            default=False,
        )
        force_overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "force_overwrite_matching_version",
            default=False,
        )
        prefill_overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
        if force_target_post_version:
            prefill_import_target_mode = "existing"
        if force_overwrite_matching_version:
            prefill_import_target_mode = "existing"
            prefill_overwrite_matching_version = True
        prefill_set_as_default = _form_checkbox_enabled(
            request.form,
            "set_as_default",
            default=True,
        )
        prefill_version_label = request.form.get("version_label", "").strip()
        prefill_version_language = request.form.get("version_language", "").strip()

        preview_ref = KemonoPostRef(service=post_ref.service, user_id=str(resolved_user_id), post_id=post_ref.post_id)
        attachments = extract_attachments(raw_payload)
        creator_posts = db.list_posts_for_creator(
            creator_id,
            sort_by="published",
            sort_direction="desc",
        )
        creator_post_ids = {int(row["id"]) for row in creator_posts}
        target_post_id = prefill_target_post_id if prefill_target_post_id in creator_post_ids else None
        if exact_match and target_post_id is None:
            target_post_id = int(exact_match["id"])
        default_import_target_mode = "existing" if target_post_id else "new"
        if prefill_import_target_mode in {"new", "existing"} and exact_match is None:
            default_import_target_mode = prefill_import_target_mode
        if force_target_post_version:
            if prefill_target_post_id is None or prefill_target_post_id not in creator_post_ids:
                flash("Forced target post was not found for this creator.", "error")
                return redirect(url_for("import_form", creator_id=creator_id, series_id=series_id, url=raw_url))
            target_post_id = prefill_target_post_id
            default_import_target_mode = "existing"
        if force_overwrite_matching_version:
            if target_post_id is None and exact_match:
                target_post_id = int(exact_match["id"])
            default_import_target_mode = "existing"
        target_attachment_index = _build_target_attachment_index(
            db,
            files_base=Path(app.config["FILES_DIR"]),
            post_ids=[int(row["id"]) for row in creator_posts],
        )

        return render_template(
            "import_preview.html",
            creator=creator,
            creator_id=creator_id,
            series_id=series_id,
            post_ref=preview_ref,
            payload=payload,
            attachments=attachments,
            creator_posts=creator_posts,
            exact_match_post=exact_match,
            default_target_post_id=target_post_id,
            can_create_new=exact_match is None,
            default_import_target_mode=default_import_target_mode,
            default_overwrite_matching_version=prefill_overwrite_matching_version,
            default_set_as_default=prefill_set_as_default,
            default_version_label=prefill_version_label or "Original",
            default_version_language=prefill_version_language,
            force_target_post_version=force_target_post_version,
            force_overwrite_matching_version=force_overwrite_matching_version,
            target_attachment_index=target_attachment_index,
        )

    @app.post("/import/commit")
    def import_commit():
        creator_id = request.form.get("creator_id", type=int)
        series_id = request.form.get("series_id", type=int)
        service = request.form.get("service", "").strip()
        user_id = request.form.get("user_id", "").strip()
        post_id = request.form.get("post_id", "").strip()

        if not creator_id or not service or not user_id or not post_id:
            flash("Missing import fields.", "error")
            return redirect(url_for("import_form"))
        try:
            series_id = _validate_import_series_selection(db, creator_id=creator_id, series_id=series_id)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("import_form"))
        overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
        force_target_post_version = _form_checkbox_enabled(
            request.form,
            "force_target_post_version",
            default=False,
        )
        force_overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "force_overwrite_matching_version",
            default=False,
        )
        import_target_mode = request.form.get("import_target_mode", "new")
        if force_target_post_version:
            import_target_mode = "existing"
        if force_overwrite_matching_version:
            import_target_mode = "existing"
            overwrite_matching_version = True
        set_as_default = _form_checkbox_enabled(
            request.form,
            "set_as_default",
            default=True,
        )
        try:
            local_post_id, imported_version_id = _import_post_into_library(
                db,
                files_base=Path(app.config["FILES_DIR"]),
                icons_base=Path(app.config["ICONS_DIR"]),
                creator_id=creator_id,
                series_id=series_id,
                service=service,
                user_id=user_id,
                post_id=post_id,
                import_target_mode=import_target_mode,
                target_post_id=request.form.get("target_post_id", type=int),
                overwrite_matching_version=overwrite_matching_version,
                set_as_default=set_as_default,
                version_label=request.form.get("version_label"),
                version_language=request.form.get("version_language"),
                requested_title=request.form.get("title"),
                requested_content=request.form.get("content"),
                requested_published_at=request.form.get("published_at"),
                requested_edited_at=request.form.get("edited_at"),
                requested_next_external_post_id=request.form.get("next_external_post_id"),
                requested_prev_external_post_id=request.form.get("prev_external_post_id"),
                tags_text=request.form.get("tags_text"),
                field_presence={
                    "published_at": "published_at" in request.form,
                    "edited_at": "edited_at" in request.form,
                    "next_external_post_id": "next_external_post_id" in request.form,
                    "prev_external_post_id": "prev_external_post_id" in request.form,
                },
                selected_attachment_indices=set(request.form.getlist("selected_attachment")),
            )
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("import_form"))
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to import post: {exc}", "error")
            return redirect(url_for("import_form"))

        flash("Post imported into local library.", "success")
        if not set_as_default:
            return redirect(url_for("post_detail", post_id=local_post_id, version_id=imported_version_id))
        return redirect(url_for("post_detail", post_id=local_post_id))

    @app.post("/import/start")
    def import_start():
        creator_id = request.form.get("creator_id", type=int)
        series_id = request.form.get("series_id", type=int)
        service = request.form.get("service", "").strip()
        user_id = request.form.get("user_id", "").strip()
        post_id = request.form.get("post_id", "").strip()

        if not creator_id or not service or not user_id or not post_id:
            return jsonify({"error": "Missing import fields."}), 400
        try:
            series_id = _validate_import_series_selection(db, creator_id=creator_id, series_id=series_id)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
        force_target_post_version = _form_checkbox_enabled(
            request.form,
            "force_target_post_version",
            default=False,
        )
        force_overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "force_overwrite_matching_version",
            default=False,
        )
        import_target_mode = request.form.get("import_target_mode", "new")
        if force_target_post_version:
            import_target_mode = "existing"
        if force_overwrite_matching_version:
            import_target_mode = "existing"
            overwrite_matching_version = True
        set_as_default = _form_checkbox_enabled(
            request.form,
            "set_as_default",
            default=True,
        )

        job_id = uuid.uuid4().hex
        job_payload = {
            "creator_id": creator_id,
            "series_id": series_id,
            "service": service,
            "user_id": user_id,
            "post_id": post_id,
            "import_target_mode": import_target_mode,
            "target_post_id": request.form.get("target_post_id", type=int),
            "overwrite_matching_version": overwrite_matching_version,
            "set_as_default": set_as_default,
            "version_label": request.form.get("version_label"),
            "version_language": request.form.get("version_language"),
            "requested_title": request.form.get("title"),
            "requested_content": request.form.get("content"),
            "requested_published_at": request.form.get("published_at"),
            "requested_edited_at": request.form.get("edited_at"),
            "requested_next_external_post_id": request.form.get("next_external_post_id"),
            "requested_prev_external_post_id": request.form.get("prev_external_post_id"),
            "tags_text": request.form.get("tags_text"),
            "field_presence": {
                "published_at": "published_at" in request.form,
                "edited_at": "edited_at" in request.form,
                "next_external_post_id": "next_external_post_id" in request.form,
                "prev_external_post_id": "prev_external_post_id" in request.form,
            },
            "selected_attachment_indices": set(request.form.getlist("selected_attachment")),
        }

        with import_jobs_lock:
            import_jobs[job_id] = {
                "status": "queued",
                "message": "Queued import job.",
                "completed": 0,
                "total": 0,
                "current_file": None,
                "redirect_url": None,
                "error": None,
                "queue_position": len(import_job_queue) + 1,
                "payload": job_payload,
            }
            import_job_queue.append(job_id)
            refresh_import_queue_positions_locked()
            import_job_queue_condition.notify()
        return jsonify(
            {
                "job_id": job_id,
                "status_url": url_for("import_job_status", job_id=job_id),
            }
        )

    @app.get("/import/jobs/<job_id>/status")
    def import_job_status(job_id: str):
        with import_jobs_lock:
            job = import_jobs.get(job_id)
            if not job:
                return jsonify({"error": "Import job not found."}), 404
            snapshot = dict(job)
        snapshot.pop("payload", None)
        return jsonify(snapshot)

    @app.get("/posts/<int:post_id>")
    def post_detail(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        creator = db.get_creator(int(post["creator_id"]))
        header_context = _build_post_header_context(post=post, creator=creator)
        requested_version_id = request.args.get("version_id", type=int)
        active_version = db.get_post_version(post_id, requested_version_id)
        if not active_version:
            versions = db.list_post_versions(post_id)
            if not versions:
                return ("Post version not found", 404)
            active_version = versions[0]
        versions = db.list_post_versions(post_id)
        active_version_id = int(active_version["id"])
        nav_scope = request.args.get("nav_scope", "series").strip().lower()
        if nav_scope not in {"series", "all"}:
            nav_scope = "series"
        attachments = db.list_attachments(post_id, version_id=active_version_id)
        local_media_map, local_media_by_name, local_media_by_path_key = _build_local_media_maps(active_version, attachments)
        _apply_postwide_media_aliases(
            db,
            post_id=post_id,
            local_media_by_name=local_media_by_name,
            local_media_by_path_key=local_media_by_path_key,
        )
        remote_media_by_name = _build_remote_media_by_name(active_version, attachments)
        files_base = Path(app.config["FILES_DIR"])
        attachment_rows = []
        for row in attachments:
            local_path = row["local_path"]
            local_abs = files_base / local_path if isinstance(local_path, str) and local_path.strip() else None
            local_available = _is_valid_file(local_abs)
            remote_url = str(row["remote_url"])
            remote_url_display = _preferred_remote_url_for_access(
                remote_url,
                row["name"],
            )
            is_image = _is_likely_image_attachment(
                remote_url=remote_url,
                name=row["name"],
                local_path=local_path,
                kind=row["kind"],
            )
            preview_url = (
                url_for("serve_file", relative_path=local_path)
                if local_available and local_path
                else remote_url_display
            )
            attachment_rows.append(
                {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "remote_url": remote_url,
                    "remote_url_display": remote_url_display,
                    "local_path": row["local_path"],
                    "kind": row["kind"],
                    "local_available": local_available,
                    "is_image": is_image,
                    "preview_url": preview_url,
                }
            )
        attachment_rows = _dedupe_post_detail_attachments(attachment_rows)

        rendered_content = render_post_content(
            active_version["content"],
            current_service=post["service"],
            current_user_id=post["external_user_id"],
            current_post_id=post_id,
            local_media_map=local_media_map,
            local_media_by_name=local_media_by_name,
            remote_media_by_name=remote_media_by_name,
        )
        creator_id = int(post["creator_id"])
        raw_series_id = post["series_id"]
        current_series_id = int(raw_series_id) if raw_series_id is not None else None
        navigator_title = "All entries"
        if nav_scope == "all":
            navigator_rows = db.list_posts_for_creator(
                creator_id,
                sort_by="published",
                sort_direction="desc",
            )
        elif current_series_id is not None:
            navigator_rows = db.list_posts_for_creator(
                creator_id,
                series_id=current_series_id,
                sort_by="published",
                sort_direction="desc",
            )
            navigator_title = str(post["series_name"]).strip() if post["series_name"] else "Series"
        else:
            navigator_rows = db.list_posts_for_creator(
                creator_id,
                unsorted_only=True,
                sort_by="published",
                sort_direction="desc",
            )
            navigator_title = "Unsorted"

        toggle_query: dict[str, Any] = {}
        if not active_version["is_default"]:
            toggle_query["version_id"] = active_version_id
        series_scope_url = url_for("post_detail", post_id=post_id, nav_scope="series", **toggle_query)
        all_scope_url = url_for("post_detail", post_id=post_id, nav_scope="all", **toggle_query)

        navigator_entries: list[dict[str, Any]] = []
        for row in navigator_rows:
            nav_post_id = int(row["id"])
            href = url_for("post_detail", post_id=nav_post_id, nav_scope=nav_scope)
            if nav_post_id == post_id and not active_version["is_default"]:
                href = url_for("post_detail", post_id=nav_post_id, nav_scope=nav_scope, version_id=active_version_id)
            navigator_entries.append(
                {
                    "id": nav_post_id,
                    "title": str(row["title"]) if row["title"] else f"Post {nav_post_id}",
                    "published_at": row["published_at"],
                    "series_name": row["series_name"],
                    "is_current": nav_post_id == post_id,
                    "href": href,
                }
            )

        return render_template(
            "post_detail.html",
            post=post,
            versions=versions,
            active_version=active_version,
            attachments=attachment_rows,
            rendered_content=rendered_content,
            header_context=header_context,
            navigator_entries=navigator_entries,
            navigator_scope=nav_scope,
            navigator_title=navigator_title,
            navigator_series_url=series_scope_url,
            navigator_all_url=all_scope_url,
        )

    @app.post("/posts/<int:post_id>/attachments/<int:attachment_id>/retry")
    def retry_attachment_download(post_id: int, attachment_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        version_id = request.args.get("version_id", type=int) or request.form.get("version_id", type=int)
        active_version = db.get_post_version(post_id, version_id)
        if not active_version:
            return ("Post version not found", 404)
        active_version_id = int(active_version["id"])
        detail_url = (
            url_for("post_detail", post_id=post_id)
            if active_version["is_default"]
            else url_for("post_detail", post_id=post_id, version_id=active_version_id)
        )

        attachment = next(
            (row for row in db.list_attachments(post_id, version_id=active_version_id) if int(row["id"]) == attachment_id),
            None,
        )
        if attachment is None:
            return ("Attachment not found", 404)

        result = _retry_attachment_row(
            db,
            files_base=Path(app.config["FILES_DIR"]),
            attachment_id=attachment_id,
            post_id=post_id,
            attachment_name=attachment["name"],
            remote_url=str(attachment["remote_url"]),
            existing_local_path=_optional_str(attachment["local_path"]),
        )
        if not bool(result["success"]):
            error = _optional_str(result["error"]) or "unknown error"
            flash(f"Retry failed for {attachment['name']}: {error}", "error")
            return redirect(detail_url)
        flash(f"Downloaded {attachment['name']}.", "success")
        return redirect(detail_url)

    @app.post("/attachments/retry-missing")
    def retry_missing_attachments():
        scope = request.form.get("scope", "all").strip().lower()
        scope_id_raw = request.form.get("scope_id", "").strip()
        return_to = request.form.get("return_to", "").strip() or url_for("attachment_manager")
        if not return_to.startswith("/"):
            return_to = url_for("attachment_manager")

        files_base = Path(app.config["FILES_DIR"])
        missing_rows = _collect_retry_scope_rows(scope, scope_id_raw)
        if not missing_rows:
            flash("No missing attachments matched this retry scope.", "success")
            return redirect(return_to)

        success_count = 0
        failure_count = 0
        failure_samples: list[str] = []
        for row in missing_rows:
            result = _retry_attachment_row(
                db,
                files_base=files_base,
                attachment_id=int(row["id"]),
                post_id=int(row["post_id"]),
                attachment_name=row["name"],
                remote_url=str(row["remote_url"]),
                existing_local_path=_optional_str(row["local_path"]),
            )
            if bool(result["success"]):
                success_count += 1
                continue
            failure_count += 1
            error = _optional_str(result["error"])
            if error and len(failure_samples) < 3:
                failure_samples.append(f"{row['name']}: {error}")

        if success_count:
            flash(f"Retried {success_count} missing attachment(s).", "success")
        if failure_count:
            detail = "; ".join(failure_samples)
            suffix = f" Examples: {detail}" if detail else ""
            flash(f"{failure_count} attachment retry attempt(s) failed.{suffix}", "error")
        return redirect(return_to)

    @app.post("/attachments/retry-missing/start")
    def start_retry_missing_attachments():
        scope = request.form.get("scope", "all").strip().lower()
        scope_id_raw = request.form.get("scope_id", "").strip()
        return_to = request.form.get("return_to", "").strip() or url_for("attachment_manager")
        if not return_to.startswith("/"):
            return_to = url_for("attachment_manager")

        job_id = uuid.uuid4().hex
        initial_total = len(_collect_retry_scope_rows(scope, scope_id_raw))
        with attachment_retry_jobs_lock:
            attachment_retry_jobs[job_id] = {
                "status": "queued",
                "message": "Queued retry job.",
                "completed": 0,
                "total": initial_total,
                "current_file": None,
                "error": None,
                "queue_position": len(attachment_retry_job_queue) + 1,
                "success_count": 0,
                "failure_count": 0,
                "failure_examples": [],
                "results": [],
                "redirect_url": return_to,
                "payload": {
                    "scope": scope,
                    "scope_id_raw": scope_id_raw,
                    "return_to": return_to,
                },
            }
            attachment_retry_job_queue.append(job_id)
            refresh_attachment_retry_queue_positions_locked()
            attachment_retry_job_queue_condition.notify()

        return jsonify(
            {
                "job_id": job_id,
                "status_url": url_for("attachment_retry_job_status", job_id=job_id),
            }
        )

    @app.get("/attachments/retry-jobs/<job_id>/status")
    def attachment_retry_job_status(job_id: str):
        with attachment_retry_jobs_lock:
            job = attachment_retry_jobs.get(job_id)
            if not job:
                return jsonify({"error": "Retry job not found."}), 404
            snapshot = dict(job)
        snapshot.pop("payload", None)
        return jsonify(snapshot)

    @app.post("/posts/<int:post_id>/delete")
    def delete_post(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)

        creator_id = int(post["creator_id"])
        series_id = int(post["series_id"]) if post["series_id"] else None
        deleted = db.delete_post(post_id)
        if not deleted:
            return ("Post not found", 404)

        files_root = Path(app.config["FILES_DIR"])
        shutil.rmtree(files_root / f"post_{post_id}", ignore_errors=True)
        flash("Post deleted.", "success")
        if series_id:
            return redirect(url_for("creator_detail", creator_id=creator_id, series_id=series_id))
        return redirect(url_for("creator_detail", creator_id=creator_id))

    @app.route("/posts/<int:post_id>/edit", methods=["GET", "POST"])
    def edit_post(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        creator = db.get_creator(int(post["creator_id"]))
        header_context = _build_post_header_context(post=post, creator=creator)
        requested_version_id = request.args.get("version_id", type=int) or request.form.get("version_id", type=int)
        active_version = db.get_post_version(post_id, requested_version_id)
        if not active_version:
            return ("Post version not found", 404)
        active_version_id = int(active_version["id"])
        versions = db.list_post_versions(post_id)
        series_list = db.list_series(post["creator_id"])
        files_base = Path(app.config["FILES_DIR"])
        attachments = db.list_attachments(post_id, version_id=active_version_id)

        attachment_rows: list[dict[str, Any]] = []
        selected_thumbnail_choice: str | None = None
        thumbnail_remote_url = _optional_str(active_version["thumbnail_remote_url"])
        thumbnail_name = _optional_str(active_version["thumbnail_name"])
        thumbnail_local_path = _optional_str(active_version["thumbnail_local_path"])
        thumbnail_preview_url: str | None = None
        active_metadata = _safe_load_metadata(active_version["metadata_json"])
        thumbnail_focus_x, thumbnail_focus_y = _extract_thumbnail_focus_from_metadata(active_metadata)
        tracked_remote_urls: set[str] = set()
        tracked_remote_path_keys: set[str] = set()
        tracked_name_keys: set[str] = set()
        tracked_remote_name_aliases: set[str] = set()

        for idx, row in enumerate(attachments):
            local_path = _optional_str(row["local_path"])
            remote_url = str(row["remote_url"])
            local_available = _is_valid_file(files_base / local_path) if local_path else False
            remote_url_display = _preferred_remote_url_for_access(remote_url, row["name"])
            is_image = _is_likely_image_attachment(
                remote_url=remote_url,
                name=row["name"],
                local_path=local_path,
                kind=row["kind"],
            )
            preview_url = (
                url_for("serve_file", relative_path=local_path)
                if local_available and local_path
                else remote_url_display
            )
            row_data = {
                "id": int(row["id"]),
                "form_key": f"id_{int(row['id'])}",
                "choice_value": f"id:{int(row['id'])}",
                "tracked": True,
                "name": str(row["name"]),
                "kind": str(row["kind"]),
                "remote_url": remote_url,
                "remote_url_display": remote_url_display,
                "local_path": local_path,
                "local_available": local_available,
                "is_image": is_image,
                "preview_url": preview_url,
            }
            attachment_rows.append(row_data)
            tracked_remote_urls.add(remote_url)
            remote_path_key = _remote_path_key(remote_url)
            if remote_path_key:
                tracked_remote_path_keys.add(remote_path_key)
            tracked_name_key = _attachment_collapse_key(row_data["name"])
            if tracked_name_key:
                tracked_name_keys.add(tracked_name_key)
            tracked_remote_name_aliases.update(_remote_filename_alias_keys(remote_url))
            if selected_thumbnail_choice is None:
                if thumbnail_remote_url and row_data["remote_url"] == thumbnail_remote_url:
                    selected_thumbnail_choice = row_data["choice_value"]
                elif thumbnail_local_path and row_data["local_path"] == thumbnail_local_path:
                    selected_thumbnail_choice = row_data["choice_value"]
                elif thumbnail_name and row_data["name"] == thumbnail_name:
                    selected_thumbnail_choice = row_data["choice_value"]

        source_candidates = extract_attachments(active_metadata)
        source_candidate_index = 0
        seen_source_keys: set[str] = set()
        seen_source_aliases: set[str] = set()
        for candidate in source_candidates:
            remote_url = str(candidate.remote_url)
            remote_path_key = _remote_path_key(remote_url)
            candidate_name_key = _attachment_collapse_key(candidate.name)
            candidate_aliases = _remote_filename_alias_keys(remote_url)
            if candidate_name_key:
                candidate_aliases.add(candidate_name_key)

            if remote_url in tracked_remote_urls:
                continue
            if remote_path_key and remote_path_key in tracked_remote_path_keys:
                continue
            if candidate_name_key and candidate_name_key in tracked_name_keys:
                continue
            if candidate_aliases and any(alias in tracked_remote_name_aliases for alias in candidate_aliases):
                continue

            source_identity = (
                f"path:{remote_path_key}"
                if remote_path_key
                else f"name:{candidate_name_key}"
                if candidate_name_key
                else f"url:{remote_url}"
            )
            if source_identity in seen_source_keys:
                continue
            if candidate_aliases and any(alias in seen_source_aliases for alias in candidate_aliases):
                continue
            seen_source_keys.add(source_identity)
            seen_source_aliases.update(candidate_aliases)
            remote_url_display = _preferred_remote_url_for_access(remote_url, candidate.name)
            is_image = _is_likely_image_attachment(
                remote_url=remote_url,
                name=candidate.name,
                local_path=None,
                kind=candidate.kind,
            )
            row_data = {
                "id": None,
                "form_key": f"src_{source_candidate_index}",
                "choice_value": f"remote:{remote_url}",
                "tracked": False,
                "name": str(candidate.name),
                "kind": str(candidate.kind),
                "remote_url": remote_url,
                "remote_url_display": remote_url_display,
                "local_path": None,
                "local_available": False,
                "is_image": is_image,
                "preview_url": remote_url_display,
            }
            source_candidate_index += 1
            attachment_rows.append(row_data)
            if selected_thumbnail_choice is None and thumbnail_remote_url and remote_url == thumbnail_remote_url:
                selected_thumbnail_choice = row_data["choice_value"]

        if thumbnail_local_path and _is_valid_file(files_base / thumbnail_local_path):
            thumbnail_preview_url = url_for("serve_file", relative_path=thumbnail_local_path)
        elif thumbnail_remote_url:
            thumbnail_preview_url = _preferred_remote_url_for_access(thumbnail_remote_url, thumbnail_name)

        if request.method == "POST":
            action = request.form.get("action", "save").strip().lower()
            if action == "remove_attachment":
                flash("Attachment changes are now applied on Save Changes.", "success")
                return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))

            title = request.form.get("title", "").strip()
            content = request.form.get("content", "")
            series_id = request.form.get("series_id", type=int)
            version_label = request.form.get("version_label", "").strip() or "Version"
            version_language = request.form.get("version_language")
            thumbnail_choice = request.form.get("thumbnail_attachment_id", "__keep__").strip()
            focus_x, focus_y = _parse_thumbnail_focus_inputs(
                request.form.get("thumbnail_focus_x"),
                request.form.get("thumbnail_focus_y"),
                fallback_x=thumbnail_focus_x,
                fallback_y=thumbnail_focus_y,
            )
            if not title:
                flash("Version title is required.", "error")
                return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))
            try:
                series_id = _validate_import_series_selection(
                    db,
                    creator_id=int(post["creator_id"]),
                    series_id=series_id,
                )
            except ValueError as exc:
                flash(str(exc), "error")
                return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))

            # Apply attachment edits only on Save.
            managed_attachments_by_choice: dict[str, dict[str, Any]] = {}
            managed_attachments: list[dict[str, Any]] = []
            local_ref_sync_updates: dict[str, tuple[str | None, str | None]] = {}
            name_sync_by_remote: dict[str, str] = {}
            rename_aliases: dict[str, str] = {}
            for item in attachment_rows:
                form_key = str(item["form_key"])
                tracked = bool(item["tracked"])
                local_path = _optional_str(item["local_path"])
                local_available = bool(local_path and _is_valid_file(files_base / local_path))
                keep_in_version: bool
                if tracked:
                    keep_values = request.form.getlist(f"attachment_keep_{form_key}")
                    keep_in_version = True if not keep_values else "1" in keep_values
                else:
                    add_values = request.form.getlist(f"attachment_add_{form_key}")
                    keep_in_version = "1" in add_values or thumbnail_choice == str(item["choice_value"])
                if not keep_in_version:
                    continue

                desired_name_raw = request.form.get(f"attachment_name_{form_key}", item["name"])
                desired_name = sanitize_filename(str(desired_name_raw or "").strip()) or str(item["name"])
                original_name = sanitize_filename(str(item["name"])).strip()
                if tracked and original_name and desired_name and original_name != desired_name:
                    name_sync_by_remote[str(item["remote_url"])] = desired_name
                    rename_aliases[original_name.lower()] = desired_name

                previous_local_path = local_path
                if local_available and local_path:
                    local_path = _rename_local_attachment_file(
                        files_base=files_base,
                        local_path=local_path,
                        desired_name=desired_name,
                        fallback_name=str(item["name"]),
                    )

                if previous_local_path and local_path != previous_local_path:
                    local_ref_sync_updates[previous_local_path] = (local_path, desired_name)

                managed = {
                    "id": item["id"],
                    "choice_value": str(item["choice_value"]),
                    "name": desired_name,
                    "remote_url": str(item["remote_url"]),
                    "local_path": local_path,
                    "kind": str(item["kind"]),
                }
                managed_attachments.append(managed)
                managed_attachments_by_choice[str(item["choice_value"])] = managed

            managed_attachments = _dedupe_managed_attachment_local_files(
                files_base=files_base,
                managed_attachments=managed_attachments,
            )

            resolved_thumbnail_name = active_version["thumbnail_name"]
            resolved_thumbnail_remote_url = active_version["thumbnail_remote_url"]
            resolved_thumbnail_local_path = active_version["thumbnail_local_path"]

            if thumbnail_choice == "__none__":
                resolved_thumbnail_name = None
                resolved_thumbnail_remote_url = None
                resolved_thumbnail_local_path = None
                focus_x = None
                focus_y = None
            elif thumbnail_choice == "__keep__":
                if selected_thumbnail_choice:
                    kept_current = managed_attachments_by_choice.get(selected_thumbnail_choice)
                    if kept_current is None:
                        if selected_thumbnail_choice.startswith("id:"):
                            resolved_thumbnail_name = None
                            resolved_thumbnail_remote_url = None
                            resolved_thumbnail_local_path = None
                            focus_x = None
                            focus_y = None
                    else:
                        resolved_thumbnail_name = kept_current["name"]
                        resolved_thumbnail_remote_url = kept_current["remote_url"]
                        resolved_thumbnail_local_path = kept_current["local_path"]
            elif thumbnail_choice != "__keep__":
                selected_attachment = managed_attachments_by_choice.get(thumbnail_choice)
                if selected_attachment is None:
                    flash("Selected thumbnail file was not found.", "error")
                    return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))
                resolved_thumbnail_name = selected_attachment["name"]
                resolved_thumbnail_remote_url = selected_attachment["remote_url"]
                resolved_thumbnail_local_path = _optional_str(selected_attachment["local_path"])

            metadata_for_save = _set_thumbnail_focus_in_metadata(active_metadata, focus_x, focus_y)
            try:
                with db.transaction() as conn:
                    db.replace_attachments(
                        post_id,
                        [
                            {
                                "name": item["name"],
                                "remote_url": item["remote_url"],
                                "local_path": item["local_path"],
                                "kind": item["kind"],
                            }
                            for item in managed_attachments
                        ],
                        version_id=active_version_id,
                        conn=conn,
                    )

                    for remote_url, new_name in name_sync_by_remote.items():
                        db.sync_attachment_name_by_remote_for_post(
                            post_id,
                            remote_url=remote_url,
                            new_name=new_name,
                            conn=conn,
                        )

                    for old_local_path, (new_local_path, new_name) in local_ref_sync_updates.items():
                        db.sync_attachment_local_refs_for_post(
                            post_id,
                            old_local_path=old_local_path,
                            new_local_path=new_local_path,
                            new_name=new_name,
                            conn=conn,
                        )

                    _reprocess_post_versions_for_media_renames(
                        db,
                        post_id=post_id,
                        rename_aliases=rename_aliases,
                        conn=conn,
                    )
                    db.update_post_series(post_id=post_id, series_id=series_id, conn=conn)
                    db.update_post_version(
                        version_id=active_version_id,
                        label=version_label,
                        language=version_language,
                        title=title,
                        content=content,
                        thumbnail_name=resolved_thumbnail_name,
                        thumbnail_remote_url=resolved_thumbnail_remote_url,
                        thumbnail_local_path=resolved_thumbnail_local_path,
                        published_at=active_version["published_at"],
                        edited_at=active_version["edited_at"],
                        next_external_post_id=active_version["next_external_post_id"],
                        prev_external_post_id=active_version["prev_external_post_id"],
                        metadata=metadata_for_save,
                        source_url=active_version["source_url"],
                        conn=conn,
                    )
            except Exception as exc:  # noqa: BLE001
                flash(f"Failed to update post: {exc}", "error")
                return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))
            flash("Post updated.", "success")
            return redirect(url_for("post_detail", post_id=post_id, version_id=active_version_id))

        tracked_attachment_rows = [item for item in attachment_rows if item["tracked"]]
        source_attachment_rows = [item for item in attachment_rows if not item["tracked"]]
        return render_template(
            "post_edit.html",
            post=post,
            versions=versions,
            active_version=active_version,
            series_list=series_list,
            edit_content=_prettify_content_for_edit(active_version["content"]),
            attachments=attachment_rows,
            tracked_attachments=tracked_attachment_rows,
            source_attachments=source_attachment_rows,
            selected_thumbnail_choice=selected_thumbnail_choice,
            thumbnail_preview_url=thumbnail_preview_url,
            thumbnail_focus_x=thumbnail_focus_x,
            thumbnail_focus_y=thumbnail_focus_y,
            header_context=header_context,
        )

    @app.post("/posts/<int:post_id>/versions/<int:version_id>/set-default")
    def set_default_post_version(post_id: int, version_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        try:
            db.set_default_post_version(post_id, version_id)
        except ValueError:
            return ("Post version not found", 404)
        flash("Default version updated.", "success")
        return redirect(url_for("post_detail", post_id=post_id, version_id=version_id))

    @app.post("/posts/<int:post_id>/versions/clone")
    def clone_post_version(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        source_version_id = request.form.get("source_version_id", type=int)
        if not source_version_id:
            return ("Missing source version", 400)
        label = request.form.get("version_label", "").strip() or "Manual translation"
        language = request.form.get("version_language")
        set_default = request.form.get("set_as_default") == "1"
        try:
            new_version_id = db.clone_post_version(
                post_id=post_id,
                source_version_id=source_version_id,
                label=label,
                language=language,
                set_default=set_default,
            )
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("post_detail", post_id=post_id, version_id=source_version_id))
        flash("Manual version created.", "success")
        return redirect(url_for("post_detail", post_id=post_id, version_id=new_version_id))

    @app.post("/posts/<int:post_id>/versions/<int:version_id>/delete")
    def delete_post_version(post_id: int, version_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        deleted = db.delete_post_version(post_id, version_id)
        if not deleted:
            return ("Post version not found", 404)
        remaining = db.list_post_versions(post_id)
        if not remaining:
            flash("Version deleted. Post has no versions left.", "success")
            return redirect(url_for("creator_detail", creator_id=post["creator_id"]))
        flash("Version deleted.", "success")
        return redirect(url_for("post_detail", post_id=post_id, version_id=int(remaining[0]["id"])))

    @app.get("/links/resolve")
    def resolve_link():
        service = request.args.get("service", "").strip()
        post_external_id = request.args.get("post", "").strip()
        user_external_id = request.args.get("user", "").strip() or None
        from_post = request.args.get("from_post", type=int)
        assumed = request.args.get("assumed_from_context") == "1"

        if not service or not post_external_id:
            return ("Missing resolver parameters", 400)

        local = db.find_local_post(service, post_external_id, user_external_id)
        if local:
            return redirect(url_for("post_detail", post_id=int(local["id"])))

        import_url = None
        inferred_user = user_external_id
        creator_id = None
        series_id = None
        if from_post:
            source_post = db.get_post(from_post)
            if source_post:
                creator_id = int(source_post["creator_id"])
                series_id = int(source_post["series_id"]) if source_post["series_id"] else None
                if not inferred_user and assumed:
                    inferred_user = source_post["external_user_id"]

        if inferred_user:
            kemono_url = f"https://kemono.cr/{service}/user/{inferred_user}/post/{post_external_id}"
        else:
            kemono_url = f"https://kemono.cr/{service}/post/{post_external_id}"

        import_url = url_for(
            "import_form",
            url=kemono_url,
            creator_id=creator_id,
            series_id=series_id,
        )

        return render_template(
            "resolve_link.html",
            service=service,
            external_user_id=inferred_user,
            external_post_id=post_external_id,
            import_url=import_url,
        )

    @app.get("/files/<path:relative_path>")
    def serve_file(relative_path: str):
        # Keep compatibility with old Windows-stored paths using backslashes.
        safe_relative = relative_path.replace("\\", "/")
        base_dir = Path(app.config["FILES_DIR"]).resolve()
        target = (base_dir / safe_relative).resolve()
        try:
            target.relative_to(base_dir)
        except ValueError:
            return ("Not found", 404)
        if not target.exists() or not target.is_file():
            return ("Not found", 404)
        detected_mime = _detect_image_mime(target)
        if detected_mime:
            return send_file(target, mimetype=detected_mime, as_attachment=False)
        return send_from_directory(app.config["FILES_DIR"], safe_relative, as_attachment=False)

    @app.get("/creator-icons/<path:relative_path>")
    def serve_creator_icon(relative_path: str):
        safe_relative = relative_path.replace("\\", "/")
        return send_from_directory(app.config["ICONS_DIR"], safe_relative, as_attachment=False)

    @app.get("/favicon.ico")
    def favicon():
        return send_from_directory(Path(app.root_path) / "static", "icon.svg", mimetype="image/svg+xml")

    return app


def _build_existing_file_indexes(
    files_base: Path,
    existing_rows: list[Any],
) -> tuple[dict[str, Path], dict[str, Path], dict[str, Path]]:
    by_remote: dict[str, Path] = {}
    by_path_key: dict[str, Path] = {}
    by_name: dict[str, Path] = {}
    for row in existing_rows:
        rel = row["local_path"]
        if not rel:
            continue
        abs_path = files_base / rel
        if not _is_valid_file(abs_path):
            continue
        by_remote[row["remote_url"]] = abs_path
        path_key = _remote_path_key(str(row["remote_url"]))
        if path_key and path_key not in by_path_key:
            by_path_key[path_key] = abs_path
        normalized_name = sanitize_filename(row["name"])
        if normalized_name and normalized_name not in by_name:
            by_name[normalized_name] = abs_path
    return by_remote, by_path_key, by_name


def _is_valid_file(path: Path | None) -> bool:
    return _file_size_if_valid(path) is not None


def _file_size_if_valid(path: Path | None) -> int | None:
    if path is None:
        return None
    try:
        path_stat = path.stat()
    except OSError:
        return None
    if not stat.S_ISREG(path_stat.st_mode):
        return None
    size = int(path_stat.st_size)
    return size if size > 0 else None


def _collect_local_file_status_by_path(files_base: Path, rows: list[Any]) -> dict[str, tuple[bool, int | None]]:
    local_paths: set[str] = set()
    for row in rows:
        local_path = _optional_str(row["local_path"])
        if local_path:
            local_paths.add(local_path)
    status: dict[str, tuple[bool, int | None]] = {}
    for local_path in local_paths:
        file_size = _file_size_if_valid(files_base / local_path)
        status[local_path] = (file_size is not None, file_size)
    return status


def _download_with_fallback_remote_url(
    remote_url: str,
    destination: Path,
    attachment_name: Any,
) -> str | None:
    urls_to_try = [remote_url]
    fallback = _kemono_data_fallback_url(remote_url, attachment_name)
    if fallback and fallback not in urls_to_try:
        urls_to_try.append(fallback)

    for candidate_url in urls_to_try:
        try:
            download_attachment(candidate_url, destination)
            return candidate_url
        except Exception:  # noqa: BLE001
            continue
    return None


def _retry_attachment_row(
    db: LibraryDBLike,
    *,
    files_base: Path,
    attachment_id: int,
    post_id: int,
    attachment_name: Any,
    remote_url: str,
    existing_local_path: str | None,
) -> dict[str, Any]:
    if existing_local_path:
        destination = files_base / existing_local_path
    else:
        destination = files_base / f"post_{post_id}" / sanitize_filename(str(attachment_name))

    try:
        used_remote_url = _download_with_fallback_remote_url(
            remote_url,
            destination,
            attachment_name,
        )
        if not used_remote_url:
            return {"success": False, "error": "all download URL variants failed", "local_path": None, "file_size": None}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": str(exc), "local_path": None, "file_size": None}

    if not _is_valid_file(destination):
        return {"success": False, "error": "downloaded file is empty", "local_path": None, "file_size": None}

    local_rel = destination.relative_to(files_base).as_posix()
    db.update_attachment_local_path(attachment_id, local_rel)
    try:
        file_size = destination.stat().st_size
    except OSError:
        file_size = None
    return {"success": True, "error": None, "local_path": local_rel, "file_size": file_size}


def _build_local_file_url(relative_path: str | None) -> str | None:
    normalized = _optional_str(relative_path)
    if normalized is None:
        return None
    segments = [quote(part, safe="") for part in normalized.replace("\\", "/").split("/") if part]
    if not segments:
        return None
    return "/files/" + "/".join(segments)


def _preferred_remote_url_for_access(remote_url: str, attachment_name: Any) -> str:
    return _kemono_data_fallback_url(remote_url, attachment_name) or remote_url


def _kemono_data_fallback_url(remote_url: str, attachment_name: Any) -> str | None:
    parsed = urlparse(remote_url)
    host = parsed.netloc.lower()
    if not host.endswith("kemono.cr"):
        return None

    raw_path = parsed.path if parsed.path.startswith("/") else f"/{parsed.path}"
    if raw_path.startswith("/data/"):
        return None
    if not re.match(r"^/[0-9a-f]{2}/[0-9a-f]{2}/[^/]+$", raw_path, flags=re.IGNORECASE):
        return None

    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "f" for key, _ in query_items):
        preferred_name = str(attachment_name).strip() if attachment_name is not None else ""
        if not preferred_name:
            preferred_name = Path(raw_path).name
        query_items.append(("f", preferred_name))

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            f"/data{raw_path}",
            parsed.params,
            urlencode(query_items, doseq=True),
            parsed.fragment,
        )
    )


def _prepare_creator_icon_update(
    creator: Any,
    *,
    icons_base: Path,
    service: str,
    user_id: str,
) -> tuple[str | None, str | None, Path | None] | None:
    current_remote = creator["icon_remote_url"]
    current_local = creator["icon_local_path"]
    expected_remote = creator_icon_url(service, user_id)
    if isinstance(current_local, str) and current_local.strip():
        local_abs = icons_base / current_local
        if _is_valid_file(local_abs):
            if current_remote != expected_remote:
                return expected_remote, current_local, None
            return None

    remote_url, local_abs = download_creator_icon(service, user_id, icons_base)
    local_rel = local_abs.relative_to(icons_base).as_posix() if local_abs else None
    return remote_url, local_rel, local_abs


def _build_post_header_context(*, post: Any, creator: Any | None) -> dict[str, Any]:
    creator_id = int(post["creator_id"])
    raw_series_id = post["series_id"]
    series_id = int(raw_series_id) if raw_series_id is not None else None
    creator_name = str(post["creator_name"]) if post["creator_name"] else "Creator"
    series_name = str(post["series_name"]).strip() if post["series_name"] else ""
    creator_href = url_for("creator_detail", creator_id=creator_id)
    series_href = (
        url_for("creator_detail", creator_id=creator_id, series_id=series_id) if series_id is not None else None
    )

    icon_local_path = None
    icon_remote_url = None
    if creator is not None:
        raw_local = creator["icon_local_path"]
        raw_remote = creator["icon_remote_url"]
        if isinstance(raw_local, str) and raw_local.strip():
            icon_local_path = raw_local
        if isinstance(raw_remote, str) and raw_remote.strip():
            icon_remote_url = raw_remote

    return {
        "title": creator_name,
        "subtitle": series_name,
        "creator_href": creator_href,
        "series_href": series_href,
        "icon_local_path": icon_local_path,
        "icon_remote_url": icon_remote_url,
    }


def _build_creator_header_context(*, creator: Any, selected_series: Any | None) -> dict[str, Any]:
    creator_id = int(creator["id"])
    creator_name = str(creator["name"]) if creator["name"] else "Creator"
    # In creator folder view we only show creator context in the global header.
    subtitle = ""
    creator_href = url_for("creator_detail", creator_id=creator_id)
    series_href = (
        url_for("creator_detail", creator_id=creator_id, series_id=int(selected_series["id"]))
        if selected_series is not None
        else None
    )

    icon_local_path = None
    icon_remote_url = None
    raw_local = creator["icon_local_path"]
    raw_remote = creator["icon_remote_url"]
    if isinstance(raw_local, str) and raw_local.strip():
        icon_local_path = raw_local
    if isinstance(raw_remote, str) and raw_remote.strip():
        icon_remote_url = raw_remote

    return {
        "title": creator_name,
        "subtitle": subtitle,
        "creator_href": creator_href,
        "series_href": series_href,
        "icon_local_path": icon_local_path,
        "icon_remote_url": icon_remote_url,
    }


def _build_local_media_maps(
    post: Any,
    attachments: list[Any],
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    local_media_map: dict[str, str] = {}
    local_media_by_name: dict[str, str] = {}
    local_media_by_path_key: dict[str, str] = {}
    local_media_map_priority: dict[str, int] = {}
    local_media_by_name_priority: dict[str, int] = {}
    local_media_by_path_key_priority: dict[str, int] = {}

    for attachment in attachments:
        local_path = attachment["local_path"]
        if not local_path:
            continue

        local_url = url_for("serve_file", relative_path=local_path)
        remote_url = attachment["remote_url"]
        kind_priority = _media_kind_priority(attachment["kind"])
        _assign_preferred(
            local_media_map,
            local_media_map_priority,
            remote_url,
            local_url,
            kind_priority,
        )
        parsed = urlparse(remote_url)
        normalized_remote = (
            f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.scheme and parsed.netloc
            else parsed.path
        )
        _assign_preferred(
            local_media_map,
            local_media_map_priority,
            normalized_remote,
            local_url,
            kind_priority,
        )

        filename = Path(parsed.path).name.lower()
        if filename:
            _assign_preferred(
                local_media_by_name,
                local_media_by_name_priority,
                filename,
                local_url,
                kind_priority,
            )

        path_key = _remote_path_key(remote_url)
        if path_key:
            _assign_preferred(
                local_media_by_path_key,
                local_media_by_path_key_priority,
                path_key,
                local_url,
                kind_priority,
            )

    metadata = _safe_load_metadata(post["metadata_json"])
    for entry in _iter_metadata_media_entries(metadata):
        name = entry.get("name")
        raw_path = entry.get("path") or entry.get("url")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue

        alias_name_key = name.strip().lower()
        path_key = _remote_path_key(raw_path)
        if not path_key:
            continue
        local_url = local_media_by_path_key.get(path_key)
        if local_url:
            path_priority = local_media_by_path_key_priority.get(path_key, 0)
            _assign_preferred(
                local_media_by_name,
                local_media_by_name_priority,
                alias_name_key,
                local_url,
                path_priority,
            )

    return local_media_map, local_media_by_name, local_media_by_path_key


def _apply_postwide_media_aliases(
    db: LibraryDBLike,
    *,
    post_id: int,
    local_media_by_name: dict[str, str],
    local_media_by_path_key: dict[str, str],
) -> None:
    if not local_media_by_path_key:
        return

    aliases_by_path: dict[str, set[str]] = {}

    for row in db.list_all_attachments_for_post(post_id):
        remote_url = _optional_str(row["remote_url"])
        if not remote_url:
            continue
        path_key = _remote_path_key(remote_url)
        if not path_key:
            continue
        bucket = aliases_by_path.setdefault(path_key, set())
        name_key = _attachment_collapse_key(row["name"])
        if name_key:
            bucket.add(name_key)
        bucket.update(_remote_filename_alias_keys(remote_url))

    for version in db.list_post_versions(post_id):
        metadata = _safe_load_metadata(version["metadata_json"])
        for entry in _iter_metadata_media_entries(metadata):
            raw_path = entry.get("path") or entry.get("url")
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            server = entry.get("server")
            resolved = _resolve_media_url(raw_path.strip(), server if isinstance(server, str) else None)
            if not resolved:
                continue
            path_key = _remote_path_key(resolved)
            if not path_key:
                continue
            name_key = _attachment_collapse_key(entry.get("name"))
            if not name_key:
                continue
            aliases_by_path.setdefault(path_key, set()).add(name_key)

    for path_key, alias_names in aliases_by_path.items():
        local_url = local_media_by_path_key.get(path_key)
        if not local_url:
            continue
        for alias in alias_names:
            if alias and alias not in local_media_by_name:
                local_media_by_name[alias] = local_url


def _build_remote_media_by_name(post: Any, attachments: list[Any]) -> dict[str, str]:
    remote_media_by_name: dict[str, str] = {}
    remote_media_priority: dict[str, int] = {}
    kemono_urls_by_ext: dict[str, set[str]] = {}

    def register_url(url: str, *, name: Any, priority: int) -> None:
        parsed = urlparse(url)
        ext = Path(parsed.path).suffix.lower()
        if ext and parsed.netloc.lower().endswith("kemono.cr"):
            kemono_urls_by_ext.setdefault(ext, set()).add(url)

        filename = Path(parsed.path).name.lower()
        if filename:
            _assign_preferred(
                remote_media_by_name,
                remote_media_priority,
                filename,
                url,
                priority,
            )

        if isinstance(name, str):
            plain_name = name.strip().lower()
            if plain_name:
                _assign_preferred(
                    remote_media_by_name,
                    remote_media_priority,
                    plain_name,
                    url,
                    priority,
                )
            normalized_name = sanitize_filename(name).lower()
            if normalized_name:
                _assign_preferred(
                    remote_media_by_name,
                    remote_media_priority,
                    normalized_name,
                    url,
                    priority,
                )

    for attachment in attachments:
        remote_url = _preferred_remote_url_for_access(
            str(attachment["remote_url"]),
            attachment["name"],
        )
        kind_priority = _media_kind_priority(attachment["kind"])
        register_url(remote_url, name=attachment["name"], priority=kind_priority)

    metadata = _safe_load_metadata(post["metadata_json"])
    for entry in _iter_metadata_media_entries(metadata):
        raw_path = entry.get("path") or entry.get("url")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        server = entry.get("server")
        if not isinstance(server, str):
            server = None
        resolved = _resolve_media_url(raw_path.strip(), server)
        if not isinstance(resolved, str) or not resolved.strip():
            continue
        metadata_name = entry.get("name")
        preferred = _preferred_remote_url_for_access(resolved, metadata_name)
        register_url(preferred, name=metadata_name, priority=15)

    for ext, urls in kemono_urls_by_ext.items():
        if len(urls) == 1:
            remote_media_by_name[f"__ext_unique__:{ext}"] = next(iter(urls))
    return remote_media_by_name


def _safe_load_metadata(raw_metadata: str | None) -> dict[str, Any]:
    if not isinstance(raw_metadata, str) or not raw_metadata.strip():
        return {}
    try:
        loaded = json.loads(raw_metadata)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


_THUMBNAIL_FOCUS_KEY = "_local_thumbnail_focus"


def _clamp_thumbnail_focus(value: Any, *, fallback: float = 50.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return fallback
    return max(0.0, min(100.0, numeric))


def _extract_thumbnail_focus_from_metadata(metadata: dict[str, Any]) -> tuple[float, float]:
    focus_block = metadata.get(_THUMBNAIL_FOCUS_KEY)
    if not isinstance(focus_block, dict):
        return 50.0, 50.0
    x = _clamp_thumbnail_focus(focus_block.get("x"), fallback=50.0)
    y = _clamp_thumbnail_focus(focus_block.get("y"), fallback=50.0)
    return x, y


def _extract_thumbnail_focus_from_raw_metadata(raw_metadata: Any) -> tuple[float, float]:
    parsed = _safe_load_metadata(raw_metadata if isinstance(raw_metadata, str) else None)
    return _extract_thumbnail_focus_from_metadata(parsed)


def _set_thumbnail_focus_in_metadata(
    metadata: dict[str, Any],
    x: float | None,
    y: float | None,
) -> dict[str, Any]:
    updated = dict(metadata)
    if x is None or y is None:
        updated.pop(_THUMBNAIL_FOCUS_KEY, None)
        return updated
    updated[_THUMBNAIL_FOCUS_KEY] = {
        "x": _clamp_thumbnail_focus(x, fallback=50.0),
        "y": _clamp_thumbnail_focus(y, fallback=50.0),
    }
    return updated


def _parse_thumbnail_focus_inputs(
    raw_x: str | None,
    raw_y: str | None,
    *,
    fallback_x: float,
    fallback_y: float,
) -> tuple[float, float]:
    return (
        _clamp_thumbnail_focus(raw_x, fallback=fallback_x),
        _clamp_thumbnail_focus(raw_y, fallback=fallback_y),
    )


def _dedupe_post_detail_attachments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    key_order: list[str] = []

    for row in rows:
        key = _post_detail_attachment_key(row)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
            key_order.append(key)
            continue
        if _should_replace_post_detail_attachment(existing, row):
            by_key[key] = row

    for key in key_order:
        winner = by_key.get(key)
        if winner is not None:
            deduped.append(winner)
    return deduped


def _post_detail_attachment_key(row: dict[str, Any]) -> str:
    local_path = _optional_str(row.get("local_path"))
    if local_path:
        return f"local:{local_path.lower()}"

    name_key = _attachment_collapse_key(row.get("name"))
    remote_key = _remote_path_key(_optional_str(row.get("remote_url")) or "")
    if name_key and remote_key:
        return f"remote_name:{name_key}|{remote_key}"
    if remote_key:
        return f"remote:{remote_key}"
    if name_key:
        return f"name:{name_key}"
    return f"id:{row.get('id')}"


def _should_replace_post_detail_attachment(existing: dict[str, Any], candidate: dict[str, Any]) -> bool:
    existing_local = bool(existing.get("local_available"))
    candidate_local = bool(candidate.get("local_available"))
    if candidate_local != existing_local:
        return candidate_local
    return _media_kind_priority(candidate.get("kind")) > _media_kind_priority(existing.get("kind"))


def _dedupe_managed_attachment_local_files(
    *,
    files_base: Path,
    managed_attachments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in managed_attachments:
        name_key = _attachment_collapse_key(item.get("name"))
        if not name_key:
            continue
        grouped.setdefault(name_key, []).append(item)

    hash_cache: dict[str, str | None] = {}

    for name_key, group in grouped.items():
        if len(group) < 2:
            continue

        for idx, left in enumerate(group):
            for right in group[idx + 1 :]:
                if not _managed_items_refer_same_file(
                    left,
                    right,
                    files_base=files_base,
                    hash_cache=hash_cache,
                ):
                    continue
                canonical_local = _pick_canonical_local_path(
                    files_base=files_base,
                    name_key=name_key,
                    local_paths=[
                        _optional_str(left.get("local_path")),
                        _optional_str(right.get("local_path")),
                    ],
                )
                if not canonical_local:
                    continue
                for item in (left, right):
                    local_path = _optional_str(item.get("local_path"))
                    if local_path == canonical_local:
                        continue
                    if local_path:
                        _remove_local_attachment_file(files_base, local_path)
                    item["local_path"] = canonical_local
                    if local_path:
                        hash_cache.pop(local_path, None)
                canonical_hash = _local_file_content_hash(files_base, canonical_local, hash_cache)
                if canonical_hash:
                    hash_cache[canonical_local] = canonical_hash
    return managed_attachments


def _managed_items_refer_same_file(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    files_base: Path,
    hash_cache: dict[str, str | None],
) -> bool:
    left_remote = _optional_str(left.get("remote_url"))
    right_remote = _optional_str(right.get("remote_url"))
    if left_remote and right_remote and _remote_path_key(left_remote) == _remote_path_key(right_remote):
        return True

    left_local = _optional_str(left.get("local_path"))
    right_local = _optional_str(right.get("local_path"))
    if left_local and right_local and left_local == right_local:
        return True

    if not left_local or not right_local:
        return False
    left_hash = _local_file_content_hash(files_base, left_local, hash_cache)
    right_hash = _local_file_content_hash(files_base, right_local, hash_cache)
    return bool(left_hash and right_hash and left_hash == right_hash)


def _pick_canonical_local_path(
    *,
    files_base: Path,
    name_key: str,
    local_paths: list[str | None],
) -> str | None:
    cleaned = [path for path in local_paths if isinstance(path, str) and path.strip()]
    if not cleaned:
        return None

    existing = [path for path in cleaned if _is_valid_file(files_base / path)]
    for path in existing:
        if _attachment_collapse_key(Path(path).name) == name_key:
            return path
    if existing:
        return existing[0]
    for path in cleaned:
        if _attachment_collapse_key(Path(path).name) == name_key:
            return path
    return cleaned[0]


def _local_file_content_hash(
    files_base: Path,
    local_path: str,
    cache: dict[str, str | None],
) -> str | None:
    if local_path in cache:
        return cache[local_path]

    path = files_base / local_path
    if not _is_valid_file(path):
        cache[local_path] = None
        return None

    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(64 * 1024), b""):
                if not chunk:
                    break
                digest.update(chunk)
    except OSError:
        cache[local_path] = None
        return None
    value = digest.hexdigest()
    cache[local_path] = value
    return value


def _iter_metadata_media_entries(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = [metadata]
    nested_post = metadata.get("post")
    if isinstance(nested_post, dict):
        sources.append(nested_post)

    for source in sources:
        file_item = source.get("file")
        if isinstance(file_item, dict):
            entries.append(file_item)

        shared_file = source.get("shared_file")
        if isinstance(shared_file, dict):
            entries.append(shared_file)

        attachments_list = source.get("attachments")
        if isinstance(attachments_list, list):
            for item in attachments_list:
                if isinstance(item, dict):
                    entries.append(item)
    return entries


def _remote_path_key(raw_path_or_url: str) -> str:
    parsed = urlparse(raw_path_or_url)
    path = parsed.path if parsed.path else raw_path_or_url
    cleaned = path.strip().lower()
    if not cleaned:
        return ""
    if cleaned.startswith("/data/"):
        cleaned = cleaned[5:]
    cleaned = re.sub(r"/{2,}", "/", cleaned)
    return cleaned


def _remote_filename_alias_keys(raw_url: str | None) -> set[str]:
    if not isinstance(raw_url, str) or not raw_url.strip():
        return set()

    parsed = urlparse(raw_url)
    aliases: set[str] = set()

    path_name = Path(parsed.path).name
    path_key = _attachment_collapse_key(path_name)
    if path_key:
        aliases.add(path_key)

    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_norm = key.strip().lower()
        if key_norm not in {"f", "file", "filename", "name", "download", "fn"}:
            continue
        value_key = _attachment_collapse_key(value)
        if value_key:
            aliases.add(value_key)

    return aliases


def _build_target_attachment_index(
    db: LibraryDBLike,
    *,
    files_base: Path,
    post_ids: list[int],
) -> dict[str, dict[str, int]]:
    index: dict[str, dict[str, int]] = {str(post_id): {} for post_id in post_ids}
    rows_by_post_id: dict[int, list[Any]] = {}
    for row in db.list_all_attachments_for_posts(post_ids):
        row_post_id = int(row["post_id"])
        rows_by_post_id.setdefault(row_post_id, []).append(row)

    for post_id in post_ids:
        key_state: dict[str, int] = {}
        for row in rows_by_post_id.get(post_id, []):
            name = row["name"]
            remote_url = row["remote_url"]
            local_path = row["local_path"]

            has_local = False
            if isinstance(local_path, str) and local_path.strip():
                has_local = _is_valid_file(files_base / local_path)

            raw_keys: list[str] = []
            if isinstance(name, str) and name.strip():
                raw_keys.append(f"name:{sanitize_filename(name).lower()}")
            if isinstance(remote_url, str) and remote_url.strip():
                path_key = _remote_path_key(remote_url)
                if path_key:
                    raw_keys.append(f"path:{path_key}")

            for item_key in raw_keys:
                existing_state = key_state.get(item_key, 0)
                if has_local:
                    key_state[item_key] = 2
                elif existing_state == 0:
                    key_state[item_key] = 1
        index[str(post_id)] = key_state
    return index


def _media_kind_priority(kind: Any) -> int:
    if not isinstance(kind, str):
        return 0
    order = {
        "file": 50,
        "attachment": 45,
        "inline_media": 44,
        "shared_file": 40,
        "video": 35,
        "embed_media": 30,
        "thumbnail": 20,
        "inline_only": 10,
    }
    return order.get(kind, 5)


def _assign_preferred(
    target_map: dict[str, str],
    target_priority: dict[str, int],
    key: str,
    value: str,
    priority: int,
) -> None:
    if not key:
        return
    existing_priority = target_priority.get(key)
    if existing_priority is None or priority > existing_priority:
        target_map[key] = value
        target_priority[key] = priority


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _format_bytes_for_display(value: Any) -> str:
    try:
        size = int(value)
    except (TypeError, ValueError):
        return "-"
    if size < 0:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    scaled = float(size)
    unit = units[0]
    for candidate in units:
        unit = candidate
        if scaled < 1024.0 or candidate == units[-1]:
            break
        scaled /= 1024.0
    if unit == "B":
        return f"{int(scaled)} {unit}"
    return f"{scaled:.1f} {unit}"


def _build_attachment_retry_display_name(row: dict[str, Any]) -> str:
    creator_name = _optional_str(row.get("creator_name")) or f"Creator {int(row['creator_id'])}"
    post_title = _optional_str(row.get("post_title")) or f"Post {int(row['post_id'])}"
    version_label = _optional_str(row.get("version_label"))
    attachment_name = _optional_str(row.get("name")) or "attachment"
    parts = [creator_name, post_title]
    if version_label:
        parts.append(version_label)
    parts.append(attachment_name)
    return " / ".join(parts)


def _filter_attachment_inventory_rows(
    rows: list[dict[str, Any]],
    *,
    search_text: str,
    state_filter: str,
    media_filter: str,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    search_lower = search_text.strip().lower()
    for row in rows:
        if search_lower and search_lower not in str(row["search_blob"]):
            continue
        if state_filter == "missing" and row["local_available"]:
            continue
        if state_filter == "local" and not row["local_available"]:
            continue
        if media_filter == "images" and not row["is_image"]:
            continue
        if media_filter == "other" and row["is_image"]:
            continue
        filtered.append(row)
    return filtered


def _sort_attachment_inventory_rows(rows: list[dict[str, Any]], *, sort_key: str) -> list[dict[str, Any]]:
    if sort_key == "creator":
        return rows

    def recent_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        published = _optional_str(row["post_published_at"]) or ""
        return (
            published == "",
            published,
            row["creator_name"].lower(),
            row["series_name"].lower(),
            row["post_title"].lower(),
            row["name"].lower(),
            row["id"],
        )

    def size_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        size = int(row["file_size"] or -1)
        return (
            -(size if size >= 0 else -1),
            row["creator_name"].lower(),
            row["series_name"].lower(),
            row["post_title"].lower(),
            row["name"].lower(),
            row["id"],
        )

    def name_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row["name"].lower(),
            row["creator_name"].lower(),
            row["series_name"].lower(),
            row["post_title"].lower(),
            row["id"],
        )

    key_func = {
        "recent": recent_sort_key,
        "size": size_sort_key,
        "name": name_sort_key,
    }.get(sort_key, name_sort_key)
    reverse = sort_key == "recent"
    return sorted(rows, key=key_func, reverse=reverse)


def _summarize_attachment_inventory(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_size = sum(int(row["file_size"] or 0) for row in rows)
    missing_count = sum(1 for row in rows if not row["local_available"])
    image_count = sum(1 for row in rows if row["is_image"])
    creator_ids = {int(row["creator_id"]) for row in rows}
    series_keys = {
        (int(row["creator_id"]), int(row["series_id"]) if row["series_id"] is not None else None)
        for row in rows
    }
    post_ids = {int(row["post_id"]) for row in rows}
    return {
        "file_count": len(rows),
        "total_size": total_size,
        "missing_count": missing_count,
        "image_count": image_count,
        "creator_count": len(creator_ids),
        "series_count": len(series_keys),
        "post_count": len(post_ids),
    }


def _build_attachment_inventory_tree(rows: list[dict[str, Any]], *, sort_key: str = "creator") -> list[dict[str, Any]]:
    creator_nodes: dict[int, dict[str, Any]] = {}
    series_sequence = 0
    for row in rows:
        row_published = _optional_str(row["post_published_at"]) or ""
        creator_id = int(row["creator_id"])
        creator_node = creator_nodes.get(creator_id)
        if creator_node is None:
            creator_node = {
                "id": creator_id,
                "name": row["creator_name"],
                "files": [],
                "series_nodes": [],
                "series_lookup": {},
                "file_count": 0,
                "missing_count": 0,
                "size_bytes": 0,
                "latest_published_at": "",
            }
            creator_nodes[creator_id] = creator_node

        series_key = (int(row["series_id"]) if row["series_id"] is not None else None, row["series_name"])
        series_node = creator_node["series_lookup"].get(series_key)
        if series_node is None:
            series_sequence += 1
            series_node = {
                "uid": f"series-{series_sequence}",
                "series_id": series_key[0],
                "name": row["series_name"],
                "posts": [],
                "post_lookup": {},
                "file_count": 0,
                "missing_count": 0,
                "size_bytes": 0,
                "latest_published_at": "",
            }
            creator_node["series_lookup"][series_key] = series_node
            creator_node["series_nodes"].append(series_node)

        post_id = int(row["post_id"])
        post_node = series_node["post_lookup"].get(post_id)
        if post_node is None:
            post_node = {
                "post_id": post_id,
                "title": row["post_title"],
                "published_at": row["post_published_at"],
                "attachments": [],
                "file_count": 0,
                "missing_count": 0,
                "size_bytes": 0,
                "latest_published_at": row_published,
            }
            series_node["post_lookup"][post_id] = post_node
            series_node["posts"].append(post_node)

        post_node["attachments"].append(row)
        post_node["file_count"] += 1
        post_node["missing_count"] += 0 if row["local_available"] else 1
        post_node["size_bytes"] += int(row["file_size"] or 0)

        series_node["file_count"] += 1
        series_node["missing_count"] += 0 if row["local_available"] else 1
        series_node["size_bytes"] += int(row["file_size"] or 0)

        creator_node["file_count"] += 1
        creator_node["missing_count"] += 0 if row["local_available"] else 1
        creator_node["size_bytes"] += int(row["file_size"] or 0)
        if row_published and row_published > str(creator_node["latest_published_at"] or ""):
            creator_node["latest_published_at"] = row_published
        if row_published and row_published > str(series_node["latest_published_at"] or ""):
            series_node["latest_published_at"] = row_published

    creator_list = list(creator_nodes.values())
    if sort_key == "size":
        for creator_node in creator_list:
            for series_node in creator_node["series_nodes"]:
                series_node["posts"].sort(
                    key=lambda post: (
                        -int(post["size_bytes"] or 0),
                        str(post["title"]).lower(),
                        int(post["post_id"]),
                    )
                )
                for post_node in series_node["posts"]:
                    post_node["attachments"].sort(
                        key=lambda row: (
                            -int(row["file_size"] or 0),
                            str(row["name"]).lower(),
                            int(row["id"]),
                        )
                    )
            creator_node["series_nodes"].sort(
                key=lambda series: (
                    -int(series["size_bytes"] or 0),
                    str(series["name"]).lower(),
                    -1 if series["series_id"] is None else int(series["series_id"]),
                )
            )
        creator_list.sort(
            key=lambda creator: (
                -int(creator["size_bytes"] or 0),
                str(creator["name"]).lower(),
                int(creator["id"]),
            )
        )
    elif sort_key == "recent":
        for creator_node in creator_list:
            for series_node in creator_node["series_nodes"]:
                series_node["posts"].sort(
                    key=lambda post: (
                        _optional_str(post["latest_published_at"]) not in {None, ""},
                        str(post["latest_published_at"] or ""),
                        str(post["title"]).lower(),
                        int(post["post_id"]),
                    ),
                    reverse=True,
                )
                for post_node in series_node["posts"]:
                    post_node["attachments"].sort(
                        key=lambda row: (
                            str(row["name"]).lower(),
                            int(row["id"]),
                        )
                    )
            creator_node["series_nodes"].sort(
                key=lambda series: (
                    _optional_str(series["latest_published_at"]) not in {None, ""},
                    str(series["latest_published_at"] or ""),
                    str(series["name"]).lower(),
                    -1 if series["series_id"] is None else int(series["series_id"]),
                ),
                reverse=True,
            )
        creator_list.sort(
            key=lambda creator: (
                _optional_str(creator["latest_published_at"]) not in {None, ""},
                str(creator["latest_published_at"] or ""),
                str(creator["name"]).lower(),
                int(creator["id"]),
            ),
            reverse=True,
        )
    elif sort_key == "name":
        for creator_node in creator_list:
            for series_node in creator_node["series_nodes"]:
                series_node["posts"].sort(
                    key=lambda post: (
                        str(post["title"]).lower(),
                        int(post["post_id"]),
                    )
                )
                for post_node in series_node["posts"]:
                    post_node["attachments"].sort(
                        key=lambda row: (
                            str(row["name"]).lower(),
                            int(row["id"]),
                        )
                    )
            creator_node["series_nodes"].sort(
                key=lambda series: (
                    str(series["name"]).lower(),
                    -1 if series["series_id"] is None else int(series["series_id"]),
                )
            )
        creator_list.sort(
            key=lambda creator: (
                str(creator["name"]).lower(),
                int(creator["id"]),
            )
        )
    return creator_list


def _filter_retry_scope_rows(
    rows: list[dict[str, Any]],
    *,
    scope: str,
    scope_id_raw: str,
) -> list[dict[str, Any]]:
    if scope == "attachment":
        try:
            attachment_id = int(scope_id_raw)
        except ValueError:
            return []
        return [row for row in rows if int(row["id"]) == attachment_id]
    if scope == "post":
        try:
            post_id = int(scope_id_raw)
        except ValueError:
            return []
        return [row for row in rows if int(row["post_id"]) == post_id]
    if scope == "creator":
        try:
            creator_id = int(scope_id_raw)
        except ValueError:
            return []
        return [row for row in rows if int(row["creator_id"]) == creator_id]
    if scope == "series":
        if scope_id_raw.startswith("unsorted:"):
            try:
                creator_id = int(scope_id_raw.split(":", 1)[1])
            except ValueError:
                return []
            return [row for row in rows if row["series_id"] is None and int(row["creator_id"]) == creator_id]
        try:
            series_id = int(scope_id_raw)
        except ValueError:
            return []
        return [row for row in rows if row["series_id"] == series_id]
    return list(rows)


_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"}
_NON_IMAGE_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".txt",
    ".pdf",
    ".mp4",
    ".webm",
    ".mkv",
    ".mov",
    ".avi",
    ".mp3",
    ".wav",
    ".ogg",
    ".flac",
}


def _extract_extension(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    source = parsed.path if parsed.path else value
    return Path(source).suffix.lower()


def _is_likely_image_attachment(
    *,
    remote_url: str | None,
    name: Any,
    local_path: str | None,
    kind: Any,
) -> bool:
    kind_text = str(kind).strip().lower() if kind is not None else ""
    if kind_text == "thumbnail":
        return True

    ext_candidates = {
        _extract_extension(remote_url),
        _extract_extension(str(name) if isinstance(name, str) else None),
        _extract_extension(local_path),
    }
    ext_candidates.discard("")
    if any(ext in _IMAGE_EXTENSIONS for ext in ext_candidates):
        return True
    if any(ext in _NON_IMAGE_EXTENSIONS for ext in ext_candidates):
        return False

    lowered_url = (remote_url or "").lower()
    if "/images/" in lowered_url or "/image/" in lowered_url:
        return True
    if "/files/" in lowered_url:
        return False
    if "pixiv.pximg.net" in lowered_url:
        return True
    if kind_text in {"inline_only"}:
        return True
    return False


def _detect_image_mime(path: Path) -> str | None:
    try:
        head = path.read_bytes()[:512]
    except OSError:
        return None
    if len(head) >= 3 and head[:3] == b"\xFF\xD8\xFF":
        return "image/jpeg"
    if len(head) >= 8 and head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if len(head) >= 6 and (head[:6] == b"GIF87a" or head[:6] == b"GIF89a"):
        return "image/gif"
    if len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    if len(head) >= 2 and head[:2] == b"BM":
        return "image/bmp"
    try:
        text_head = head.decode("utf-8", errors="ignore").lstrip().lower()
    except Exception:  # noqa: BLE001
        text_head = ""
    if text_head.startswith("<?xml") or text_head.startswith("<svg") or "<svg" in text_head[:256]:
        return "image/svg+xml"
    return None


def _attachment_collapse_key(name: Any) -> str:
    if not isinstance(name, str):
        return ""
    normalized = sanitize_filename(name).strip().lower()
    return normalized


def _remove_local_attachment_file(files_base: Path, local_path: str) -> None:
    target = files_base / local_path
    try:
        if target.is_file():
            target.unlink()
    except OSError:
        return


def _rename_local_attachment_file(
    *,
    files_base: Path,
    local_path: str,
    desired_name: str,
    fallback_name: str,
) -> str:
    current = files_base / local_path
    if not current.exists() or not current.is_file():
        return local_path

    safe_name = sanitize_filename(desired_name.strip()) or sanitize_filename(fallback_name.strip()) or current.name
    suffix = Path(safe_name).suffix
    if not suffix:
        inherited_suffix = Path(current.name).suffix or Path(fallback_name).suffix
        if inherited_suffix:
            safe_name = f"{safe_name}{inherited_suffix}"

    destination = current.with_name(safe_name)
    if destination == current:
        return local_path

    if destination.exists():
        if _paths_have_same_content(current, destination):
            try:
                current.unlink()
            except OSError:
                pass
            return destination.relative_to(files_base).as_posix()
        stem = destination.stem
        ext = destination.suffix
        counter = 2
        while True:
            candidate = destination.with_name(f"{stem}_{counter}{ext}")
            if not candidate.exists():
                destination = candidate
                break
            counter += 1

    try:
        current.rename(destination)
    except OSError:
        return local_path

    return destination.relative_to(files_base).as_posix()


def _paths_have_same_content(first: Path, second: Path) -> bool:
    if not first.is_file() or not second.is_file():
        return False
    try:
        if first.stat().st_size != second.stat().st_size:
            return False
    except OSError:
        return False

    left = hashlib.sha256()
    right = hashlib.sha256()
    try:
        with first.open("rb") as left_handle:
            for chunk in iter(lambda: left_handle.read(64 * 1024), b""):
                if not chunk:
                    break
                left.update(chunk)
        with second.open("rb") as right_handle:
            for chunk in iter(lambda: right_handle.read(64 * 1024), b""):
                if not chunk:
                    break
                right.update(chunk)
    except OSError:
        return False
    return left.digest() == right.digest()


def _extract_thumbnail_from_payload(
    normalized_payload: dict[str, Any],
    raw_payload: dict[str, Any],
) -> tuple[str | None, str | None]:
    file_item = normalized_payload.get("file")
    if not isinstance(file_item, dict):
        nested = raw_payload.get("post")
        if isinstance(nested, dict):
            file_item = nested.get("file")
    if not isinstance(file_item, dict):
        return None, None

    name = _optional_str(file_item.get("name"))
    raw_path = _optional_str(file_item.get("path") or file_item.get("url"))
    server = _optional_str(file_item.get("server"))
    remote_url = _resolve_media_url(raw_path, server)

    if not name and remote_url:
        parsed = urlparse(remote_url)
        inferred = Path(parsed.path).name
        name = inferred or None
    return name, remote_url


def _resolve_media_url(raw_path: str | None, server: str | None) -> str | None:
    if not raw_path:
        return None
    if raw_path.startswith(("http://", "https://")):
        return raw_path
    if server and server.startswith(("http://", "https://")):
        if raw_path.startswith("/"):
            return f"{server.rstrip('/')}{raw_path}"
        return f"{server.rstrip('/')}/{raw_path.lstrip('/')}"
    return to_absolute_kemono_url(raw_path)


def _find_thumbnail_local_path(
    saved_attachments: list[dict[str, Any]],
    *,
    thumbnail_name: str | None,
    thumbnail_remote_url: str | None,
) -> str | None:
    if thumbnail_remote_url:
        for attachment in saved_attachments:
            if attachment.get("remote_url") == thumbnail_remote_url and attachment.get("local_path"):
                return str(attachment["local_path"])

    if thumbnail_name:
        normalized_thumbnail_name = sanitize_filename(thumbnail_name).lower()
        for attachment in saved_attachments:
            local = attachment.get("local_path")
            name = attachment.get("name")
            if not local or not isinstance(name, str):
                continue
            if sanitize_filename(name).lower() == normalized_thumbnail_name:
                return str(local)
    return None


def _import_post_into_library(
    db: LibraryDBLike,
    *,
    files_base: Path,
    icons_base: Path,
    creator_id: int,
    series_id: int | None,
    service: str,
    user_id: str,
    post_id: str,
    import_target_mode: str,
    target_post_id: int | None,
    overwrite_matching_version: bool,
    set_as_default: bool,
    version_label: str | None,
    version_language: str | None,
    requested_title: str | None,
    requested_content: str | None,
    requested_published_at: str | None,
    requested_edited_at: str | None,
    requested_next_external_post_id: str | None,
    requested_prev_external_post_id: str | None,
    tags_text: str | None,
    field_presence: dict[str, bool],
    selected_attachment_indices: set[str],
    progress_callback: Callable[[int, int, str | None], None] | None = None,
) -> tuple[int, int]:
    creator = db.get_creator(creator_id)
    if not creator:
        raise ValueError("Creator not found.")

    series_id = _validate_import_series_selection(db, creator_id=creator_id, series_id=series_id)
    exact_match_post = _find_import_source_match(
        db,
        service=service,
        user_id=user_id,
        post_id=post_id,
        creator_id=creator_id,
    )

    local_post_id: int | None = None
    imported_into_new_local_post = exact_match_post is None and import_target_mode != "existing"
    if exact_match_post:
        local_post_id = int(exact_match_post["id"])
        if import_target_mode == "new":
            raise ValueError("This source already exists locally. Import as a version or overwrite it.")
        if import_target_mode == "existing" and target_post_id and target_post_id != local_post_id:
            raise ValueError(
                f"This source already exists under local post #{local_post_id}. "
                "Pick that post or overwrite the existing version there."
            )
    elif import_target_mode == "existing":
        if not target_post_id:
            raise ValueError("Pick a target post for version import.")
        target_post = db.get_post(target_post_id)
        if not target_post or int(target_post["creator_id"]) != creator_id:
            raise ValueError("Target post was not found for this creator.")
        local_post_id = target_post_id

    post_ref = KemonoPostRef(service=service, user_id=user_id, post_id=post_id)
    raw_payload = fetch_post_json(post_ref)
    payload = normalize_post_payload(raw_payload)
    creator_icon_update = _prepare_creator_icon_update(
        creator,
        icons_base=icons_base,
        service=service,
        user_id=user_id,
    )

    all_attachments = extract_attachments(raw_payload)
    selected_attachments = [
        candidate for idx, candidate in enumerate(all_attachments) if str(idx) in selected_attachment_indices
    ]
    if progress_callback:
        progress_callback(0, len(selected_attachments), None)

    title = _resolve_import_title(requested_title, payload.get("title"), service=service, post_id=post_id)
    content = _resolve_import_content(requested_content, payload.get("content"))
    thumbnail_name, thumbnail_remote_url = _extract_thumbnail_from_payload(payload, raw_payload)
    published_at = _resolve_import_optional_metadata(
        requested_published_at,
        payload.get("published"),
        field_present=field_presence.get("published_at", False),
    )
    edited_at = _resolve_import_optional_metadata(
        requested_edited_at,
        payload.get("edited"),
        field_present=field_presence.get("edited_at", False),
    )
    next_external_post_id = _resolve_import_optional_metadata(
        requested_next_external_post_id,
        payload.get("next"),
        field_present=field_presence.get("next_external_post_id", False),
    )
    prev_external_post_id = _resolve_import_optional_metadata(
        requested_prev_external_post_id,
        payload.get("prev"),
        field_present=field_presence.get("prev_external_post_id", False),
    )
    source_url = post_ref.canonical_url
    created_files: set[Path] = set()
    created_icon_path: Path | None = creator_icon_update[2] if creator_icon_update is not None else None
    download_root: Path | None = None
    try:
        with db.transaction() as conn:
            db.attach_creator_external(creator_id, service=service, external_user_id=user_id, conn=conn)
            if creator_icon_update is not None:
                db.update_creator_icon(
                    creator_id,
                    icon_remote_url=creator_icon_update[0],
                    icon_local_path=creator_icon_update[1],
                    conn=conn,
                )
            if imported_into_new_local_post:
                local_post_id = db.upsert_post(
                    creator_id=creator_id,
                    series_id=series_id,
                    service=service,
                    external_user_id=user_id,
                    external_post_id=post_id,
                    title=str(title),
                    content=str(content),
                    metadata=raw_payload,
                    source_url=source_url,
                    thumbnail_name=thumbnail_name,
                    thumbnail_remote_url=thumbnail_remote_url,
                    thumbnail_local_path=None,
                    published_at=published_at,
                    edited_at=edited_at,
                    next_external_post_id=next_external_post_id,
                    prev_external_post_id=prev_external_post_id,
                    conn=conn,
                )
            if local_post_id is None:
                raise RuntimeError("Import target resolution failed.")

            if series_id is not None:
                db.update_post_series(local_post_id, series_id, conn=conn)

            existing_version = db.find_version_by_source(
                post_id=local_post_id,
                service=service,
                external_user_id=user_id,
                external_post_id=post_id,
                conn=conn,
            )
            conflicting_version = None
            if not existing_version:
                conflicting_version = db.find_version_by_source_global(
                    service=service,
                    external_user_id=user_id,
                    external_post_id=post_id,
                    conn=conn,
                )
                if conflicting_version and int(conflicting_version["post_id"]) != local_post_id:
                    raise ValueError(
                        "This source version already exists locally under "
                        f"post #{int(conflicting_version['post_id'])} as version #{int(conflicting_version['id'])}."
                    )
            if existing_version and not overwrite_matching_version and not imported_into_new_local_post:
                raise ValueError("Matching source version already exists. Enable overwrite to replace it.")

            is_new_version = existing_version is None or imported_into_new_local_post
            resolved_version_label = _resolve_import_version_label(version_label, payload.get("title"), is_new_version)
            resolved_version_language = _optional_str(version_language)

            if existing_version:
                version_id = int(existing_version["id"])
                db.update_post_version(
                    version_id=version_id,
                    label=resolved_version_label,
                    language=resolved_version_language,
                    title=str(title),
                    content=str(content),
                    thumbnail_name=thumbnail_name,
                    thumbnail_remote_url=thumbnail_remote_url,
                    thumbnail_local_path=None,
                    published_at=published_at,
                    edited_at=edited_at,
                    next_external_post_id=next_external_post_id,
                    prev_external_post_id=prev_external_post_id,
                    metadata=raw_payload,
                    source_url=source_url,
                    conn=conn,
                )
            else:
                version_id = db.create_post_version(
                    post_id=local_post_id,
                    label=resolved_version_label,
                    language=resolved_version_language,
                    origin_kind=db.VERSION_ORIGIN_SOURCE,
                    source_service=service,
                    source_user_id=user_id,
                    source_post_id=post_id,
                    title=str(title),
                    content=str(content),
                    metadata=raw_payload,
                    source_url=source_url,
                    thumbnail_name=thumbnail_name,
                    thumbnail_remote_url=thumbnail_remote_url,
                    thumbnail_local_path=None,
                    published_at=published_at,
                    edited_at=edited_at,
                    next_external_post_id=next_external_post_id,
                    prev_external_post_id=prev_external_post_id,
                    set_default=set_as_default,
                    conn=conn,
                )

            download_root = files_base / f"post_{local_post_id}"
            existing_rows = db.list_all_attachments_for_post(local_post_id, conn=conn)
            existing_by_remote, existing_by_path_key, existing_by_name = _build_existing_file_indexes(
                files_base,
                existing_rows,
            )

            saved: list[dict[str, Any]] = []
            total = len(selected_attachments)
            for idx, candidate in enumerate(selected_attachments, start=1):
                filename = sanitize_filename(candidate.name)
                path_key = _remote_path_key(candidate.remote_url)
                destination = (
                    existing_by_remote.get(candidate.remote_url)
                    or existing_by_path_key.get(path_key)
                    or existing_by_name.get(filename)
                    or (download_root / filename)
                )
                needs_download = not _is_valid_file(destination)
                if needs_download:
                    used_remote_url = _download_with_fallback_remote_url(
                        candidate.remote_url,
                        destination,
                        candidate.name,
                    )
                    if used_remote_url and destination is not None and _is_valid_file(destination):
                        created_files.add(destination)
                    if not used_remote_url:
                        destination = None
                if destination and _is_valid_file(destination):
                    local_path = destination.relative_to(files_base).as_posix()
                else:
                    local_path = None
                saved.append(
                    {
                        "name": candidate.name,
                        "remote_url": candidate.remote_url,
                        "local_path": local_path,
                        "kind": candidate.kind,
                    }
                )
                if progress_callback:
                    progress_callback(idx, total, candidate.name)

            thumbnail_local_path = _find_thumbnail_local_path(
                saved,
                thumbnail_name=thumbnail_name,
                thumbnail_remote_url=thumbnail_remote_url,
            )
            db.replace_attachments(local_post_id, saved, version_id=version_id, conn=conn)
            db.update_post_version(
                version_id=version_id,
                label=resolved_version_label,
                language=resolved_version_language,
                title=str(title),
                content=str(content),
                thumbnail_name=thumbnail_name,
                thumbnail_remote_url=thumbnail_remote_url,
                thumbnail_local_path=thumbnail_local_path,
                published_at=published_at,
                edited_at=edited_at,
                next_external_post_id=next_external_post_id,
                prev_external_post_id=prev_external_post_id,
                metadata=raw_payload,
                source_url=source_url,
                conn=conn,
            )
            tags = _parse_tags_text(tags_text) if tags_text is not None else _extract_tags(payload)
            db.replace_tags(local_post_id, tags, version_id=version_id, conn=conn)
            db.replace_previews(local_post_id, _extract_previews(raw_payload), version_id=version_id, conn=conn)
            if set_as_default:
                db.set_default_post_version(local_post_id, version_id, conn=conn)
            else:
                db.sync_post_from_default_version(local_post_id, conn=conn)
    except Exception:
        if created_icon_path is not None:
            try:
                if created_icon_path.is_file():
                    created_icon_path.unlink()
            except OSError:
                pass
        for path in created_files:
            try:
                if path.is_file():
                    path.unlink()
            except OSError:
                pass
        if imported_into_new_local_post and download_root is not None:
            shutil.rmtree(download_root, ignore_errors=True)
        raise

    return local_post_id, version_id


def _extract_tags(payload: dict[str, Any]) -> list[str]:
    tags = payload.get("tags")
    if not isinstance(tags, list):
        return []
    out: list[str] = []
    for item in tags:
        if isinstance(item, str):
            normalized = item.strip()
            if normalized:
                out.append(normalized)
    return out


def _validate_import_series_selection(
    db: LibraryDBLike,
    *,
    creator_id: int,
    series_id: int | None,
) -> int | None:
    if series_id is None:
        return None
    series = db.get_series(series_id)
    if not series or int(series["creator_id"]) != creator_id:
        raise ValueError("Selected series was not found for this creator.")
    return series_id


def _find_import_source_match(
    db: LibraryDBLike,
    *,
    service: str,
    user_id: str,
    post_id: str,
    creator_id: int,
):
    version_match = db.find_version_by_source_global(
        service=service,
        external_user_id=user_id,
        external_post_id=post_id,
    )
    if version_match:
        exact_match = db.get_post(int(version_match["post_id"]))
    else:
        exact_match = db.find_post_by_source(service, user_id, post_id)
    if not exact_match:
        return None
    if int(exact_match["creator_id"]) == creator_id:
        return exact_match

    conflicting_creator = db.get_creator(int(exact_match["creator_id"]))
    conflicting_name = (
        str(conflicting_creator["name"]).strip()
        if conflicting_creator and conflicting_creator["name"]
        else f"creator #{exact_match['creator_id']}"
    )
    if version_match:
        raise ValueError(
            f'This source already exists under creator "{conflicting_name}" '
            f'as post #{exact_match["id"]} version #{int(version_match["id"])}.'
        )
    raise ValueError(f'This source already exists under creator "{conflicting_name}" as post #{exact_match["id"]}.')


def _parse_tags_text(raw_tags: str | None) -> list[str]:
    if raw_tags is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_tags.split(","):
        tag = part.strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out


def _resolve_import_version_label(requested_label: str | None, payload_title: Any, is_new_version: bool) -> str:
    if isinstance(requested_label, str) and requested_label.strip():
        return requested_label.strip()
    if is_new_version:
        return "Original" if _optional_str(payload_title) else "Version"
    return "Version"


def _parse_boolish(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "on", "yes"}


def _form_checkbox_enabled(form: Any, field_name: str, *, default: bool) -> bool:
    values = form.getlist(field_name)
    if not values:
        return default
    return any(_parse_boolish(value, default=False) for value in values)


def _resolve_import_title(
    requested_title: str | None,
    payload_title: Any,
    *,
    service: str,
    post_id: str,
) -> str:
    if isinstance(requested_title, str) and requested_title.strip():
        return requested_title.strip()
    fallback = _optional_str(payload_title)
    return fallback or f"{service}:{post_id}"


def _resolve_import_content(requested_content: str | None, payload_content: Any) -> str:
    if isinstance(requested_content, str):
        return requested_content
    return str(payload_content) if isinstance(payload_content, str) else ""


def _resolve_import_optional_metadata(
    requested_value: str | None,
    payload_value: Any,
    *,
    field_present: bool,
) -> str | None:
    if field_present:
        return _optional_str(requested_value)
    return _optional_str(payload_value)


def _reprocess_post_versions_for_media_renames(
    db: LibraryDBLike,
    *,
    post_id: int,
    rename_aliases: dict[str, str],
    conn: sqlite3.Connection | None = None,
) -> None:
    if not rename_aliases:
        return

    versions = db.list_post_versions(post_id)
    for version in versions:
        version_id = int(version["id"])
        original_content = str(version["content"] or "")
        original_metadata = _safe_load_metadata(version["metadata_json"])

        rewritten_content = _rewrite_content_media_names(original_content, rename_aliases)
        rewritten_metadata = _rewrite_metadata_media_names(original_metadata, rename_aliases)

        if rewritten_content == original_content and rewritten_metadata == original_metadata:
            continue

        db.update_post_version_content_metadata(
            version_id=version_id,
            content=rewritten_content,
            metadata=rewritten_metadata,
            conn=conn,
        )


def _rewrite_content_media_names(content: str, rename_aliases: dict[str, str]) -> str:
    if not content or not rename_aliases:
        return content

    if not re.search(r"<[a-zA-Z][^>]*>", content):
        updated = content
        for old_key, new_name in rename_aliases.items():
            if not old_key or not new_name:
                continue
            updated = updated.replace(old_key, new_name)
        return updated

    soup = BeautifulSoup(content, "html.parser")
    url_attrs = (
        ("img", "src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("a", "href"),
    )
    for tag_name, attr in url_attrs:
        for node in soup.find_all(tag_name):
            raw_url = node.get(attr)
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue
            rewritten = _rewrite_url_media_names(raw_url.strip(), rename_aliases)
            if rewritten != raw_url:
                node[attr] = rewritten

    for img in soup.find_all("img"):
        for attr in ("alt", "title"):
            raw_value = img.get(attr)
            if not isinstance(raw_value, str) or not raw_value.strip():
                continue
            replacement = rename_aliases.get(sanitize_filename(raw_value).lower())
            if replacement:
                img[attr] = replacement

    return str(soup)


def _rewrite_metadata_media_names(node: Any, rename_aliases: dict[str, str], *, field: str = "") -> Any:
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            out[key] = _rewrite_metadata_media_names(value, rename_aliases, field=str(key).strip().lower())
        return out
    if isinstance(node, list):
        return [_rewrite_metadata_media_names(item, rename_aliases, field=field) for item in node]
    if isinstance(node, str):
        trimmed = node.strip()
        if field in {"name", "filename", "file_name"}:
            replacement = rename_aliases.get(sanitize_filename(trimmed).lower())
            if replacement:
                return replacement
        if field in {"path", "url", "remote_url", "src", "href", "thumbnail_url"} or "://" in trimmed or "/" in trimmed:
            rewritten_url = _rewrite_url_media_names(trimmed, rename_aliases)
            if rewritten_url != trimmed:
                return rewritten_url
        return node
    return node


def _rewrite_url_media_names(raw_url: str, rename_aliases: dict[str, str]) -> str:
    parsed = urlparse(raw_url)
    updated_path = parsed.path
    changed = False

    basename = Path(parsed.path).name
    if basename:
        replacement = rename_aliases.get(sanitize_filename(basename).lower())
        if replacement and replacement != basename:
            updated_path = f"{parsed.path[: -len(basename)]}{replacement}"
            changed = True

    updated_query_pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = key.strip().lower()
        if key_lower in {"f", "file", "filename", "name", "download", "fn"}:
            replacement = rename_aliases.get(sanitize_filename(value).lower())
            if replacement and replacement != value:
                updated_query_pairs.append((key, replacement))
                changed = True
                continue
        updated_query_pairs.append((key, value))

    if not changed:
        return raw_url

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            updated_path,
            parsed.params,
            urlencode(updated_query_pairs, doseq=True),
            parsed.fragment,
        )
    )


def _extract_previews(raw_payload: dict[str, Any]) -> list[dict[str, str]]:
    previews = raw_payload.get("previews")
    if not isinstance(previews, list):
        return []

    out: list[dict[str, str]] = []
    for item in previews:
        if not isinstance(item, dict):
            continue
        path = _optional_str(item.get("path"))
        if not path:
            continue
        out.append(
            {
                "type": _optional_str(item.get("type")) or "",
                "server": _optional_str(item.get("server")) or "",
                "name": _optional_str(item.get("name")) or "",
                "path": path,
            }
        )
    return out


def _prettify_content_for_edit(content: str | None) -> str:
    if not isinstance(content, str) or not content.strip():
        return ""
    raw = content.strip()
    if not re.search(r"<[a-zA-Z][^>]*>", raw):
        return content
    # Keep formatting conservative: only split *sibling* adjacent tags.
    # Do not normalize internal whitespace, so snippets like <p><br></p>
    # remain structurally untouched.
    return re.sub(r"(</[^>]+>)\s*(<[^/][^>]*>)", r"\1\n\2", raw).strip()


def _format_datetime_for_display(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = value.strip()
    if not cleaned:
        return ""

    normalized = cleaned[:-1] + "+00:00" if cleaned.endswith("Z") else cleaned
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return cleaned
    return parsed.strftime("%Y-%m-%d %H:%M")


def _render_markdown_snippet(value: Any) -> Markup:
    if not isinstance(value, str):
        return Markup("")
    cleaned = value.strip()
    if not cleaned:
        return Markup("")

    escaped = html.escape(cleaned).replace("\r\n", "\n")

    escaped = re.sub(
        r"\[([^\]]+)\]\((https?://[^\s)]+)\)",
        r'<a href="\2" target="_blank" rel="noopener noreferrer">\1</a>',
        escaped,
    )
    escaped = re.sub(r"\*\*([^*\n]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\*([^*\n]+)\*", r"<em>\1</em>", escaped)
    escaped = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", escaped)

    blocks = [part.strip() for part in re.split(r"\n{2,}", escaped) if part.strip()]
    rendered = "".join(f"<p>{block.replace(chr(10), '<br>')}</p>" for block in blocks)
    safe = bleach.clean(
        rendered,
        tags=["p", "br", "strong", "em", "code", "a"],
        attributes={"a": ["href", "target", "rel"]},
        protocols=["http", "https"],
        strip=True,
    )
    return Markup(safe)
