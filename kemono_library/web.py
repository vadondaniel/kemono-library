from __future__ import annotations

import html
import json
import re
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import bleach
from flask import Flask, flash, jsonify, redirect, render_template, request, send_from_directory, url_for
from markupsafe import Markup

from .db import LibraryDB
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

    @app.template_filter("format_datetime")
    def format_datetime_filter(value: Any) -> str:
        return _format_datetime_for_display(value)

    @app.template_filter("render_markdown")
    def render_markdown_filter(value: Any) -> Markup:
        return _render_markdown_snippet(value)

    @app.get("/")
    def index():
        creators = db.list_creators()
        recent_posts = db.list_recent_posts()
        return render_template("index.html", creators=creators, recent_posts=recent_posts)

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

        return render_template("creator_edit.html", creator=creator)

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
        sort_by = request.args.get("sort", "published").strip().lower()
        sort_direction = request.args.get("direction", "desc").strip().lower()

        if sort_by not in {"published", "title"}:
            sort_by = "published"
        if sort_direction not in {"asc", "desc"}:
            sort_direction = "desc"

        selected_series_id: int | None = None
        unsorted_only = False
        active_folder = "all"
        if folder == "unsorted":
            unsorted_only = True
            active_folder = "unsorted"
        elif requested_series_id is not None and requested_series_id in series_by_id:
            selected_series_id = requested_series_id
            active_folder = "series"

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
        return render_template(
            "creator_detail.html",
            creator=creator,
            series_list=series_list,
            posts=posts,
            selected_series=series_by_id.get(selected_series_id) if selected_series_id is not None else None,
            active_folder=active_folder,
            sort_by=sort_by,
            sort_direction=sort_direction,
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
        if not name:
            flash("Series/group name is required.", "error")
            return redirect(url_for("creator_detail", creator_id=creator_id, series_id=series_id))

        db.update_series(
            series_id,
            name=name,
            description=description,
            tags_text=tags_text,
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
        prefill_import_target_mode = request.args.get("import_target_mode", "").strip().lower()
        prefill_target_post_id = request.args.get("target_post_id", type=int)
        prefill_overwrite_matching_version = _parse_boolish(
            request.args.get("overwrite_matching_version"),
            default=True,
        )
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

        prefill_import_target_mode = request.form.get("import_target_mode", "").strip().lower()
        prefill_target_post_id = request.form.get("target_post_id", type=int)
        prefill_overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
        prefill_set_as_default = _form_checkbox_enabled(
            request.form,
            "set_as_default",
            default=True,
        )
        prefill_version_label = request.form.get("version_label", "").strip()
        prefill_version_language = request.form.get("version_language", "").strip()

        preview_ref = KemonoPostRef(service=post_ref.service, user_id=str(resolved_user_id), post_id=post_ref.post_id)
        attachments = extract_attachments(raw_payload)
        exact_match = db.find_post_by_source(preview_ref.service, preview_ref.user_id or "", preview_ref.post_id)
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
        overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
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
                import_target_mode=request.form.get("import_target_mode", "new"),
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
        overwrite_matching_version = _form_checkbox_enabled(
            request.form,
            "overwrite_matching_version",
            default=True,
        )
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
            "import_target_mode": request.form.get("import_target_mode", "new"),
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
                "message": "Queued import job...",
                "completed": 0,
                "total": 0,
                "current_file": None,
                "redirect_url": None,
                "error": None,
            }

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
                job = import_jobs.get(job_id)
                if not job:
                    return
                job.update(
                    {
                        "status": "running",
                        "message": message,
                        "completed": completed,
                        "total": total,
                        "current_file": current_file,
                    }
                )

        def worker() -> None:
            try:
                with import_jobs_lock:
                    job = import_jobs.get(job_id)
                    if job:
                        job.update({"status": "running", "message": "Fetching post payload..."})

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
                    job = import_jobs.get(job_id)
                    if job:
                        total = int(job.get("total") or 0)
                        completed = int(job.get("completed") or 0)
                        job.update(
                            {
                                "status": "completed",
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
                    job = import_jobs.get(job_id)
                    if job:
                        job.update(
                            {
                                "status": "failed",
                                "message": "Import failed.",
                                "error": str(exc),
                            }
                        )

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
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
        return jsonify(snapshot)

    @app.get("/posts/<int:post_id>")
    def post_detail(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        requested_version_id = request.args.get("version_id", type=int)
        active_version = db.get_post_version(post_id, requested_version_id)
        if not active_version:
            versions = db.list_post_versions(post_id)
            if not versions:
                return ("Post version not found", 404)
            active_version = versions[0]
        versions = db.list_post_versions(post_id)
        active_version_id = int(active_version["id"])
        attachments = db.list_attachments(post_id, version_id=active_version_id)
        local_media_map, local_media_by_name = _build_local_media_maps(active_version, attachments)
        remote_media_by_name = _build_remote_media_by_name(active_version, attachments)
        files_base = Path(app.config["FILES_DIR"])
        attachment_rows = []
        for row in attachments:
            local_path = row["local_path"]
            local_abs = files_base / local_path if isinstance(local_path, str) and local_path.strip() else None
            local_available = _is_valid_file(local_abs)
            attachment_rows.append(
                {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "remote_url": row["remote_url"],
                    "remote_url_display": _preferred_remote_url_for_access(
                        str(row["remote_url"]),
                        row["name"],
                    ),
                    "local_path": row["local_path"],
                    "kind": row["kind"],
                    "local_available": local_available,
                }
            )

        rendered_content = render_post_content(
            active_version["content"],
            current_service=post["service"],
            current_user_id=post["external_user_id"],
            current_post_id=post_id,
            local_media_map=local_media_map,
            local_media_by_name=local_media_by_name,
            remote_media_by_name=remote_media_by_name,
        )
        return render_template(
            "post_detail.html",
            post=post,
            versions=versions,
            active_version=active_version,
            attachments=attachment_rows,
            rendered_content=rendered_content,
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

        files_base = Path(app.config["FILES_DIR"])
        existing_local = attachment["local_path"]
        if isinstance(existing_local, str) and existing_local.strip():
            destination = files_base / existing_local
        else:
            destination = files_base / f"post_{post_id}" / sanitize_filename(str(attachment["name"]))

        try:
            used_remote_url = _download_with_fallback_remote_url(
                str(attachment["remote_url"]),
                destination,
                attachment["name"],
            )
            if not used_remote_url:
                raise RuntimeError("all download URL variants failed")
        except Exception as exc:  # noqa: BLE001
            flash(f"Retry failed for {attachment['name']}: {exc}", "error")
            return redirect(detail_url)

        if not _is_valid_file(destination):
            flash(f"Retry failed for {attachment['name']}: downloaded file is empty.", "error")
            return redirect(detail_url)

        local_rel = destination.relative_to(files_base).as_posix()
        db.update_attachment_local_path(attachment_id, local_rel)
        flash(f"Downloaded {attachment['name']}.", "success")
        return redirect(detail_url)

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
        selected_thumbnail_attachment_id: int | None = None
        thumbnail_remote_url = _optional_str(active_version["thumbnail_remote_url"])
        thumbnail_name = _optional_str(active_version["thumbnail_name"])
        thumbnail_local_path = _optional_str(active_version["thumbnail_local_path"])
        thumbnail_preview_url: str | None = None
        active_metadata = _safe_load_metadata(active_version["metadata_json"])
        thumbnail_focus_x, thumbnail_focus_y = _extract_thumbnail_focus_from_metadata(active_metadata)

        for row in attachments:
            local_path = _optional_str(row["local_path"])
            local_available = _is_valid_file(files_base / local_path) if local_path else False
            row_data = {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "kind": str(row["kind"]),
                "remote_url": str(row["remote_url"]),
                "remote_url_display": _preferred_remote_url_for_access(str(row["remote_url"]), row["name"]),
                "local_path": local_path,
                "local_available": local_available,
            }
            attachment_rows.append(row_data)
            if selected_thumbnail_attachment_id is None:
                if thumbnail_remote_url and row_data["remote_url"] == thumbnail_remote_url:
                    selected_thumbnail_attachment_id = row_data["id"]
                elif thumbnail_local_path and row_data["local_path"] == thumbnail_local_path:
                    selected_thumbnail_attachment_id = row_data["id"]
                elif thumbnail_name and row_data["name"] == thumbnail_name:
                    selected_thumbnail_attachment_id = row_data["id"]

        if thumbnail_local_path and _is_valid_file(files_base / thumbnail_local_path):
            thumbnail_preview_url = url_for("serve_file", relative_path=thumbnail_local_path)
        elif thumbnail_remote_url:
            thumbnail_preview_url = _preferred_remote_url_for_access(thumbnail_remote_url, thumbnail_name)

        if request.method == "POST":
            action = request.form.get("action", "save").strip().lower()

            if action == "remove_attachment":
                attachment_id = request.form.get("attachment_id", type=int)
                if not attachment_id:
                    flash("Attachment not found.", "error")
                    return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))

                removed = next((item for item in attachment_rows if item["id"] == attachment_id), None)
                if not removed:
                    flash("Attachment not found.", "error")
                    return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))

                remaining = [
                    {
                        "name": item["name"],
                        "remote_url": item["remote_url"],
                        "local_path": item["local_path"],
                        "kind": item["kind"],
                    }
                    for item in attachment_rows
                    if item["id"] != attachment_id
                ]
                db.replace_attachments(post_id, remaining, version_id=active_version_id)

                if (
                    (thumbnail_remote_url and removed["remote_url"] == thumbnail_remote_url)
                    or (thumbnail_local_path and removed["local_path"] == thumbnail_local_path)
                ):
                    updated_metadata = _set_thumbnail_focus_in_metadata(active_metadata, None, None)
                    db.update_post_version(
                        version_id=active_version_id,
                        label=active_version["label"],
                        language=active_version["language"],
                        title=active_version["title"],
                        content=active_version["content"],
                        thumbnail_name=None,
                        thumbnail_remote_url=None,
                        thumbnail_local_path=None,
                        published_at=active_version["published_at"],
                        edited_at=active_version["edited_at"],
                        next_external_post_id=active_version["next_external_post_id"],
                        prev_external_post_id=active_version["prev_external_post_id"],
                        metadata=updated_metadata,
                        source_url=active_version["source_url"],
                    )
                flash("Attachment removed from this version.", "success")
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

            resolved_thumbnail_name = active_version["thumbnail_name"]
            resolved_thumbnail_remote_url = active_version["thumbnail_remote_url"]
            resolved_thumbnail_local_path = active_version["thumbnail_local_path"]

            if thumbnail_choice == "__none__":
                resolved_thumbnail_name = None
                resolved_thumbnail_remote_url = None
                resolved_thumbnail_local_path = None
                focus_x = None
                focus_y = None
            elif thumbnail_choice != "__keep__":
                try:
                    selected_thumbnail_id = int(thumbnail_choice)
                except (TypeError, ValueError):
                    flash("Selected thumbnail file was not found.", "error")
                    return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))
                selected_attachment = next(
                    (item for item in attachment_rows if item["id"] == selected_thumbnail_id),
                    None,
                )
                if selected_attachment is None:
                    flash("Selected thumbnail file was not found.", "error")
                    return redirect(url_for("edit_post", post_id=post_id, version_id=active_version_id))
                resolved_thumbnail_name = selected_attachment["name"]
                resolved_thumbnail_remote_url = selected_attachment["remote_url"]
                if selected_attachment["local_path"] and selected_attachment["local_available"]:
                    resolved_thumbnail_local_path = selected_attachment["local_path"]
                else:
                    resolved_thumbnail_local_path = None

            metadata_for_save = _set_thumbnail_focus_in_metadata(active_metadata, focus_x, focus_y)
            db.update_post_series(post_id=post_id, series_id=series_id)
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
            )
            flash("Post updated.", "success")
            return redirect(url_for("post_detail", post_id=post_id, version_id=active_version_id))
        return render_template(
            "post_edit.html",
            post=post,
            versions=versions,
            active_version=active_version,
            series_list=series_list,
            edit_content=_prettify_content_for_edit(active_version["content"]),
            attachments=attachment_rows,
            selected_thumbnail_attachment_id=selected_thumbnail_attachment_id,
            thumbnail_preview_url=thumbnail_preview_url,
            thumbnail_focus_x=thumbnail_focus_x,
            thumbnail_focus_y=thumbnail_focus_y,
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
        return send_from_directory(app.config["FILES_DIR"], safe_relative, as_attachment=False)

    @app.get("/creator-icons/<path:relative_path>")
    def serve_creator_icon(relative_path: str):
        safe_relative = relative_path.replace("\\", "/")
        return send_from_directory(app.config["ICONS_DIR"], safe_relative, as_attachment=False)

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
    return bool(path and path.exists() and path.is_file() and path.stat().st_size > 0)


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


def _ensure_creator_icon(
    db: LibraryDB,
    *,
    icons_base: Path,
    creator_id: int,
    service: str,
    user_id: str,
) -> None:
    creator = db.get_creator(creator_id)
    if not creator:
        return

    current_remote = creator["icon_remote_url"]
    current_local = creator["icon_local_path"]
    expected_remote = creator_icon_url(service, user_id)
    if isinstance(current_local, str) and current_local.strip():
        local_abs = icons_base / current_local
        if _is_valid_file(local_abs):
            if current_remote != expected_remote:
                db.update_creator_icon(
                    creator_id,
                    icon_remote_url=expected_remote,
                    icon_local_path=current_local,
                )
            return

    remote_url, local_abs = download_creator_icon(service, user_id, icons_base)
    local_rel = local_abs.relative_to(icons_base).as_posix() if local_abs else None
    db.update_creator_icon(
        creator_id,
        icon_remote_url=remote_url,
        icon_local_path=local_rel,
    )


def _build_local_media_maps(
    post: Any,
    attachments: list[Any],
) -> tuple[dict[str, str], dict[str, str]]:
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

    return local_media_map, local_media_by_name


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
    cleaned = path.strip()
    return cleaned.lower() if cleaned else ""


def _build_target_attachment_index(
    db: LibraryDB,
    *,
    files_base: Path,
    post_ids: list[int],
) -> dict[str, dict[str, int]]:
    index: dict[str, dict[str, int]] = {}
    for post_id in post_ids:
        key_state: dict[str, int] = {}
        for row in db.list_all_attachments_for_post(post_id):
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
    db: LibraryDB,
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

    post_ref = KemonoPostRef(service=service, user_id=user_id, post_id=post_id)
    raw_payload = fetch_post_json(post_ref)
    payload = normalize_post_payload(raw_payload)

    db.attach_creator_external(creator_id, service=service, external_user_id=user_id)
    _ensure_creator_icon(
        db,
        icons_base=icons_base,
        creator_id=creator_id,
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
    exact_match_post = db.find_post_by_source(service, user_id, post_id)
    local_post_id: int
    if exact_match_post:
        local_post_id = int(exact_match_post["id"])
        if import_target_mode == "new":
            raise ValueError("This source already exists locally. Import as a version or overwrite it.")
    elif import_target_mode == "existing":
        if not target_post_id:
            raise ValueError("Pick a target post for version import.")
        target_post = db.get_post(target_post_id)
        if not target_post or int(target_post["creator_id"]) != creator_id:
            raise ValueError("Target post was not found for this creator.")
        local_post_id = target_post_id
    else:
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
        )

    if series_id is not None:
        db.update_post_series(local_post_id, series_id)

    existing_version = db.find_version_by_source(
        post_id=local_post_id,
        service=service,
        external_user_id=user_id,
        external_post_id=post_id,
    )
    if existing_version and not overwrite_matching_version:
        raise ValueError("Matching source version already exists. Enable overwrite to replace it.")

    resolved_version_label = _resolve_import_version_label(version_label, payload.get("title"), existing_version is None)
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
        )
    else:
        version_id = db.create_post_version(
            post_id=local_post_id,
            label=resolved_version_label,
            language=resolved_version_language,
            is_manual=False,
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
        )

    download_root = files_base / f"post_{local_post_id}"
    existing_rows = db.list_all_attachments_for_post(local_post_id)
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
    db.replace_attachments(local_post_id, saved, version_id=version_id)
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
    )
    tags = _parse_tags_text(tags_text) if tags_text is not None else _extract_tags(payload)
    db.replace_tags(local_post_id, tags, version_id=version_id)
    db.replace_previews(local_post_id, _extract_previews(raw_payload), version_id=version_id)
    if set_as_default:
        db.set_default_post_version(local_post_id, version_id)
    else:
        db.sync_post_from_default_version(local_post_id)
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
