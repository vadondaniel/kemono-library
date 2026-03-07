from __future__ import annotations

from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, send_from_directory, url_for

from .db import LibraryDB
from .kemono import (
    KemonoPostRef,
    download_attachment,
    extract_attachments,
    fetch_post_json,
    normalize_post_payload,
    parse_kemono_post_url,
    sanitize_filename,
)
from .rendering import render_post_content


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY="dev-local-secret",
        DATABASE=str(Path(app.root_path).parent / "data" / "library.db"),
        FILES_DIR=str(Path(app.root_path).parent / "data" / "files"),
    )
    if test_config:
        app.config.update(test_config)

    db = LibraryDB(app.config["DATABASE"])
    db.init_schema()
    Path(app.config["FILES_DIR"]).mkdir(parents=True, exist_ok=True)
    app.db = db  # type: ignore[attr-defined]

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

    @app.get("/creators/<int:creator_id>")
    def creator_detail(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)
        series_list = db.list_series(creator_id)
        posts = db.list_posts_for_creator(creator_id)
        return render_template(
            "creator_detail.html",
            creator=creator,
            series_list=series_list,
            posts=posts,
        )

    @app.post("/creators/<int:creator_id>/series")
    def create_series(creator_id: int):
        creator = db.get_creator(creator_id)
        if not creator:
            return ("Creator not found", 404)
        name = request.form.get("name", "").strip()
        if not name:
            flash("Series/group name is required.", "error")
            return redirect(url_for("creator_detail", creator_id=creator_id))
        db.create_series(creator_id, name)
        flash("Series saved.", "success")
        return redirect(url_for("creator_detail", creator_id=creator_id))

    @app.get("/import")
    def import_form():
        creators = db.list_creators()
        selected_creator = request.args.get("creator_id", type=int)
        selected_series = request.args.get("series_id", type=int)
        prefill_url = request.args.get("url", "")
        return render_template(
            "import_form.html",
            creators=creators,
            selected_creator=selected_creator,
            selected_series=selected_series,
            prefill_url=prefill_url,
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

        preview_ref = KemonoPostRef(service=post_ref.service, user_id=str(resolved_user_id), post_id=post_ref.post_id)
        attachments = extract_attachments(payload)

        return render_template(
            "import_preview.html",
            creator=creator,
            creator_id=creator_id,
            series_id=series_id,
            post_ref=preview_ref,
            payload=payload,
            attachments=attachments,
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

        creator = db.get_creator(creator_id)
        if not creator:
            flash("Creator not found.", "error")
            return redirect(url_for("import_form"))

        post_ref = KemonoPostRef(service=service, user_id=user_id, post_id=post_id)
        try:
            raw_payload = fetch_post_json(post_ref)
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to fetch post during save: {exc}", "error")
            return redirect(url_for("import_form"))
        payload = normalize_post_payload(raw_payload)

        db.attach_creator_external(creator_id, service=service, external_user_id=user_id)
        all_attachments = extract_attachments(payload)
        selected_indices = set(request.form.getlist("selected_attachment"))
        selected_attachments = [
            candidate for idx, candidate in enumerate(all_attachments) if str(idx) in selected_indices
        ]

        title = payload.get("title") or f"{service}:{post_id}"
        content = payload.get("content") or ""
        source_url = post_ref.canonical_url
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
        )

        download_root = Path(app.config["FILES_DIR"]) / f"post_{local_post_id}"
        saved = []
        for candidate in selected_attachments:
            filename = sanitize_filename(candidate.name)
            destination = download_root / filename
            try:
                download_attachment(candidate.remote_url, destination)
                local_path = str(destination.relative_to(Path(app.config["FILES_DIR"])))
            except Exception:  # noqa: BLE001
                local_path = None
            saved.append(
                {
                    "name": candidate.name,
                    "remote_url": candidate.remote_url,
                    "local_path": local_path,
                    "kind": candidate.kind,
                }
            )
        db.replace_attachments(local_post_id, saved)

        flash("Post imported into local library.", "success")
        return redirect(url_for("post_detail", post_id=local_post_id))

    @app.get("/posts/<int:post_id>")
    def post_detail(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        attachments = db.list_attachments(post_id)
        rendered_content = render_post_content(
            post["content"],
            current_service=post["service"],
            current_user_id=post["external_user_id"],
            current_post_id=post_id,
        )
        return render_template(
            "post_detail.html",
            post=post,
            attachments=attachments,
            rendered_content=rendered_content,
        )

    @app.route("/posts/<int:post_id>/edit", methods=["GET", "POST"])
    def edit_post(post_id: int):
        post = db.get_post(post_id)
        if not post:
            return ("Post not found", 404)
        series_list = db.list_series(post["creator_id"])
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "")
            series_id = request.form.get("series_id", type=int)
            if not title:
                flash("Title is required.", "error")
                return redirect(url_for("edit_post", post_id=post_id))
            db.update_post(post_id=post_id, title=title, content=content, series_id=series_id)
            flash("Post updated.", "success")
            return redirect(url_for("post_detail", post_id=post_id))
        return render_template("post_edit.html", post=post, series_list=series_list)

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
        return send_from_directory(app.config["FILES_DIR"], relative_path, as_attachment=True)

    return app
