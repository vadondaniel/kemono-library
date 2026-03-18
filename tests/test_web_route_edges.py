import time

from kemono_library.web import create_app


def _make_app(tmp_path):
    return create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )


def _wait_for_async_status(client, status_url: str, *, attempts: int = 200):
    last_payload = None
    for _ in range(attempts):
        response = client.get(status_url)
        assert response.status_code == 200
        payload = response.get_json()
        assert isinstance(payload, dict)
        last_payload = payload
        if payload.get("status") in {"completed", "failed"}:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for async job status; last payload={last_payload}")


def test_import_preview_requires_creator_id(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().post(
        "/import/preview",
        data={"post_url": "https://kemono.cr/fanbox/post/100"},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_preview_rejects_unknown_creator(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/post/100",
            "creator_id": "999",
            "series_id": "",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_preview_rejects_invalid_post_url(tmp_path):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Preview Creator")

    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://example.com/not-supported",
            "creator_id": str(creator_id),
            "series_id": "",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_preview_handles_fetch_failure(tmp_path, monkeypatch):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Preview Creator")

    def fake_fetch(*_args, **_kwargs):
        raise RuntimeError("fetch failed")

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)

    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/100",
            "creator_id": str(creator_id),
            "series_id": "",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_preview_requires_resolved_user_id(tmp_path, monkeypatch):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Preview Creator")

    def fake_fetch(*_args, **_kwargs):
        return {
            "post": {
                "title": "No User",
                "content": "",
                "attachments": [],
            },
            "attachments": [],
        }

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)

    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/post/100",
            "creator_id": str(creator_id),
            "series_id": "",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_preview_force_target_requires_valid_creator_post(tmp_path, monkeypatch):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Preview Creator")

    def fake_fetch(*_args, **_kwargs):
        return {
            "post": {
                "title": "Force Target",
                "content": "",
                "user": "70479526",
                "attachments": [],
            },
            "attachments": [],
        }

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)

    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/100",
            "creator_id": str(creator_id),
            "series_id": "",
            "force_target_post_version": "1",
            "target_post_id": "999",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/import?" in response.headers["Location"]


def test_import_commit_requires_required_fields(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": "1",
            "service": "fanbox",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/import")


def test_import_commit_rejects_foreign_series(tmp_path):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_a = db.create_creator("Creator A")
    creator_b = db.create_creator("Creator B")
    foreign_series_id = db.create_series(creator_b, "Foreign")

    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": str(creator_a),
            "series_id": str(foreign_series_id),
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/import")


def test_import_commit_handles_unexpected_import_exception(tmp_path, monkeypatch):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Creator A")

    def fake_import(*_args, **_kwargs):
        raise RuntimeError("unexpected failure")

    monkeypatch.setattr("kemono_library.web._import_post_into_library", fake_import)

    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/import")


def test_import_start_requires_required_fields(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().post(
        "/import/start",
        data={
            "creator_id": "1",
            "service": "fanbox",
            "user_id": "70479526",
        },
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["error"] == "Missing import fields."


def test_import_start_force_flags_override_mode_and_overwrite(tmp_path, monkeypatch):
    app = _make_app(tmp_path)
    captured: dict[str, object] = {}

    def fake_import(*_args, **kwargs):
        captured["import_target_mode"] = kwargs["import_target_mode"]
        captured["overwrite_matching_version"] = kwargs["overwrite_matching_version"]
        return 1, 1

    monkeypatch.setattr("kemono_library.web._import_post_into_library", fake_import)

    client = app.test_client()
    start = client.post(
        "/import/start",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
            "import_target_mode": "new",
            "overwrite_matching_version": "0",
            "force_target_post_version": "1",
            "force_overwrite_matching_version": "1",
        },
    )
    assert start.status_code == 200
    payload = start.get_json()
    assert isinstance(payload, dict)
    status_url = payload["status_url"]

    final_status = _wait_for_async_status(client, status_url)
    assert final_status["status"] == "completed"
    assert captured["import_target_mode"] == "existing"
    assert captured["overwrite_matching_version"] is True


def test_import_job_status_returns_404_for_unknown_job(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().get("/import/jobs/does-not-exist/status")
    assert response.status_code == 404
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["error"] == "Import job not found."


def test_attachment_retry_job_status_returns_404_for_unknown_job(tmp_path):
    app = _make_app(tmp_path)
    response = app.test_client().get("/attachments/retry-jobs/does-not-exist/status")
    assert response.status_code == 404
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["error"] == "Retry job not found."


def test_attachment_retry_async_job_completes_when_scope_has_no_missing_rows(tmp_path):
    app = _make_app(tmp_path)
    client = app.test_client()

    start = client.post(
        "/attachments/retry-missing/start",
        data={
            "scope": "post",
            "scope_id": "999",
            "return_to": "https://invalid.example/not-allowed",
        },
    )
    assert start.status_code == 200
    payload = start.get_json()
    assert isinstance(payload, dict)
    status_url = payload["status_url"]

    final_status = _wait_for_async_status(client, status_url)
    assert final_status["status"] == "completed"
    assert final_status["message"] == "No missing attachments matched this retry scope."
    assert final_status["total"] == 0
    assert final_status["completed"] == 0
    assert final_status["redirect_url"] == "/attachments"


def test_version_routes_cover_not_found_and_terminal_delete_paths(tmp_path):
    app = _make_app(tmp_path)
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Version Route Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="route-user",
        external_post_id="route-post",
        title="Route Post",
        content="route body",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/route-user/post/route-post",
    )
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])

    client = app.test_client()

    missing_post_default = client.post("/posts/999999/versions/1/set-default")
    assert missing_post_default.status_code == 404

    missing_version_default = client.post(f"/posts/{post_id}/versions/999999/set-default")
    assert missing_version_default.status_code == 404

    missing_source_clone = client.post(f"/posts/{post_id}/versions/clone", data={})
    assert missing_source_clone.status_code == 400
    assert b"Missing source version" in missing_source_clone.data

    invalid_source_clone = client.post(
        f"/posts/{post_id}/versions/clone",
        data={"source_version_id": "999999"},
        follow_redirects=False,
    )
    assert invalid_source_clone.status_code == 302
    assert invalid_source_clone.headers["Location"].endswith(f"/posts/{post_id}?version_id=999999")

    missing_post_delete = client.post("/posts/999999/versions/1/delete")
    assert missing_post_delete.status_code == 404

    missing_version_delete = client.post(f"/posts/{post_id}/versions/999999/delete")
    assert missing_version_delete.status_code == 404

    delete_last = client.post(f"/posts/{post_id}/versions/{version_id}/delete", follow_redirects=False)
    assert delete_last.status_code == 302
    assert delete_last.headers["Location"].endswith(f"/creators/{creator_id}")
    assert db.list_post_versions(post_id) == []
