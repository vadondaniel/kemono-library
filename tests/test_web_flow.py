from pathlib import Path
import time
from bs4 import BeautifulSoup
from werkzeug.datastructures import MultiDict

from kemono_library.web import create_app


def test_import_and_resolve_flow(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    payload = {
        "post": {
            "title": "Post 100",
            "content": '<a href="https://kemono.cr/fanbox/post/101">linked</a>',
            "user": "70479526",
            "file": {"name": "cover.jpg", "path": "/data/x/cover.jpg"},
            "attachments": [],
            "published": "2025-10-25T12:00:00",
            "edited": "2025-10-25T12:00:00",
            "next": "200",
            "prev": "050",
            "tags": ["alpha", "beta"],
        },
        "attachments": [],
        "previews": [
            {
                "type": "thumbnail",
                "server": "https://n1.kemono.cr",
                "name": "cover-thumb.jpg",
                "path": "/x/y/thumb.jpg",
            },
            {
                "type": "thumbnail",
                "server": "https://n1.kemono.cr",
                "name": "cover-thumb-alias.jpg",
                "path": "/x/y/thumb.jpg",
            },
        ],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()

    creator_response = client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)
    assert creator_response.status_code == 302
    assert "/creators/" in creator_response.headers["Location"]

    preview = client.post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/100",
            "creator_id": "1",
            "series_id": "",
        },
    )
    assert preview.status_code == 200
    assert b"cover.jpg" in preview.data

    commit = client.post(
        "/import/commit",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
            "selected_attachment": "0",
        },
        follow_redirects=False,
    )
    assert commit.status_code == 302
    assert commit.headers["Location"].endswith("/posts/1")
    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert attachments[0]["local_path"] == "post_1/cover.jpg"
    post = app.db.get_post(1)  # type: ignore[attr-defined]
    assert post["published_at"] == "2025-10-25T12:00:00"
    assert post["edited_at"] == "2025-10-25T12:00:00"
    assert post["next_external_post_id"] == "200"
    assert post["prev_external_post_id"] == "050"
    assert post["thumbnail_name"] == "cover.jpg"
    assert post["thumbnail_remote_url"] == "https://kemono.cr/data/x/cover.jpg"
    assert post["thumbnail_local_path"] == "post_1/cover.jpg"
    creator = app.db.get_creator(1)  # type: ignore[attr-defined]
    assert creator["icon_remote_url"] == "https://img.kemono.cr/icons/fanbox/70479526"
    assert creator["icon_local_path"] == "fanbox_70479526.jpg"
    tags = app.db.list_tags(1)  # type: ignore[attr-defined]
    previews = app.db.list_previews(1)  # type: ignore[attr-defined]
    assert [row["tag"] for row in tags] == ["alpha", "beta"]
    assert len(previews) == 1
    assert previews[0]["name"] == "cover-thumb.jpg"

    detail = client.get("/posts/1")
    assert detail.status_code == 200
    assert b"/links/resolve?service=fanbox&amp;post=101" in detail.data
    assert b"Published:" in detail.data
    assert b"2025-10-25T12:00:00" in detail.data

    unresolved = client.get("/links/resolve?service=fanbox&post=100&user=70479526")
    assert unresolved.status_code == 302
    assert unresolved.headers["Location"].endswith("/posts/1")


def test_import_start_reports_live_progress_until_complete(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    payload = {
        "post": {
            "title": "Async Import",
            "content": "",
            "user": "70479526",
            "attachments": [
                {"name": "a.jpg", "path": "/aa/bb/a.jpg"},
                {"name": "b.jpg", "path": "/aa/bb/b.jpg"},
            ],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        time.sleep(0.01)
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)

    start = client.post(
        "/import/start",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "200",
            "selected_attachment": ["0", "1"],
        },
    )
    assert start.status_code == 200
    start_payload = start.get_json()
    assert isinstance(start_payload, dict)
    status_url = start_payload.get("status_url")
    assert isinstance(status_url, str)

    final_status: dict | None = None
    for _ in range(150):
        status_response = client.get(status_url)
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert isinstance(status_payload, dict)
        if status_payload.get("status") == "completed":
            final_status = status_payload
            break
        if status_payload.get("status") == "failed":
            raise AssertionError(status_payload.get("error"))
        time.sleep(0.01)

    assert final_status is not None
    assert final_status["redirect_url"] == "/posts/1"
    assert final_status["total"] == 2
    assert final_status["completed"] == 2


def test_served_files_are_inline_not_forced_download(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    file_path = Path(app.config["FILES_DIR"]) / "post_1" / "img.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"fakejpg")

    client = app.test_client()
    response = client.get("/files/post_1/img.jpg")

    assert response.status_code == 200
    disposition = response.headers.get("Content-Disposition", "")
    assert "attachment" not in disposition.lower()


def test_favicon_route_serves_svg_icon(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    client = app.test_client()
    response = client.get("/favicon.ico")
    assert response.status_code == 200
    assert b"<svg" in response.data


def test_reimport_reuses_existing_file_without_duplicate_suffix(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    payload = {
        "post": {
            "title": "Post Reimport",
            "content": "",
            "user": "70479526",
            "file": {"name": "cover.jpg", "path": "/data/x/cover.jpg"},
            "attachments": [],
        },
        "attachments": [],
    }

    download_calls = {"count": 0}

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        download_calls["count"] += 1
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)

    commit_data = {
        "creator_id": "1",
        "series_id": "",
        "service": "fanbox",
        "user_id": "70479526",
        "post_id": "100",
        "selected_attachment": "0",
    }
    first = client.post("/import/commit", data=commit_data, follow_redirects=False)
    second = client.post("/import/commit", data=commit_data, follow_redirects=False)

    assert first.status_code == 302
    assert second.status_code == 302
    assert download_calls["count"] == 1

    files = sorted((Path(app.config["FILES_DIR"]) / "post_1").glob("cover*.jpg"))
    assert len(files) == 1
    assert files[0].name == "cover.jpg"

    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert len(attachments) == 1
    assert attachments[0]["local_path"] == "post_1/cover.jpg"


def test_import_commit_applies_metadata_overrides(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    payload = {
        "post": {
            "title": "Source Title",
            "content": "Source content",
            "user": "70479526",
            "attachments": [],
            "published": "2025-10-25T12:00:00",
            "edited": "2025-10-26T12:00:00",
            "next": "200",
            "prev": "050",
            "tags": ["alpha", "beta"],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)

    commit = client.post(
        "/import/commit",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
            "title": "Manual Title",
            "content": "Manual content",
            "published_at": "2024-01-01T03:04:05",
            "edited_at": "",
            "next_external_post_id": "999",
            "prev_external_post_id": "",
            "tags_text": "one, two, one",
        },
        follow_redirects=False,
    )
    assert commit.status_code == 302

    post = app.db.get_post(1)  # type: ignore[attr-defined]
    assert post["title"] == "Manual Title"
    assert post["content"] == "Manual content"
    assert post["published_at"] == "2024-01-01T03:04:05"
    assert post["edited_at"] is None
    assert post["next_external_post_id"] == "999"
    assert post["prev_external_post_id"] is None
    tags = app.db.list_tags(1)  # type: ignore[attr-defined]
    assert [row["tag"] for row in tags] == ["one", "two"]


def test_reimport_force_overwrite_ignores_new_mode_conflict(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Creator A")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Existing title",
        content="Old",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )
    version = db.get_post_version(post_id)
    assert version is not None

    payload = {
        "post": {
            "title": "Reimported title",
            "content": "Reimported content",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
            "import_target_mode": "new",
            "overwrite_matching_version": "0",
            "force_overwrite_matching_version": "1",
            "set_as_default": "1",
            "version_label": "Original",
            "version_language": "",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302

    updated_version = db.get_post_version(post_id)
    assert updated_version is not None
    assert updated_version["title"] == "Reimported title"
    assert updated_version["content"] == "Reimported content"


def test_post_detail_maps_inline_alias_to_local_download(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Alias Creator")
    metadata = {
        "post": {
            "content": (
                '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
                'vh5E4UKF5F5EUzIIXGjVkRkc.jpeg" rel="noopener noreferrer"></a></p>'
            ),
            "attachments": [
                {"name": "eO8wzPLjankw59mg6YeTMzxN.jpeg", "path": "/c2/4f/hash.jpg"},
                {"name": "vh5E4UKF5F5EUzIIXGjVkRkc.jpeg", "path": "/c2/4f/hash.jpg"},
            ],
        }
    }
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="999",
        title="Alias Case",
        content=metadata["post"]["content"],
        metadata=metadata,
        source_url="https://kemono.cr/fanbox/user/70479526/post/999",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "eO8wzPLjankw59mg6YeTMzxN.jpeg",
                "remote_url": "https://kemono.cr/c2/4f/hash.jpg",
                "local_path": f"post_{post_id}/eO8wzPLjankw59mg6YeTMzxN.jpeg",
                "kind": "attachment",
            }
        ],
    )

    file_path = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "eO8wzPLjankw59mg6YeTMzxN.jpeg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"fakejpg")

    client = app.test_client()
    detail = client.get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected_local = f"/files/post_{post_id}/eO8wzPLjankw59mg6YeTMzxN.jpeg".encode()
    assert b'href="' + expected_local + b'"' in detail.data
    assert b'src="' + expected_local + b'"' in detail.data


def test_post_detail_series_name_links_to_series_view(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Series Link Creator")
    series_id = db.create_series(creator_id, "Folder A")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="12345",
        external_post_id="1000",
        title="Series Linked Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/12345/post/1000",
    )

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected_series_link = f'href="/creators/{creator_id}?series_id={series_id}"'.encode()
    assert expected_series_link in detail.data
    assert b"Folder A" in detail.data


def test_post_detail_prefers_attachment_over_inline_same_name(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Priority Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="1001",
        title="Priority Case",
        content=(
            '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
            'same-name.jpeg" rel="noopener noreferrer"></a></p>'
        ),
        metadata={
            "post": {
                "attachments": [{"name": "same-name.jpeg", "path": "/hashes/same-name.jpg"}]
            }
        },
        source_url="https://kemono.cr/fanbox/user/70479526/post/1001",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "same-name.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/10791194/same-name.jpeg",
                "local_path": f"post_{post_id}/inline-same-name.jpeg",
                "kind": "inline_media",
            },
            {
                "name": "same-name.jpeg",
                "remote_url": "https://kemono.cr/hashes/same-name.jpg",
                "local_path": f"post_{post_id}/attachment-same-name.jpeg",
                "kind": "attachment",
            },
        ],
    )

    files_root = Path(app.config["FILES_DIR"]) / f"post_{post_id}"
    files_root.mkdir(parents=True, exist_ok=True)
    (files_root / "inline-same-name.jpeg").write_bytes(b"inline")
    (files_root / "attachment-same-name.jpeg").write_bytes(b"attachment")

    client = app.test_client()
    detail = client.get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected = f"/files/post_{post_id}/attachment-same-name.jpeg".encode()
    assert b'href="' + expected + b'"' in detail.data
    assert b'src="' + expected + b'"' in detail.data


def test_post_detail_falls_back_to_attachment_remote_when_local_missing(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Remote Fallback Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="1002",
        title="Fallback Case",
        content=(
            '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
            'same-name.jpeg" rel="noopener noreferrer"></a></p>'
        ),
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/1002",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "same-name.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/10791194/same-name.jpeg",
                "local_path": None,
                "kind": "inline_media",
            },
            {
                "name": "same-name.jpeg",
                "remote_url": "https://n3.kemono.cr/hashes/same-name.jpg",
                "local_path": None,
                "kind": "attachment",
            },
        ],
    )

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected = b'https://n3.kemono.cr/hashes/same-name.jpg'
    assert b'href="' + expected + b'"' in detail.data
    assert b'src="' + expected + b'"' in detail.data
    assert b"download missing" not in detail.data
    assert b"retry" in detail.data


def test_post_detail_dedupes_saved_files_that_point_to_same_local_file(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Saved Files Dedupe Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="2001",
        title="Dedupe Case",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/2001",
    )
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])

    local_rel = f"post_{post_id}/same.jpg"
    local_abs = tmp_path / "files" / local_rel
    local_abs.parent.mkdir(parents=True, exist_ok=True)
    local_abs.write_bytes(b"same")

    db.replace_attachments(
        post_id,
        [
            {
                "name": "same.jpg",
                "remote_url": "https://n1.kemono.cr/aa/bb/shared.jpg",
                "local_path": local_rel,
                "kind": "inline_media",
            },
            {
                "name": "same.jpg",
                "remote_url": "https://n2.kemono.cr/aa/bb/shared.jpg",
                "local_path": local_rel,
                "kind": "attachment",
            },
            {
                "name": "other.jpg",
                "remote_url": "https://n3.kemono.cr/xx/yy/other.jpg",
                "local_path": None,
                "kind": "attachment",
            },
        ],
        version_id=version_id,
    )

    response = app.test_client().get(f"/posts/{post_id}?version_id={version_id}")
    assert response.status_code == 200

    soup = BeautifulSoup(response.data, "html.parser")
    rows = soup.select(".post-file-list li")
    assert len(rows) == 2
    same_links = soup.select('.post-file-list a[title="same.jpg"]')
    assert len(same_links) == 1


def test_post_detail_includes_lightbox_hooks_for_inline_and_saved_images(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Lightbox Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="3001",
        title="Lightbox Case",
        content=(
            '<p><a href="https://n1.kemono.cr/aa/bb/content-image.jpg" rel="noopener noreferrer">'
            '<img src="https://n1.kemono.cr/aa/bb/content-image.jpg" alt="content-image.jpg"></a></p>'
        ),
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/3001",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "saved-image.jpg",
                "remote_url": "https://n2.kemono.cr/cc/dd/saved-image.jpg",
                "local_path": None,
                "kind": "attachment",
            }
        ],
    )

    response = app.test_client().get(f"/posts/{post_id}")
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "data-post-lightbox" in html
    assert "data-post-file-image-trigger" in html
    assert "/static/post_detail.js" in html

    soup = BeautifulSoup(response.data, "html.parser")
    inline_links = soup.select(".post-content a.post-image-link")
    assert inline_links


def test_post_detail_uses_metadata_kemono_url_for_fanbox_file_link_without_attachment_row(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Metadata Fallback Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="67922",
        external_post_id="6266002",
        title="vault",
        content=(
            '<p><a href="https://downloads.fanbox.cc/files/post/6266002/'
            'iYQR8ofim9yr8Iw0ONjv4E1A.zip" rel="noopener noreferrer nofollow">vault</a></p>'
        ),
        metadata={
            "post": {
                "attachments": [
                    {"name": "vault.zip", "path": "/68/d4/68d43b0c0b364665540eb944a7e2a1f75fe56c37d4ed8a7cfca2329ddf1a62fb.zip"}
                ]
            },
            "attachments": [
                {
                    "server": "https://n4.kemono.cr",
                    "name": "vault.zip",
                    "path": "/68/d4/68d43b0c0b364665540eb944a7e2a1f75fe56c37d4ed8a7cfca2329ddf1a62fb.zip",
                }
            ],
        },
        source_url="https://kemono.cr/fanbox/user/67922/post/6266002",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "cover.jpg",
                "remote_url": "https://kemono.cr/aa/bb/cover.jpg",
                "local_path": f"post_{post_id}/cover.jpg",
                "kind": "thumbnail",
            }
        ],
    )

    file_path = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "cover.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"ok")

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected = (
        b"https://n4.kemono.cr/data/68/d4/68d43b0c0b364665540eb944a7e2a1f75fe56c37d4ed8a7cfca2329ddf1a62fb.zip"
        b"?f=vault.zip"
    )
    assert b'href="' + expected + b'"' in detail.data


def test_retry_attachment_download_updates_missing_file(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Retry Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-user",
        external_post_id="1003",
        title="Retry Me",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-user/post/1003",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "broken.jpg",
                "remote_url": "https://n1.kemono.cr/path/broken.jpg",
                "local_path": f"post_{post_id}/broken.jpg",
                "kind": "attachment",
            }
        ],
    )

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"recovered")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    attachment_id = int(db.list_attachments(post_id)[0]["id"])
    response = app.test_client().post(
        f"/posts/{post_id}/attachments/{attachment_id}/retry",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith(f"/posts/{post_id}")

    saved_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "broken.jpg"
    assert saved_file.exists()
    assert saved_file.read_bytes() == b"recovered"
    updated = db.list_attachments(post_id)[0]
    assert updated["local_path"] == f"post_{post_id}/broken.jpg"


def test_retry_attachment_uses_kemono_data_url_fallback(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Retry Fallback Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="67922",
        external_post_id="1004",
        title="Retry Fallback",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/67922/post/1004",
    )
    original_remote = "https://n2.kemono.cr/53/68/536867864d85f832daa65e77715618c7f54435002860281650ae44401f47b117.txt"
    expected_fallback = (
        "https://n2.kemono.cr/data/53/68/536867864d85f832daa65e77715618c7f54435002860281650ae44401f47b117.txt"
        "?f=Text_for_translation.txt"
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "Text_for_translation.txt",
                "remote_url": original_remote,
                "local_path": None,
                "kind": "attachment",
            }
        ],
    )

    attempted_urls: list[str] = []

    def fake_download(remote_url, destination):  # noqa: ARG001
        attempted_urls.append(remote_url)
        if remote_url == original_remote:
            raise RuntimeError("404 Client Error")
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    client = app.test_client()
    detail = client.get(f"/posts/{post_id}")
    assert detail.status_code == 200
    assert expected_fallback.encode() in detail.data

    attachment_id = int(db.list_attachments(post_id)[0]["id"])
    response = client.post(
        f"/posts/{post_id}/attachments/{attachment_id}/retry",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith(f"/posts/{post_id}")
    assert attempted_urls == [original_remote, expected_fallback]


def test_edit_page_prettifies_html_content(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Editor Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="123",
        external_post_id="456",
        title="Edit Me",
        content="<p><strong>Hello</strong><br>World</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/123/post/456",
    )

    client = app.test_client()
    response = client.get(f"/posts/{post_id}/edit")

    assert response.status_code == 200
    assert b"<textarea id=\"content\" name=\"content\" rows=\"18\">" in response.data
    assert b"&lt;p&gt;" in response.data
    assert b"&lt;strong&gt;" in response.data
    assert b"Hello" in response.data


def test_edit_post_saves_thumbnail_focus_and_applies_to_creator_grid(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Focus Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="111",
        external_post_id="222",
        title="Focus Me",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/111/post/222",
        thumbnail_name="cover.jpg",
        thumbnail_remote_url="https://kemono.cr/a/b/cover.jpg",
        thumbnail_local_path=None,
    )

    version_row = db.get_post_version(post_id)
    assert version_row is not None
    version_id = int(version_row["id"])

    client = app.test_client()
    response = client.post(
        f"/posts/{post_id}/edit",
        data={
            "version_id": str(version_id),
            "version_label": "Original",
            "version_language": "",
            "title": "Focus Me",
            "series_id": "",
            "thumbnail_attachment_id": "__keep__",
            "thumbnail_focus_x": "22.5",
            "thumbnail_focus_y": "77.5",
            "content": "<p>Body</p>",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302

    creator_page = client.get(f"/creators/{creator_id}")
    assert creator_page.status_code == 200
    html = creator_page.data.decode("utf-8")
    assert "object-position: 22.5% 77.5%" in html


def test_edit_post_attachment_management_is_save_based_and_updates_inline_media(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Attachment Creator")
    remote_local = "https://n1.kemono.cr/a1/b2/local_one.jpg"
    remote_missing = "https://n2.kemono.cr/c3/d4/missing_two.jpg"
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="u-1",
        external_post_id="p-1",
        title="Attachment Post",
        content=f'<p><a href="{remote_local}">local</a></p>',
        metadata={
            "post": {
                "attachments": [
                    {"name": "extra.png", "path": "/z9/y8/extra.png"},
                ]
            }
        },
        source_url="https://kemono.cr/fanbox/user/u-1/post/p-1",
    )
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])

    local_rel = f"post_{post_id}/local_one.jpg"
    local_abs = Path(tmp_path / "files" / local_rel)
    local_abs.parent.mkdir(parents=True, exist_ok=True)
    local_abs.write_bytes(b"local-bytes")

    db.replace_attachments(
        post_id,
        [
            {"name": "local_one.jpg", "remote_url": remote_local, "local_path": local_rel, "kind": "attachment"},
            {"name": "missing_two.jpg", "remote_url": remote_missing, "local_path": None, "kind": "attachment"},
        ],
        version_id=version_id,
    )
    manual_version_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=version_id,
        label="Manual edit",
        language=None,
        set_default=False,
    )

    rows = db.list_attachments(post_id, version_id=manual_version_id)
    by_name = {str(row["name"]): int(row["id"]) for row in rows}
    local_id = by_name["local_one.jpg"]
    missing_id = by_name["missing_two.jpg"]

    client = app.test_client()
    edit_page = client.get(f"/posts/{post_id}/edit?version_id={manual_version_id}")
    assert edit_page.status_code == 200
    assert "extra.png" in edit_page.data.decode("utf-8")

    before_detail = client.get(f"/posts/{post_id}?version_id={manual_version_id}")
    assert before_detail.status_code == 200
    assert f"/files/{local_rel}" in before_detail.data.decode("utf-8")

    response = client.post(
        f"/posts/{post_id}/edit",
        data=MultiDict(
            [
                ("version_id", str(manual_version_id)),
                ("version_label", "Manual edit"),
                ("version_language", ""),
                ("title", "Attachment Post"),
                ("series_id", ""),
                ("thumbnail_attachment_id", "__keep__"),
                ("thumbnail_focus_x", "50"),
                ("thumbnail_focus_y", "50"),
                ("content", f'<p><a href="{remote_local}">local</a></p>'),
                (f"attachment_keep_id_{local_id}", "0"),
                (f"attachment_keep_id_{local_id}", "1"),
                (f"attachment_name_id_{local_id}", "renamed local"),
                (f"attachment_keep_local_id_{local_id}", "0"),
                (f"attachment_keep_id_{missing_id}", "0"),
                (f"attachment_keep_id_{missing_id}", "1"),
                (f"attachment_name_id_{missing_id}", "missing-two.txt"),
                ("attachment_add_src_0", "0"),
                ("attachment_add_src_0", "1"),
                ("attachment_name_src_0", "extra-localized.png"),
            ]
        ),
        follow_redirects=False,
    )
    assert response.status_code == 302

    updated_rows = db.list_attachments(post_id, version_id=manual_version_id)
    assert len(updated_rows) == 3
    updated_by_remote = {str(row["remote_url"]): row for row in updated_rows}
    updated_local = updated_by_remote[remote_local]
    updated_missing = updated_by_remote[remote_missing]
    updated_extra = updated_by_remote["https://kemono.cr/z9/y8/extra.png"]

    assert str(updated_local["name"]).startswith("renamed_local")
    assert updated_local["local_path"] is None
    assert not local_abs.exists()
    assert str(updated_missing["name"]) == "missing-two.txt"
    assert updated_missing["local_path"] is None
    assert str(updated_extra["name"]) == "extra-localized.png"
    assert updated_extra["local_path"] is None

    after_detail = client.get(f"/posts/{post_id}?version_id={manual_version_id}")
    assert after_detail.status_code == 200
    html_after = after_detail.data.decode("utf-8")
    assert f"/files/{local_rel}" not in html_after
    assert "/a1/b2/local_one.jpg" in html_after


def test_edit_post_dedupes_local_file_for_same_name_and_same_bytes(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Collapse Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="u-2",
        external_post_id="p-2",
        title="Collapse Post",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-2/post/p-2",
    )
    source_version = db.get_post_version(post_id)
    assert source_version is not None
    source_version_id = int(source_version["id"])
    manual_version_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=source_version_id,
        label="Manual",
        language=None,
        set_default=False,
    )

    db.replace_attachments(
        post_id,
        [
            {
                "name": "first.jpg",
                "remote_url": "https://n1.kemono.cr/a/1/first-source.jpg",
                "local_path": f"post_{post_id}/first.jpg",
                "kind": "attachment",
            },
            {
                "name": "second.jpg",
                "remote_url": "https://n2.kemono.cr/b/2/second-source.jpg",
                "local_path": f"post_{post_id}/second.jpg",
                "kind": "attachment",
            },
        ],
        version_id=manual_version_id,
    )
    files_dir = tmp_path / "files" / f"post_{post_id}"
    files_dir.mkdir(parents=True, exist_ok=True)
    (files_dir / "first.jpg").write_bytes(b"same-image")
    (files_dir / "second.jpg").write_bytes(b"same-image")
    rows = db.list_attachments(post_id, version_id=manual_version_id)
    by_name = {str(row["name"]): int(row["id"]) for row in rows}
    first_id = by_name["first.jpg"]
    second_id = by_name["second.jpg"]

    client = app.test_client()
    response = client.post(
        f"/posts/{post_id}/edit",
        data=MultiDict(
            [
                ("version_id", str(manual_version_id)),
                ("version_label", "Manual"),
                ("version_language", ""),
                ("title", "Collapse Post"),
                ("series_id", ""),
                ("thumbnail_attachment_id", "__keep__"),
                ("thumbnail_focus_x", "50"),
                ("thumbnail_focus_y", "50"),
                ("content", "<p>Body</p>"),
                (f"attachment_keep_id_{first_id}", "0"),
                (f"attachment_keep_id_{first_id}", "1"),
                (f"attachment_keep_id_{second_id}", "0"),
                (f"attachment_keep_id_{second_id}", "1"),
                (f"attachment_name_id_{first_id}", "merged name.jpg"),
                (f"attachment_name_id_{second_id}", "merged name.jpg"),
            ]
        ),
        follow_redirects=False,
    )
    assert response.status_code == 302

    updated_rows = db.list_attachments(post_id, version_id=manual_version_id)
    assert len(updated_rows) == 2
    assert {str(row["name"]) for row in updated_rows} == {"merged_name.jpg"}
    local_paths = {
        str(row["local_path"])
        for row in updated_rows
        if isinstance(row["local_path"], str) and row["local_path"].strip()
    }
    assert len(local_paths) == 1
    shared_local = next(iter(local_paths))
    assert (tmp_path / "files" / shared_local).is_file()
    assert not (files_dir / "merged_name_2.jpg").exists()


def test_creator_folder_filter_and_sort_modes(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Folder Creator")
    main_series_id = db.create_series(creator_id, "Main Series")
    side_series_id = db.create_series(creator_id, "Side Series")

    db.upsert_post(
        creator_id=creator_id,
        series_id=main_series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="100",
        title="Beta Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/100",
        published_at="2025-01-02T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=side_series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="101",
        title="Alpha Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/101",
        published_at="2025-01-01T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="102",
        title="Gamma Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/102",
        published_at="2025-01-03T00:00:00",
    )

    client = app.test_client()
    root = client.get(f"/creators/{creator_id}")
    assert root.status_code == 200
    soup = BeautifulSoup(root.data, "html.parser")
    folder_names = [node.get_text(strip=True) for node in soup.select(".folder-explorer-grid .folder-tile-name")]
    assert "Unsorted" not in folder_names
    filter_links = [anchor.get("href") or "" for anchor in soup.select(".creator-sort-bar a")]
    assert any("folder=unsorted" in href for href in filter_links)
    assert any("All posts" in anchor.get_text(strip=True) for anchor in soup.select(".creator-sort-bar a"))
    assert any("Unsorted" in anchor.get_text(strip=True) for anchor in soup.select(".creator-sort-bar a"))

    title_sorted = client.get(f"/creators/{creator_id}?sort=title&direction=asc")
    assert title_sorted.status_code == 200
    title_html = title_sorted.data.decode("utf-8")
    assert title_html.index("Alpha Post") < title_html.index("Beta Post") < title_html.index("Gamma Post")

    unsorted_only = client.get(f"/creators/{creator_id}?folder=unsorted")
    assert unsorted_only.status_code == 200
    unsorted_html = unsorted_only.data.decode("utf-8")
    assert "Gamma Post" in unsorted_html
    assert "Alpha Post" not in unsorted_html
    assert "Beta Post" not in unsorted_html

    series_only = client.get(f"/creators/{creator_id}?series_id={main_series_id}")
    assert series_only.status_code == 200
    series_html = series_only.data.decode("utf-8")
    assert "Beta Post" in series_html
    assert "Alpha Post" not in series_html
    assert "Gamma Post" not in series_html

    published_desc = client.get(f"/creators/{creator_id}?sort=published&direction=desc")
    assert published_desc.status_code == 200
    published_desc_html = published_desc.data.decode("utf-8")
    assert published_desc_html.index("Gamma Post") < published_desc_html.index("Beta Post") < published_desc_html.index(
        "Alpha Post"
    )


def test_creator_does_not_show_unsorted_folder_tile_without_series(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("No Series Creator")
    db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="300",
        title="Lone Unsorted Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/300",
        published_at="2025-01-03T00:00:00",
    )

    response = app.test_client().get(f"/creators/{creator_id}")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    folder_names = [node.get_text(strip=True) for node in soup.select(".folder-explorer-grid .folder-tile-name")]
    assert "Unsorted" not in folder_names
    assert not any("folder=unsorted" in (anchor.get("href") or "") for anchor in soup.select(".folder-explorer-grid a"))
    assert any("folder=unsorted" in (anchor.get("href") or "") for anchor in soup.select(".creator-sort-bar a"))


def test_series_folder_tile_uses_first_entry_thumbnail(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Series Thumb Creator")
    series_id = db.create_series(creator_id, "Main Arc")

    db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="301",
        title="Older",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/301",
        thumbnail_local_path="post_301/old.jpg",
        published_at="2025-01-01T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="302",
        title="Newer",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/302",
        thumbnail_local_path="post_302/new.jpg",
        published_at="2025-01-02T00:00:00",
    )

    series_rows = db.list_series(creator_id)
    assert len(series_rows) == 1
    assert series_rows[0]["cover_thumbnail_local_path"] == "post_302/new.jpg"

    client = app.test_client()
    response = client.get(f"/creators/{creator_id}")
    assert response.status_code == 200
    assert b'class="folder-tile-thumb"' in response.data
    assert b"/files/post_302/new.jpg" in response.data


def test_creator_import_context_and_series_folder_metadata_mode(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Context Creator")
    series_id = db.create_series(
        creator_id,
        "Folder A",
        description="Folder description",
        tags_text="tag-a, tag-b",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="uid-1",
        external_post_id="700",
        title="One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/uid-1/post/700",
        published_at="2025-01-01T00:00:00",
    )

    client = app.test_client()

    root = client.get(f"/creators/{creator_id}")
    assert root.status_code == 200
    expected_root_import = f'href="/import?creator_id={creator_id}"'.encode()
    assert b'class="button-link button-ghost creator-import-action"' in root.data
    assert expected_root_import in root.data
    assert b"Import here" not in root.data
    assert b'class="folder-explorer-grid"' in root.data

    folder = client.get(f"/creators/{creator_id}?series_id={series_id}")
    assert folder.status_code == 200
    expected_import = f'href="/import?creator_id={creator_id}&amp;series_id={series_id}"'.encode()
    assert b'class="button-link button-ghost creator-import-action"' in folder.data
    assert expected_import in folder.data
    assert b'class="series-meta-title"' in folder.data
    assert b'Folder A' in folder.data
    assert b'Folder description' in folder.data
    assert b'tag-a' in folder.data
    assert b'tag-b' in folder.data
    assert b'Series Details' not in folder.data
    assert b'Series / Folder A' not in folder.data
    assert b'folder-explorer-grid' not in folder.data


def test_series_metadata_update_flow(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Meta Creator")
    series_id = db.create_series(creator_id, "Before")

    response = app.test_client().post(
        f"/creators/{creator_id}/series/{series_id}",
        data={
            "name": "After",
            "description": "Updated description",
            "tags_text": "tag one, tag two",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    updated = next(row for row in db.list_series(creator_id) if int(row["id"]) == series_id)
    assert updated["name"] == "After"
    assert updated["description"] == "Updated description"
    assert updated["tags_text"] == "tag one, tag two"


def test_creator_edit_flow_updates_metadata(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Creator Before")
    client = app.test_client()

    edit_get = client.get(f"/creators/{creator_id}/edit")
    assert edit_get.status_code == 200
    assert b"Edit Creator" in edit_get.data
    assert b"Creator Before" in edit_get.data

    edit_post = client.post(
        f"/creators/{creator_id}/edit",
        data={
            "name": "Creator After",
            "description": "Creator description",
            "tags_text": "tag-a, tag-b",
        },
        follow_redirects=False,
    )
    assert edit_post.status_code == 302
    assert edit_post.headers["Location"].endswith(f"/creators/{creator_id}")

    updated = db.get_creator(creator_id)
    assert updated is not None
    assert updated["name"] == "Creator After"
    assert updated["description"] == "Creator description"
    assert updated["tags_text"] == "tag-a, tag-b"


def test_creator_edit_duplicate_name_is_rejected(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Creator A")
    db.create_creator("Creator B")

    response = app.test_client().post(
        f"/creators/{creator_id}/edit",
        data={"name": "Creator B", "description": "", "tags_text": ""},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith(f"/creators/{creator_id}/edit")
    unchanged = db.get_creator(creator_id)
    assert unchanged is not None
    assert unchanged["name"] == "Creator A"


def test_creator_description_and_tags_show_only_on_all_posts_view(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Profile Creator")
    db.update_creator(
        creator_id,
        name="Profile Creator",
        description="**Bold line**\n\nSecond line",
        tags_text="tag-a, tag-b",
    )
    series_id = db.create_series(creator_id, "Folder One")

    client = app.test_client()
    root = client.get(f"/creators/{creator_id}")
    assert root.status_code == 200
    assert b'class="creator-profile-meta"' in root.data
    assert b"<strong>Bold line</strong>" in root.data
    assert b"Second line" in root.data
    assert b"creator-profile-tag" in root.data
    assert b"tag-a" in root.data
    assert b"tag-b" in root.data

    folder = client.get(f"/creators/{creator_id}?series_id={series_id}")
    assert folder.status_code == 200
    assert b'class="creator-profile-meta"' not in folder.data


def test_delete_post_removes_files_and_record(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Delete Post Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="delete-user",
        external_post_id="501",
        title="Delete Me",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/delete-user/post/501",
    )
    post_dir = Path(app.config["FILES_DIR"]) / f"post_{post_id}"
    post_dir.mkdir(parents=True, exist_ok=True)
    (post_dir / "sample.txt").write_text("x", encoding="utf-8")

    response = app.test_client().post(f"/posts/{post_id}/delete", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith(f"/creators/{creator_id}")
    assert db.get_post(post_id) is None
    assert not post_dir.exists()


def test_delete_creator_removes_posts_files_and_icon(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Delete Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="delete-user",
        external_post_id="601",
        title="Delete Creator Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/delete-user/post/601",
    )

    post_dir = Path(app.config["FILES_DIR"]) / f"post_{post_id}"
    post_dir.mkdir(parents=True, exist_ok=True)
    (post_dir / "sample.txt").write_text("x", encoding="utf-8")

    icon_rel = "fanbox_delete-user.jpg"
    icon_path = Path(app.config["ICONS_DIR"]) / icon_rel
    icon_path.parent.mkdir(parents=True, exist_ok=True)
    icon_path.write_bytes(b"icon")
    db.update_creator_icon(
        creator_id,
        icon_remote_url="https://img.kemono.cr/icons/fanbox/delete-user",
        icon_local_path=icon_rel,
    )

    response = app.test_client().post(f"/creators/{creator_id}/delete", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")
    assert db.get_creator(creator_id) is None
    assert db.get_post(post_id) is None
    assert not post_dir.exists()
    assert not icon_path.exists()


def test_import_can_add_non_default_version_to_existing_post(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    payload = {
        "post": {
            "title": "Translated Title",
            "content": "translated content",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    creator_id = db.create_creator("Versioned Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Original Title",
        content="jp content",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )

    client = app.test_client()
    response = client.post(
        "/import/commit",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "200",
            "import_target_mode": "existing",
            "target_post_id": str(post_id),
            "set_as_default": "0",
            "version_label": "EN TL",
            "version_language": "en",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].startswith(f"/posts/{post_id}?version_id=")

    versions = db.list_post_versions(post_id)
    assert len(versions) == 2
    default_version = next(row for row in versions if row["is_default"])
    assert default_version["source_post_id"] == "100"
    imported = next(row for row in versions if row["source_post_id"] == "200")
    assert imported["label"] == "EN TL"
    assert imported["language"] == "en"


def test_import_commit_force_target_post_version_ignores_new_mode(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    payload = {
        "post": {
            "title": "Alt Translation",
            "content": "alt content",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        destination = Path(icons_root) / f"{service}_{user_id}.jpg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"icon")
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", destination)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    creator_id = db.create_creator("Forced Target Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Original Title",
        content="jp content",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )

    client = app.test_client()
    response = client.post(
        "/import/commit",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "300",
            "import_target_mode": "new",
            "target_post_id": str(post_id),
            "force_target_post_version": "1",
            "overwrite_matching_version": "0",
            "set_as_default": "0",
            "version_label": "KR TL",
            "version_language": "ko",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].startswith(f"/posts/{post_id}?version_id=")

    creator_posts = db.list_posts_for_creator(creator_id)
    assert len(creator_posts) == 1
    versions = db.list_post_versions(post_id)
    assert len(versions) == 2
    imported = next(row for row in versions if row["source_post_id"] == "300")
    assert imported["label"] == "KR TL"
    assert imported["language"] == "ko"


def test_post_detail_uses_requested_version_id(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Version Switch Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="321",
        external_post_id="654",
        title="JP title",
        content="jp body",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/321/post/654",
    )

    base_version = db.list_post_versions(post_id)[0]
    clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(base_version["id"]),
        label="EN",
        language="en",
        set_default=False,
    )
    clone_row = db.get_post_version(post_id, clone_id)
    assert clone_row is not None
    db.update_post_version(
        version_id=clone_id,
        label="EN",
        language="en",
        title="English title",
        content="english body",
        thumbnail_name=clone_row["thumbnail_name"],
        thumbnail_remote_url=clone_row["thumbnail_remote_url"],
        thumbnail_local_path=clone_row["thumbnail_local_path"],
        published_at=clone_row["published_at"],
        edited_at=clone_row["edited_at"],
        next_external_post_id=clone_row["next_external_post_id"],
        prev_external_post_id=clone_row["prev_external_post_id"],
        metadata={},
        source_url=clone_row["source_url"],
    )

    detail = app.test_client().get(f"/posts/{post_id}?version_id={clone_id}")
    assert detail.status_code == 200
    assert b"English title" in detail.data


def test_edit_version_reimport_link_only_for_non_manual_versions(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Reimport Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="777",
        external_post_id="888",
        title="Base",
        content="base",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/777/post/888",
    )
    versions = db.list_post_versions(post_id)
    assert versions
    non_manual_id = int(versions[0]["id"])

    client = app.test_client()
    non_manual_page = client.get(f"/posts/{post_id}/edit?version_id={non_manual_id}")
    assert non_manual_page.status_code == 200
    assert b"Import new version" in non_manual_page.data
    assert b"force_target_post_version=1" in non_manual_page.data
    assert b"Reimport and overwrite this version" in non_manual_page.data
    assert b"/import?" in non_manual_page.data
    assert b"url=https://kemono.cr/fanbox/user/777/post/888" in non_manual_page.data

    manual_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=non_manual_id,
        label="Manual",
        language="en",
        set_default=False,
    )
    manual_page = client.get(f"/posts/{post_id}/edit?version_id={manual_id}")
    assert manual_page.status_code == 200
    assert b"Reimport and overwrite this version" not in manual_page.data


def test_resolve_link_matches_version_source_tuple(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Resolver Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="700",
        external_post_id="100",
        title="Base",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/700/post/100",
    )

    db.create_post_version(
        post_id=post_id,
        label="EN",
        language="en",
        is_manual=False,
        source_service="fanbox",
        source_user_id="700",
        source_post_id="200",
        title="EN",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/700/post/200",
        set_default=False,
    )

    response = app.test_client().get("/links/resolve?service=fanbox&user=700&post=200")
    assert response.status_code == 302
    assert response.headers["Location"].endswith(f"/posts/{post_id}")
