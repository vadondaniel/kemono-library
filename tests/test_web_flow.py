import sqlite3
import threading
from pathlib import Path
import time
from bs4 import BeautifulSoup
import pytest
from werkzeug.datastructures import MultiDict

from kemono_library.db import LibraryDB
from kemono_library.web import _import_post_into_library, create_app


def _page_title(response) -> str:
    soup = BeautifulSoup(response.data, "html.parser")
    node = soup.select_one("title")
    return node.get_text(strip=True) if node is not None else ""


def _expected_page_title(*parts: str) -> str:
    return " \u00b7 ".join([*parts, "Kemono Library"])


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
    assert _page_title(preview) == _expected_page_title("Post 100", "Import Preview")

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
    assert b"data-nav-replace-href" in detail.data
    assert _page_title(detail) == _expected_page_title("Post 100", "Creator A")

    unresolved = client.get("/links/resolve?service=fanbox&post=100&user=70479526")
    assert unresolved.status_code == 302
    assert unresolved.headers["Location"].endswith("/posts/1")


def test_import_corrects_extensionless_image_name_from_downloaded_bytes(tmp_path, monkeypatch):
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
            "title": "Extensionless Asset",
            "content": "",
            "user": "70479526",
            "attachments": [{"name": "loop_image", "path": "/data/x/loop_image"}],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"GIF89a" + b"\x00" * 24)

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    creator_response = client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)
    assert creator_response.status_code == 302

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
    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert len(attachments) == 1
    assert attachments[0]["name"] == "loop_image.gif"
    assert attachments[0]["local_path"] == "post_1/loop_image.gif"
    saved_file = Path(app.config["FILES_DIR"]) / "post_1" / "loop_image.gif"
    assert saved_file.is_file()


def test_import_bounds_very_long_attachment_name_for_local_path(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    long_name = f"https_www.patreon.com_media-u_{'A' * 340}_301638009"
    payload = {
        "post": {
            "title": "Long Name Import",
            "content": "",
            "user": "70479526",
            "attachments": [{"name": long_name, "path": "/data/x/long-image.jpg"}],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    creator_response = client.post("/creators", data={"name": "Creator Long"}, follow_redirects=False)
    assert creator_response.status_code == 302

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

    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert len(attachments) == 1
    local_path = str(attachments[0]["local_path"])
    filename = Path(local_path).name
    assert len(filename) <= 180
    assert filename
    assert not filename.startswith(".")
    saved_file = Path(app.config["FILES_DIR"]) / local_path
    assert saved_file.is_file()


def test_import_reuses_existing_extensionless_image_and_still_corrects_extension(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Creator Existing")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Existing Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "loop_image.jpg",
                "remote_url": "https://kemono.cr/data/x/loop_image",
                "local_path": f"post_{post_id}/loop_image.jpg",
                "kind": "attachment",
            }
        ],
    )
    existing_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "loop_image.jpg"
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.write_bytes(b"GIF89a" + b"\x00" * 24)

    payload = {
        "post": {
            "title": "Extensionless Asset",
            "content": "",
            "user": "70479526",
            "attachments": [{"name": "loop_image", "path": "/data/x/loop_image"}],
        },
        "attachments": [],
    }
    download_calls = {"count": 0}

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        download_calls["count"] += 1
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"GIF89a" + b"\x00" * 24)

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    _, version_id = _import_post_into_library(
        db,
        files_base=Path(app.config["FILES_DIR"]),
        icons_base=Path(app.config["ICONS_DIR"]),
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        user_id="70479526",
        post_id="100",
        import_target_mode="existing",
        target_post_id=post_id,
        overwrite_matching_version=True,
        set_as_default=True,
        version_label=None,
        version_language=None,
        requested_title=None,
        requested_content=None,
        requested_published_at=None,
        requested_edited_at=None,
        requested_next_external_post_id=None,
        requested_prev_external_post_id=None,
        tags_text=None,
        field_presence={},
        selected_attachment_indices={"0"},
    )
    assert version_id > 0
    assert download_calls["count"] == 0

    attachments = db.list_attachments(post_id)
    assert len(attachments) == 1
    assert attachments[0]["name"] == "loop_image.gif"
    assert attachments[0]["local_path"] == f"post_{post_id}/loop_image.gif"
    assert not existing_file.exists()
    assert (Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "loop_image.gif").is_file()


def test_import_includes_embed_links_without_attempting_download(tmp_path, monkeypatch):
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
            "title": "Embed Link Post",
            "content": "<p>Has external embed.</p>",
            "user": "3085566",
            "embed": {
                "url": "https://inkyleafpatreononly.blogspot.com/2024/08/foxy-fairy-tale.html",
                "subject": "Foxy Fairy-Tale",
            },
            "attachments": [],
        },
        "attachments": [],
    }
    download_calls: list[str] = []

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        download_calls.append(str(remote_url))
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    creator_response = client.post("/creators", data={"name": "Embed Creator"}, follow_redirects=False)
    assert creator_response.status_code == 302

    preview = client.post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/patreon/user/3085566/post/111110523",
            "creator_id": "1",
            "series_id": "",
        },
    )
    assert preview.status_code == 200
    assert b"Foxy Fairy-Tale" in preview.data

    commit = client.post(
        "/import/commit",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "patreon",
            "user_id": "3085566",
            "post_id": "111110523",
            "selected_attachment": "0",
        },
        follow_redirects=False,
    )
    assert commit.status_code == 302
    assert commit.headers["Location"].endswith("/posts/1")
    assert download_calls == []

    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert len(attachments) == 1
    assert attachments[0]["kind"] == "embed_link"
    assert attachments[0]["local_path"] is None
    assert attachments[0]["remote_url"] == "https://inkyleafpatreononly.blogspot.com/2024/08/foxy-fairy-tale.html"


def test_import_preview_and_commit_supports_coomer_urls(tmp_path, monkeypatch):
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
            "title": "Coomer Post",
            "content": "<p>from coomer</p>",
            "user": "belledelphine",
            "attachments": [],
        },
        "attachments": [],
    }
    seen_hosts: list[str] = []

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        seen_hosts.append(ref.host)
        return payload

    def fake_icon_download(service, user_id, icons_root, base_url=None):  # noqa: ARG001
        return (f"https://img.coomer.st/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    client = app.test_client()
    creator_response = client.post("/creators", data={"name": "Coomer Creator"}, follow_redirects=False)
    assert creator_response.status_code == 302

    preview = client.post(
        "/import/preview",
        data={
            "post_url": "https://coomer.st/onlyfans/user/belledelphine/post/997022061",
            "creator_id": "1",
            "series_id": "",
        },
    )
    assert preview.status_code == 200
    assert b'value="https://coomer.st"' in preview.data

    commit = client.post(
        "/import/commit",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "onlyfans",
            "user_id": "belledelphine",
            "post_id": "997022061",
            "source_base": "https://coomer.st",
        },
        follow_redirects=False,
    )
    assert commit.status_code == 302
    assert commit.headers["Location"].endswith("/posts/1")
    assert seen_hosts == ["coomer.st", "coomer.st"]
    post = app.db.get_post(1)  # type: ignore[attr-defined]
    assert post["source_url"] == "https://coomer.st/onlyfans/user/belledelphine/post/997022061"


def test_import_form_renders_tabbed_quick_import_ui(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    creator_id = app.db.create_creator("Tabbed Import Creator")  # type: ignore[attr-defined]

    client = app.test_client()
    response = client.get(f"/import?creator_id={creator_id}&tab=quick")
    assert response.status_code == 200
    assert _page_title(response) == _expected_page_title("Quick Import")
    soup = BeautifulSoup(response.data, "html.parser")
    shell = soup.select_one("[data-import-form-tabs]")
    assert shell is not None
    assert shell.get("data-import-default-tab") == "quick"
    assert shell.get("data-import-title-single") == _expected_page_title("Import Post")
    assert shell.get("data-import-title-quick") == _expected_page_title("Quick Import")
    assert soup.select_one("[data-import-tab-trigger='single']") is not None
    assert soup.select_one("[data-import-tab-trigger='quick']") is not None
    assert soup.select_one("[data-quick-link-input]") is not None
    assert soup.select_one("[data-quick-link-paste]") is not None
    assert soup.select_one("[data-quick-link-list]") is not None
    assert soup.select_one("[data-quick-hidden-urls]") is not None
    assert b"/static/import_form.js" in response.data

    single_response = client.get(f"/import?creator_id={creator_id}&tab=single")
    assert single_response.status_code == 200
    assert _page_title(single_response) == _expected_page_title("Import Post")


def test_quick_import_multiple_posts_metadata_only_skips_downloads(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Batch Creator")

    payload_by_post_id = {
        "100": {
            "post": {
                "title": "Batch 100",
                "content": "",
                "user": "70479526",
                "attachments": [{"name": "100-a.jpg", "path": "/data/100-a.jpg"}],
            },
            "attachments": [],
        },
        "101": {
            "post": {
                "title": "Batch 101",
                "content": "",
                "user": "70479526",
                "attachments": [{"name": "101-a.jpg", "path": "/data/101-a.jpg"}],
            },
            "attachments": [],
        },
    }
    download_calls: list[tuple[str, str]] = []

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        payload = payload_by_post_id.get(str(ref.post_id))
        if payload is None:
            raise AssertionError(f"Unexpected post id {ref.post_id}")
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        download_calls.append((str(remote_url), str(destination)))
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    response = app.test_client().post(
        "/import/quick",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "post_urls": "\n".join(
                [
                    "https://kemono.cr/fanbox/user/70479526/post/100",
                    "https://kemono.cr/fanbox/user/70479526/post/101",
                ]
            ),
            "skip_attachment_downloads": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].startswith(f"/import?creator_id={creator_id}")
    assert not download_calls

    imported_100 = db.find_post_by_source("fanbox", "70479526", "100")
    imported_101 = db.find_post_by_source("fanbox", "70479526", "101")
    assert imported_100 is not None
    assert imported_101 is not None
    attachments_100 = db.list_attachments(int(imported_100["id"]))
    attachments_101 = db.list_attachments(int(imported_101["id"]))
    assert len(attachments_100) == 1
    assert len(attachments_101) == 1
    assert attachments_100[0]["local_path"] is None
    assert attachments_101[0]["local_path"] is None


def test_quick_import_accepts_hidden_post_url_values(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Hidden Field Batch Creator")

    payload = {
        "post": {
            "title": "Batch 200",
            "content": "",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        assert str(ref.post_id) == "200"
        return payload

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    response = app.test_client().post(
        "/import/quick",
        data=MultiDict(
            [
                ("creator_id", str(creator_id)),
                ("series_id", ""),
                ("post_urls", ""),
                ("post_url_values", "https://kemono.cr/fanbox/user/70479526/post/200"),
                ("skip_attachment_downloads", "1"),
            ]
        ),
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"] == "/posts/1"
    imported = db.find_post_by_source("fanbox", "70479526", "200")
    assert imported is not None


def test_quick_import_reports_failures_and_prefills_failed_urls(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Batch Creator")

    payload = {
        "post": {
            "title": "Batch 100",
            "content": "",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        assert str(ref.post_id) == "100"
        return payload

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    response = app.test_client().post(
        "/import/quick",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "post_urls": "\n".join(
                [
                    "https://kemono.cr/fanbox/user/70479526/post/100",
                    "https://example.com/not-kemono",
                ]
            ),
            "skip_attachment_downloads": "1",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Quick import finished: 1 succeeded, 1 failed." in response.data
    assert b"Only supported archive post links are supported." in response.data
    assert b"https://example.com/not-kemono" in response.data
    assert db.find_post_by_source("fanbox", "70479526", "100") is not None


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
                {"name": "c.jpg", "path": "/aa/bb/c.jpg"},
                {"name": "d.jpg", "path": "/aa/bb/d.jpg"},
                {"name": "e.jpg", "path": "/aa/bb/e.jpg"},
            ],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        time.sleep(0.04)
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
            "import_target_mode": "new",
            "overwrite_matching_version": "0",
            "selected_attachment": ["0", "1", "2", "3", "4"],
        },
    )
    assert start.status_code == 200
    start_payload = start.get_json()
    assert isinstance(start_payload, dict)
    status_url = start_payload.get("status_url")
    assert isinstance(status_url, str)

    final_status: dict | None = None
    saw_partial_progress = False
    for _ in range(250):
        status_response = client.get(status_url)
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert isinstance(status_payload, dict)
        if status_payload.get("status") == "running":
            total = int(status_payload.get("total") or 0)
            completed = int(status_payload.get("completed") or 0)
            if total == 5 and 0 < completed < total:
                saw_partial_progress = True
        if status_payload.get("status") == "completed":
            final_status = status_payload
            break
        if status_payload.get("status") == "failed":
            raise AssertionError(status_payload.get("error"))
        time.sleep(0.01)

    assert final_status is not None
    assert final_status["redirect_url"] == "/posts/1"
    assert saw_partial_progress is True
    assert final_status["total"] == 5
    assert final_status["completed"] == 5


def test_import_downloads_use_three_worker_concurrency(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Concurrent Import Creator")

    payload = {
        "post": {
            "title": "Concurrency",
            "content": "",
            "user": "70479526",
            "attachments": [
                {"name": f"{idx}.jpg", "path": f"/aa/bb/{idx}.jpg"}
                for idx in range(8)
            ],
        },
        "attachments": [],
    }

    active_downloads = 0
    max_active_downloads = 0
    guard = threading.Lock()

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        nonlocal active_downloads, max_active_downloads
        with guard:
            active_downloads += 1
            max_active_downloads = max(max_active_downloads, active_downloads)
        try:
            time.sleep(0.03)
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            Path(destination).write_bytes(b"ok")
        finally:
            with guard:
                active_downloads -= 1

    def fake_icon_download(service, user_id, icons_root):  # noqa: ARG001
        return (f"https://img.kemono.cr/icons/{service}/{user_id}", None)

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    monkeypatch.setattr("kemono_library.web.download_creator_icon", fake_icon_download)

    _import_post_into_library(
        db,
        files_base=Path(app.config["FILES_DIR"]),
        icons_base=Path(app.config["ICONS_DIR"]),
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        user_id="70479526",
        post_id="901",
        import_target_mode="new",
        target_post_id=None,
        overwrite_matching_version=True,
        set_as_default=True,
        version_label=None,
        version_language=None,
        requested_title=None,
        requested_content=None,
        requested_published_at=None,
        requested_edited_at=None,
        requested_next_external_post_id=None,
        requested_prev_external_post_id=None,
        tags_text=None,
        field_presence={},
        selected_attachment_indices={str(idx) for idx in range(8)},
    )

    assert 2 <= max_active_downloads <= 3


def test_import_start_queues_later_jobs_until_running_import_finishes(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    allow_first_download = threading.Event()
    first_download_started = threading.Event()

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        post_num = str(ref.post_id)
        return {
            "post": {
                "title": f"Title {post_num}",
                "content": f"Body {post_num}",
                "user": str(ref.user_id),
                "attachments": [
                    {
                        "name": f"{post_num}.jpg",
                        "path": f"/data/{post_num}.jpg",
                    }
                ],
            },
            "attachments": [],
        }

    def fake_download(remote_url, destination):  # noqa: ARG001
        if remote_url.endswith("/200.jpg"):
            first_download_started.set()
            if not allow_first_download.wait(timeout=2):
                raise AssertionError("timed out waiting to release first queued import")
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

    first_start = client.post(
        "/import/start",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "200",
            "import_target_mode": "new",
            "overwrite_matching_version": "0",
            "selected_attachment": ["0"],
        },
    )
    assert first_start.status_code == 200
    first_status_url = first_start.get_json()["status_url"]

    assert first_download_started.wait(timeout=2)

    second_start = client.post(
        "/import/start",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "201",
            "import_target_mode": "new",
            "overwrite_matching_version": "0",
            "selected_attachment": ["0"],
        },
    )
    assert second_start.status_code == 200
    second_status_url = second_start.get_json()["status_url"]

    queued_status = client.get(second_status_url).get_json()
    assert queued_status["status"] == "queued"
    assert queued_status["queue_position"] == 1
    assert "Queue position: 1" in queued_status["message"]

    allow_first_download.set()

    second_running_seen = False
    second_completed = False
    for _ in range(200):
        first_status = client.get(first_status_url).get_json()
        second_status = client.get(second_status_url).get_json()
        if first_status["status"] == "failed":
            raise AssertionError(first_status["error"])
        if second_status["status"] == "failed":
            raise AssertionError(second_status["error"])
        if second_status["status"] == "running":
            second_running_seen = True
        if first_status["status"] == "completed" and second_status["status"] == "completed":
            second_completed = True
            break
        time.sleep(0.01)

    assert second_running_seen is True
    assert second_completed is True


def test_import_preview_rejects_source_owned_by_other_creator(tmp_path, monkeypatch):
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

    creator_a = db.create_creator("Creator A")
    creator_b = db.create_creator("Creator B")
    db.upsert_post(
        creator_id=creator_a,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Existing",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )

    payload = {
        "post": {
            "title": "Imported",
            "content": "",
            "user": "70479526",
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)

    response = app.test_client().post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/100",
            "creator_id": str(creator_b),
            "series_id": "",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"This source already exists under creator" in response.data
    assert b"Creator A" in response.data


def test_import_preview_client_redirects_with_history_replace(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )
    response = app.test_client().get("/static/import_preview.js")
    assert response.status_code == 200
    assert b"window.location.replace(payload.redirect_url);" in response.data
    assert b"window.location.assign(payload.redirect_url);" not in response.data


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


def test_import_commit_rejects_source_owned_by_other_creator(tmp_path):
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

    creator_a = db.create_creator("Creator A")
    creator_b = db.create_creator("Creator B")
    post_id = db.upsert_post(
        creator_id=creator_a,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Existing title",
        content="Old",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )

    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": str(creator_b),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"This source already exists under creator" in response.data
    assert b"Creator A" in response.data
    assert len(db.list_posts_for_creator(creator_b)) == 0
    original = db.get_post(post_id)
    assert original is not None
    assert int(original["creator_id"]) == creator_a
    assert original["title"] == "Existing title"


def test_import_commit_rolls_back_new_post_when_download_fails(tmp_path, monkeypatch):
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
            "title": "Broken Import",
            "content": "",
            "user": "70479526",
            "file": {"name": "boom.jpg", "path": "/aa/bb/boom.jpg"},
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

    def failing_replace_attachments(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("replace attachments failed")

    monkeypatch.setattr(db, "replace_attachments", failing_replace_attachments)

    creator_id = db.create_creator("Creator A")
    with pytest.raises(RuntimeError, match="replace attachments failed"):
        _import_post_into_library(
            db,
            files_base=Path(app.config["FILES_DIR"]),
            icons_base=Path(app.config["ICONS_DIR"]),
            creator_id=creator_id,
            series_id=None,
            service="fanbox",
            user_id="70479526",
            post_id="100",
            import_target_mode="new",
            target_post_id=None,
            overwrite_matching_version=False,
            set_as_default=True,
            version_label=None,
            version_language=None,
            requested_title=None,
            requested_content=None,
            requested_published_at=None,
            requested_edited_at=None,
            requested_next_external_post_id=None,
            requested_prev_external_post_id=None,
            tags_text=None,
            field_presence={
                "published_at": False,
                "edited_at": False,
                "next_external_post_id": False,
                "prev_external_post_id": False,
            },
            selected_attachment_indices={"0"},
        )

    creator = db.get_creator(creator_id)
    assert creator is not None
    assert creator["service"] is None
    assert creator["external_user_id"] is None
    assert creator["icon_remote_url"] is None
    assert creator["icon_local_path"] is None
    assert db.list_posts_for_creator(creator_id) == []
    assert not (Path(app.config["FILES_DIR"]) / "post_1").exists()
    assert not (Path(app.config["ICONS_DIR"]) / "fanbox_70479526.jpg").exists()


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


def test_import_start_rejects_series_owned_by_other_creator(tmp_path):
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

    creator_a = db.create_creator("Creator A")
    creator_b = db.create_creator("Creator B")
    foreign_series_id = db.create_series(creator_b, "Foreign Series")

    response = app.test_client().post(
        "/import/start",
        data={
            "creator_id": str(creator_a),
            "series_id": str(foreign_series_id),
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
        },
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload["error"] == "Selected series was not found for this creator."


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


def test_post_detail_navigator_defaults_to_series_and_supports_all_scope(tmp_path):
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

    creator_id = db.create_creator("Navigator Creator")
    series_a_id = db.create_series(creator_id, "Series A")
    series_b_id = db.create_series(creator_id, "Series B")

    series_a_first = db.upsert_post(
        creator_id=creator_id,
        series_id=series_a_id,
        service="fanbox",
        external_user_id="u-nav",
        external_post_id="1001",
        title="A One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav/post/1001",
        published_at="2025-01-03T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_a_id,
        service="fanbox",
        external_user_id="u-nav",
        external_post_id="1002",
        title="A Two",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav/post/1002",
        published_at="2025-01-02T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_b_id,
        service="fanbox",
        external_user_id="u-nav",
        external_post_id="1003",
        title="B One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav/post/1003",
        published_at="2025-01-01T00:00:00",
    )
    unsorted_first = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="u-nav",
        external_post_id="1004",
        title="Unsorted One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav/post/1004",
        published_at="2025-01-04T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="u-nav",
        external_post_id="1005",
        title="Unsorted Two",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav/post/1005",
        published_at="2025-01-05T00:00:00",
    )

    client = app.test_client()

    series_default = client.get(f"/posts/{series_a_first}")
    assert series_default.status_code == 200
    series_soup = BeautifulSoup(series_default.data, "html.parser")
    series_nav = series_soup.select_one(".post-nav-list")
    assert series_nav is not None
    series_nav_text = series_nav.get_text(" ", strip=True)
    assert "A One" in series_nav_text
    assert "A Two" in series_nav_text
    assert "B One" not in series_nav_text
    assert "Unsorted One" not in series_nav_text
    assert any("nav_scope=all" in (link.get("href") or "") for link in series_soup.select(".post-nav-scope-toggle a"))
    assert series_soup.select_one(f'.post-nav-link.is-active[href="/posts/{series_a_first}?nav_scope=series"]') is not None

    all_scope = client.get(f"/posts/{series_a_first}?nav_scope=all")
    assert all_scope.status_code == 200
    all_soup = BeautifulSoup(all_scope.data, "html.parser")
    all_nav = all_soup.select_one(".post-nav-list")
    assert all_nav is not None
    all_nav_text = all_nav.get_text(" ", strip=True)
    assert "A One" in all_nav_text
    assert "A Two" in all_nav_text
    assert "B One" in all_nav_text
    assert "Unsorted One" in all_nav_text

    unsorted_default = client.get(f"/posts/{unsorted_first}")
    assert unsorted_default.status_code == 200
    unsorted_soup = BeautifulSoup(unsorted_default.data, "html.parser")
    unsorted_head = unsorted_soup.select_one(".post-nav-head small")
    assert unsorted_head is not None
    assert unsorted_head.get_text(strip=True) == "Unsorted"
    unsorted_nav = unsorted_soup.select_one(".post-nav-list")
    assert unsorted_nav is not None
    unsorted_nav_text = unsorted_nav.get_text(" ", strip=True)
    assert "Unsorted One" in unsorted_nav_text
    assert "Unsorted Two" in unsorted_nav_text
    assert "A One" not in unsorted_nav_text
    assert "B One" not in unsorted_nav_text
    assert unsorted_soup.select_one(f'.post-nav-link.is-active[href="/posts/{unsorted_first}?nav_scope=series"]') is not None


def test_post_detail_navigator_endpoint_returns_scope_payload_without_reloading_context(tmp_path):
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

    creator_id = db.create_creator("Navigator API Creator")
    series_a_id = db.create_series(creator_id, "Series A")
    series_b_id = db.create_series(creator_id, "Series B")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=series_a_id,
        service="fanbox",
        external_user_id="u-nav-api",
        external_post_id="2001",
        title="A One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav-api/post/2001",
        published_at="2025-02-03T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_a_id,
        service="fanbox",
        external_user_id="u-nav-api",
        external_post_id="2002",
        title="A Two",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav-api/post/2002",
        published_at="2025-02-02T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_b_id,
        service="fanbox",
        external_user_id="u-nav-api",
        external_post_id="2003",
        title="B One",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-nav-api/post/2003",
        published_at="2025-02-01T00:00:00",
    )

    for mode in ("reader", "gallery"):
        response = app.test_client().get(f"/posts/{post_id}/navigator?nav_scope=all&view={mode}")
        assert response.status_code == 200
        payload = response.get_json()
        assert isinstance(payload, dict)
        assert payload["scope"] == "all"
        assert f"view={mode}" in payload["series_scope_url"]
        assert f"view={mode}" in payload["all_scope_url"]
        assert "nav_scope=all" in payload["all_scope_url"]
        entries = payload["entries"]
        assert isinstance(entries, list)
        assert len(entries) == 3
        hrefs = [str(entry["href"]) for entry in entries]
        assert all("nav_scope=all" in href for href in hrefs)
        assert all(f"view={mode}" in href for href in hrefs)
        assert any(bool(entry["is_current"]) for entry in entries)
        assert all(isinstance(entry["published_at_display"], str) for entry in entries)


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


def test_post_detail_rewrites_inline_only_alias_to_local_and_hides_duplicate_saved_file(tmp_path):
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
    creator_id = db.create_creator("Inline Alias Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="1001",
        title="Inline Alias Case",
        content=(
            '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
            'QRcFc9aLl1C80MnOd4yOFKIF.jpeg" rel="noopener noreferrer"></a></p>'
        ),
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/1001",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "1.jpg",
                "remote_url": "https://kemono.cr/79/14/79144a60c6a0cc563340b925d02d14b890b8e0460754d314b8b233d04fcb3e2f.jpg",
                "local_path": f"post_{post_id}/1.jpg",
                "kind": "attachment",
            },
            {
                "name": "1.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/10791194/QRcFc9aLl1C80MnOd4yOFKIF.jpeg",
                "local_path": None,
                "kind": "inline_only",
            },
        ],
    )

    files_root = Path(app.config["FILES_DIR"]) / f"post_{post_id}"
    files_root.mkdir(parents=True, exist_ok=True)
    (files_root / "1.jpg").write_bytes(b"img")

    response = app.test_client().get(f"/posts/{post_id}")
    assert response.status_code == 200

    local_href = f"/files/post_{post_id}/1.jpg".encode()
    assert b'href="' + local_href + b'"' in response.data
    assert b'src="' + local_href + b'"' in response.data
    assert b"QRcFc9aLl1C80MnOd4yOFKIF.jpeg" not in response.data

    soup = BeautifulSoup(response.data, "html.parser")
    same_links = soup.select('.post-file-list a[title="1.jpg"]')
    assert len(same_links) == 1
    assert same_links[0].get("href") == f"/files/post_{post_id}/1.jpg"


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


def test_embed_link_attachment_is_reference_only_not_missing_or_retryable(tmp_path):
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
    creator_id = db.create_creator("Embed Link Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="patreon",
        external_user_id="3085566",
        external_post_id="111110523",
        title="Embed Link Post",
        content="<p>embed link</p>",
        metadata={},
        source_url="https://kemono.cr/patreon/user/3085566/post/111110523",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "Foxy Fairy-Tale",
                "remote_url": "https://inkyleafpatreononly.blogspot.com/2024/08/foxy-fairy-tale.html",
                "local_path": None,
                "kind": "embed_link",
            }
        ],
    )

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    assert b"Foxy Fairy-Tale" in detail.data
    assert b"retry" not in detail.data.lower()

    inventory = app.test_client().get("/attachments")
    assert inventory.status_code == 200
    html = inventory.get_data(as_text=True)
    assert "Foxy Fairy-Tale" in html
    assert "0 missing" in html


def test_import_preview_renders_embed_card_with_allowlisted_iframe(tmp_path, monkeypatch):
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
            "title": "Video Embed Post",
            "content": "<p>Has rich embed</p>",
            "user": "70479526",
            "embed": {
                "subject": "Preview Trailer",
                "description": "Watch this trailer.",
                "url": "https://www.youtube.com/watch?v=abc123",
                "thumbnail_url": "https://img.youtube.com/vi/abc123/hqdefault.jpg",
                "html": '<iframe src="https://www.youtube.com/embed/abc123" width="560" height="315"></iframe>',
            },
            "attachments": [],
        },
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)

    client = app.test_client()
    creator_response = client.post("/creators", data={"name": "Embed Preview Creator"}, follow_redirects=False)
    assert creator_response.status_code == 302

    preview = client.post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/777",
            "creator_id": "1",
            "series_id": "",
        },
    )
    assert preview.status_code == 200
    soup = BeautifulSoup(preview.data, "html.parser")
    embed_section = soup.select_one(".import-preview-embeds")
    assert embed_section is not None
    assert "Preview Trailer" in embed_section.get_text(" ", strip=True)
    iframe = embed_section.select_one("iframe")
    assert iframe is not None
    assert iframe.get("src") == "https://www.youtube.com/embed/abc123"


def test_post_detail_embed_card_blocks_non_allowlisted_iframe(tmp_path):
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
    creator_id = db.create_creator("Embed Safety Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="3001",
        title="Blocked iframe example",
        content="<p>Testing embeds</p>",
        metadata={
            "post": {
                "embed": {
                    "subject": "Unknown player",
                    "description": "Should not render iframe inline.",
                    "url": "https://evil.example/watch/123",
                    "html": '<iframe src="https://evil.example/embed/123" width="640" height="360"></iframe>',
                }
            }
        },
        source_url="https://kemono.cr/fanbox/user/70479526/post/3001",
    )

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    html = detail.get_data(as_text=True)
    assert "Unknown player" in html
    assert "Open embed source" in html
    assert "evil.example" in html
    assert "<iframe" not in html


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


def test_post_detail_header_mode_switcher_renders_only_on_post_pages(tmp_path):
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
    creator_id = db.create_creator("Mode Header Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="mode-h",
        external_post_id="5001",
        title="Mode Header Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/mode-h/post/5001",
    )

    home = app.test_client().get("/")
    assert home.status_code == 200
    assert b"data-post-view-mode-switcher" not in home.data

    detail = app.test_client().get(f"/posts/{post_id}")
    assert detail.status_code == 200
    assert b"data-post-view-mode-switcher" in detail.data
    assert b"data-post-view-mode-option" in detail.data


def test_post_detail_renders_content_view_settings_controls(tmp_path):
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
    creator_id = db.create_creator("Reader Settings Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="reader-settings",
        external_post_id="5009",
        title="Reader Settings Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/reader-settings/post/5009",
    )

    classic = app.test_client().get(f"/posts/{post_id}")
    assert classic.status_code == 200
    classic_soup = BeautifulSoup(classic.data, "html.parser")
    assert classic_soup.select_one("[data-post-content-settings]") is not None
    assert classic_soup.select_one("[data-post-view-mode-switcher]") is not None
    assert classic_soup.select_one("[data-theme-toggle-radio][value='auto']") is not None
    assert classic_soup.select_one("[data-theme-toggle-radio][value='light']") is not None
    assert classic_soup.select_one("[data-theme-toggle-radio][value='dark']") is not None
    assert classic_soup.select_one("[data-post-content-font-size]") is not None
    assert classic_soup.select_one("[data-post-content-line-height]") is not None
    assert classic_soup.select_one("[data-post-content-font-family]") is not None
    assert classic_soup.select_one("[data-post-content-text-align]") is not None
    assert classic_soup.select_one("[data-post-content-settings-reset]") is not None

    response = app.test_client().get(f"/posts/{post_id}?view=reader")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    shell = soup.select_one(".post-view-shell")
    assert shell is not None
    assert shell.get("data-post-creator-id") == str(creator_id)
    assert soup.select_one("[data-post-content-settings]") is not None
    assert soup.select_one("[data-post-content-font-size]") is not None
    assert soup.select_one("[data-post-content-line-height]") is not None
    assert soup.select_one("[data-post-content-font-family]") is not None
    assert soup.select_one("[data-post-content-text-align]") is not None
    assert soup.select_one("[data-post-content-settings-reset]") is not None

    gallery = app.test_client().get(f"/posts/{post_id}?view=gallery")
    assert gallery.status_code == 200
    gallery_soup = BeautifulSoup(gallery.data, "html.parser")
    assert gallery_soup.select_one("[data-post-content-settings]") is not None
    assert gallery_soup.select_one("[data-theme-toggle-radio][value='auto']") is not None
    assert gallery_soup.select_one("[data-theme-toggle-radio][value='light']") is not None
    assert gallery_soup.select_one("[data-theme-toggle-radio][value='dark']") is not None
    assert gallery_soup.select_one("[data-post-content-font-size]") is None
    assert gallery_soup.select_one("[data-post-content-line-height]") is None
    assert gallery_soup.select_one("[data-post-content-font-family]") is None
    assert gallery_soup.select_one("[data-post-content-text-align]") is None
    assert gallery_soup.select_one("[data-post-content-settings-reset]") is None


def test_post_content_settings_css_overrides_global_typography_rules():
    css_path = Path(__file__).resolve().parents[1] / "kemono_library" / "static" / "style.css"
    css = css_path.read_text(encoding="utf-8")
    assert ".post-content :is(p, li, blockquote, table, thead, tbody, tr, td, th)" in css
    assert ".post-content :is(h1, h2, h3, h4, h5, h6)" in css
    assert ".post-content h1 {" in css
    assert ".post-content h6 {" in css


def test_post_detail_reader_mode_propagates_view_and_renders_left_viewer_layout(tmp_path):
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
    creator_id = db.create_creator("Reader Layout Creator")
    series_id = db.create_series(creator_id, "Reader Series")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="reader-user",
        external_post_id="5002",
        title="Reader Post",
        content='<p><img src="https://n1.kemono.cr/aa/bb/inline.jpg" alt="inline"></p>',
        metadata={},
        source_url="https://kemono.cr/fanbox/user/reader-user/post/5002",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "page-01.jpg",
                "remote_url": "https://n1.kemono.cr/aa/bb/page-01.jpg",
                "local_path": None,
                "kind": "attachment",
            },
            {
                "name": "notes.txt",
                "remote_url": "https://n1.kemono.cr/aa/bb/notes.txt",
                "local_path": None,
                "kind": "attachment",
            },
        ],
    )

    response = app.test_client().get(f"/posts/{post_id}?view=reader")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    shell = soup.select_one(".post-view-shell.is-reader")
    assert shell is not None
    assert shell.get("data-post-view-mode") == "reader"
    assert shell.get("data-post-navigator-url") == f"/posts/{post_id}/navigator"
    assert soup.select_one("[data-post-reader-panel]") is not None
    assert soup.select_one(".header-actions [data-post-reader-nav-open]") is not None
    assert soup.select_one(".post-viewer-head-actions [data-post-reader-nav-open]") is None
    assert soup.select_one("[data-post-reader-nav-sheet]") is not None
    assert soup.select_one(".post-view-sidebar") is None
    assert soup.select_one("[data-post-file-launcher]") is not None
    assert soup.select_one("[data-post-reader-source-image]") is not None
    assert soup.select_one("[data-post-view-mode-switcher]") is not None
    assert soup.select_one(".post-viewer-head-actions a[href*='/edit']") is None
    main = soup.select_one("main.container")
    assert main is not None
    assert "is-post-reader-layout" in (main.get("class") or [])
    body = soup.select_one("body")
    assert body is not None
    assert "is-post-reader-page" in (body.get("class") or [])
    assert any("view=reader" in (link.get("href") or "") for link in soup.select(".post-nav-scope-toggle a"))

    direct_children = [child for child in shell.children if getattr(child, "name", None)]
    assert direct_children
    first = direct_children[0]
    assert getattr(first, "attrs", {}).get("data-post-reader-panel") == ""


def test_post_detail_gallery_mode_renders_post_header_then_viewer_with_image_launcher(tmp_path):
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
    creator_id = db.create_creator("Gallery Layout Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="gallery-user",
        external_post_id="5005",
        title="Gallery Post",
        content='<p><img src="https://n1.kemono.cr/aa/bb/gallery-inline.jpg" alt="inline"></p>',
        metadata={},
        source_url="https://kemono.cr/fanbox/user/gallery-user/post/5005",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "gallery-page-01.jpg",
                "remote_url": "https://n1.kemono.cr/aa/bb/gallery-page-01.jpg",
                "local_path": None,
                "kind": "attachment",
            },
            {
                "name": "gallery-notes.txt",
                "remote_url": "https://n1.kemono.cr/aa/bb/gallery-notes.txt",
                "local_path": None,
                "kind": "attachment",
            },
        ],
    )

    response = app.test_client().get(f"/posts/{post_id}?view=gallery")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    shell = soup.select_one(".post-view-shell.is-gallery")
    assert shell is not None
    assert "is-reader" not in (shell.get("class") or [])
    assert shell.get("data-post-view-mode") == "gallery"
    assert soup.select_one("[data-post-reader-panel]") is not None
    assert soup.select_one("[data-post-file-launcher]") is None
    assert soup.select_one("[data-post-reader-source-image]") is None
    assert soup.select_one(".post-file-list") is not None
    assert soup.select_one(".post-file-image-trigger") is not None
    assert soup.select_one(".post-content") is None
    assert soup.select_one(".post-content-source[data-post-content]") is not None
    assert soup.select_one(".post-embed-list-shell") is None
    assert soup.select_one(".post-view-sidebar") is None
    assert soup.select_one(".header-actions [data-post-reader-nav-open]") is None
    assert soup.select_one("[data-post-reader-nav-sheet]") is None
    main = soup.select_one("main.container")
    assert main is not None
    assert "is-post-gallery-layout" in (main.get("class") or [])
    body = soup.select_one("body")
    assert body is not None
    assert "is-post-gallery-page" in (body.get("class") or [])

    gallery_main = soup.select_one(".post-view-main.is-gallery")
    assert gallery_main is not None
    gallery_blocks = [child for child in gallery_main.children if getattr(child, "name", None)]
    assert gallery_blocks
    assert "post-viewer-info" in (gallery_blocks[0].get("class") or [])

    shell_blocks = [child for child in shell.children if getattr(child, "name", None)]
    assert shell_blocks
    assert "post-view-main" in (shell_blocks[0].get("class") or [])
    assert shell_blocks[1].get("data-post-reader-panel") == ""


def test_post_detail_invalid_view_falls_back_to_classic(tmp_path):
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
    creator_id = db.create_creator("View Fallback Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="mode-fallback",
        external_post_id="5003",
        title="Fallback Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/mode-fallback/post/5003",
    )

    response = app.test_client().get(f"/posts/{post_id}?view=invalid")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    shell = soup.select_one(".post-view-shell")
    assert shell is not None
    raw_classes = shell.get("class")
    if isinstance(raw_classes, list):
        shell_classes = [str(value) for value in raw_classes]
    elif isinstance(raw_classes, str):
        shell_classes = [raw_classes]
    else:
        shell_classes = []
    assert "is-reader" not in shell_classes
    assert shell.get("data-post-view-mode") == "classic"
    assert soup.select_one("[data-post-reader-panel]") is None
    assert soup.select_one(".post-viewer-head-actions a[href*='/edit']") is not None


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


def test_retry_attachment_redirect_preserves_reader_view(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Retry View Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-view",
        external_post_id="5004",
        title="Retry View Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-view/post/5004",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "missing.png",
                "remote_url": "https://n1.kemono.cr/path/missing.png",
                "local_path": None,
                "kind": "attachment",
            }
        ],
    )

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])
    attachment_id = int(db.list_attachments(post_id, version_id=version_id)[0]["id"])

    response = app.test_client().post(
        f"/posts/{post_id}/attachments/{attachment_id}/retry?version_id={version_id}&view=reader",
        follow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["Location"]
    assert location.startswith(f"/posts/{post_id}?")
    assert "view=reader" in location


def test_retry_attachment_redirect_preserves_gallery_view(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Retry Gallery View Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-gallery-view",
        external_post_id="5006",
        title="Retry Gallery View Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-gallery-view/post/5006",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "missing-gallery.png",
                "remote_url": "https://n1.kemono.cr/path/missing-gallery.png",
                "local_path": None,
                "kind": "attachment",
            }
        ],
    )

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])
    attachment_id = int(db.list_attachments(post_id, version_id=version_id)[0]["id"])

    response = app.test_client().post(
        f"/posts/{post_id}/attachments/{attachment_id}/retry?version_id={version_id}&view=gallery",
        follow_redirects=False,
    )
    assert response.status_code == 302
    location = response.headers["Location"]
    assert location.startswith(f"/posts/{post_id}?")
    assert "view=gallery" in location


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


def test_homepage_links_to_attachment_manager(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    response = app.test_client().get("/")
    assert response.status_code == 200
    assert b'href="/attachments"' in response.data
    assert b"Attachments" in response.data
    assert _page_title(response) == _expected_page_title("Local Kemono Archive")


def test_post_links_expose_creator_for_preferred_view_injection(tmp_path):
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
    creator_id = db.create_creator("Link Pref Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="pref-user",
        external_post_id="7711",
        title="Link Pref Post",
        content="<p>body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/pref-user/post/7711",
    )

    home = app.test_client().get("/")
    assert home.status_code == 200
    home_soup = BeautifulSoup(home.data, "html.parser")
    recent_link = home_soup.select_one("a.home-recent-item[data-post-detail-link]")
    assert recent_link is not None
    assert recent_link.get("href") == f"/posts/{post_id}"
    assert recent_link.get("data-post-creator-id") == str(creator_id)

    creator_page = app.test_client().get(f"/creators/{creator_id}")
    assert creator_page.status_code == 200
    creator_soup = BeautifulSoup(creator_page.data, "html.parser")
    creator_links = creator_soup.select("a[data-post-detail-link][data-post-creator-id]")
    assert creator_links
    assert any(link.get("data-post-creator-id") == str(creator_id) for link in creator_links)


def test_attachment_manager_lists_grouped_inventory_with_sizes(tmp_path):
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
    series_id = db.create_series(creator_id, "Collected Set")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="att-user",
        external_post_id="1005",
        title="Attachment Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/att-user/post/1005",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "saved.jpg",
                "remote_url": "https://n1.kemono.cr/path/saved.jpg",
                "local_path": f"post_{post_id}/saved.jpg",
                "kind": "attachment",
            },
            {
                "name": "missing.zip",
                "remote_url": "https://n1.kemono.cr/path/missing.zip",
                "local_path": f"post_{post_id}/missing.zip",
                "kind": "attachment",
            },
        ],
    )
    saved_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "saved.jpg"
    saved_file.parent.mkdir(parents=True, exist_ok=True)
    saved_file.write_bytes(b"abcde")

    response = app.test_client().get("/attachments")
    assert response.status_code == 200
    assert _page_title(response) == _expected_page_title("Attachment Inventory")
    soup = BeautifulSoup(response.data, "html.parser")
    text = soup.get_text(" ", strip=True)
    assert "Inventory" in text
    assert "Attachment Creator" in text
    assert "Collected Set" in text
    assert "Attachment Post" in text
    assert "saved.jpg" in text
    assert "missing.zip" in text
    assert "5 B" in text
    assert "1 missing" in text
    assert "Apply" not in text
    assert soup.select_one("[data-attachment-filter-form]") is not None
    assert soup.select_one("[data-attachment-filter-search]") is not None
    assert soup.select_one("[data-attachment-retry-overlay]") is not None
    assert soup.select_one("[data-attachment-retry-form]") is not None
    preview_image = soup.select_one("[data-attachment-preview-image]")
    assert preview_image is not None
    assert preview_image.get("data-preview-src")
    assert preview_image.get("src") is None
    assert soup.select_one("[data-attachment-retry-progress-failures]") is not None
    assert soup.select_one("[data-attachment-retry-result]") is not None
    assert soup.select_one("[data-attachment-retry-result-examples]") is not None
    assert soup.select_one("[data-attachment-retry-result-note]") is None
    assert soup.select_one("[data-attachment-retry-dock]") is not None
    assert soup.select_one("[data-attachment-retry-restore]") is not None
    assert soup.select_one("[data-attachment-retry-close]") is not None
    assert soup.select_one("[data-attachment-retry-refresh]") is None
    assert soup.select_one("[data-attachment-retry-dismiss]") is None
    ext_chips = [node.get_text(strip=True) for node in soup.select(".attachment-file-ext-badge")]
    assert ".jpg" in ext_chips
    assert ".zip" in ext_chips
    assert b"/static/attachment_manager.js" in response.data


def test_attachment_manager_suppresses_inline_alias_missing_when_local_exists(tmp_path):
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
    creator_id = db.create_creator("Inline Alias Inventory Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="alias-user",
        external_post_id="2001",
        title="Inline Alias Inventory Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/alias-user/post/2001",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "1.jpg",
                "remote_url": "https://kemono.cr/data/hash/1.jpg",
                "local_path": f"post_{post_id}/1.jpg",
                "kind": "attachment",
            },
            {
                "name": "1.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/2001/inline-random.jpeg",
                "local_path": None,
                "kind": "inline_only",
            },
        ],
    )
    local_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "1.jpg"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"img")

    response = app.test_client().get("/attachments")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    cards = soup.select("[data-attachment-card]")
    assert len(cards) == 1
    page_text = soup.get_text(" ", strip=True)
    assert "1.jpg" in page_text
    assert "1.jpeg" not in page_text
    assert "0 missing" in page_text
    assert "Retry All Missing" not in page_text
    assert "Retry Missing" not in page_text


def test_attachment_manager_deferred_tree_loads_skeleton_and_fragment(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
            "ATTACHMENT_MANAGER_DEFER_TREE": True,
        }
    )
    db = app.db  # type: ignore[attr-defined]
    first_creator_id = db.create_creator("Z Creator First")
    first_post_id = db.upsert_post(
        creator_id=first_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="defer-user",
        external_post_id="9010",
        title="Deferred Post A",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/defer-user/post/9010",
    )
    db.replace_attachments(
        first_post_id,
        [
            {
                "name": "deferred-a.jpg",
                "remote_url": "https://n1.kemono.cr/path/deferred.jpg",
                "local_path": f"post_{first_post_id}/deferred-a.jpg",
                "kind": "attachment",
            }
        ],
    )
    second_creator_id = db.create_creator("A Creator Second")
    second_post_id = db.upsert_post(
        creator_id=second_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="defer-user-2",
        external_post_id="9011",
        title="Deferred Post B",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/defer-user-2/post/9011",
    )
    db.replace_attachments(
        second_post_id,
        [
            {
                "name": "deferred-b.jpg",
                "remote_url": "https://n1.kemono.cr/path/deferred-b.jpg",
                "local_path": f"post_{second_post_id}/deferred-b.jpg",
                "kind": "attachment",
            }
        ],
    )

    client = app.test_client()
    page = client.get("/attachments")
    assert page.status_code == 200
    page_soup = BeautifulSoup(page.data, "html.parser")
    assert page_soup.select_one("[data-attachment-tree-deferred]") is not None
    assert "Loading full attachment inventory..." not in page.get_data(as_text=True)
    assert "deferred-a.jpg" not in page.get_data(as_text=True)
    skeleton_names = [node.get_text(strip=True) for node in page_soup.select("[data-attachment-skeleton] .attachment-tree-summary strong")]
    assert skeleton_names == ["Z Creator First", "A Creator Second"]

    tree = client.get("/attachments/tree")
    assert tree.status_code == 200
    tree_html = tree.get_data(as_text=True)
    assert "deferred-a.jpg" in tree_html
    assert "deferred-b.jpg" in tree_html
    assert "data-attachment-card" in tree_html
    tree_soup = BeautifulSoup(tree_html, "html.parser")
    hydrated_names = [node.get_text(strip=True) for node in tree_soup.select(".attachment-tree-creator > .attachment-tree-summary strong")]
    assert hydrated_names == ["Z Creator First", "A Creator Second"]


def test_attachment_manager_deferred_summary_suppresses_inline_alias_missing(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
            "ATTACHMENT_MANAGER_DEFER_TREE": True,
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Deferred Alias Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="defer-alias-user",
        external_post_id="9012",
        title="Deferred Alias Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/defer-alias-user/post/9012",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "1.jpg",
                "remote_url": "https://kemono.cr/data/hash/1.jpg",
                "local_path": f"post_{post_id}/1.jpg",
                "kind": "attachment",
            },
            {
                "name": "1.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/9012/inline-random.jpeg",
                "local_path": None,
                "kind": "inline_only",
            },
        ],
    )
    local_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "1.jpg"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"img")

    client = app.test_client()
    page = client.get("/attachments")
    assert page.status_code == 200
    soup = BeautifulSoup(page.data, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    assert "1 missing" not in page_text
    assert "0 missing" in page_text
    assert "Retry All Missing" not in page_text
    size_summary = soup.select_one("[data-attachment-summary-size]")
    assert size_summary is not None
    assert size_summary.get("data-size-bytes") == "3"
    skeleton = soup.select_one("[data-attachment-skeleton] .attachment-tree-summary small")
    assert skeleton is not None
    assert "1 files, 0 missing" in skeleton.get_text(" ", strip=True)

    tree = client.get("/attachments/tree")
    assert tree.status_code == 200
    tree_soup = BeautifulSoup(tree.data, "html.parser")
    cards = tree_soup.select("[data-attachment-card]")
    assert len(cards) == 1
    assert "1.jpeg" not in tree.get_data(as_text=True)


def test_attachment_manager_only_auto_opens_when_one_creator(tmp_path):
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
    creator_id = db.create_creator("Solo Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="solo-user",
        external_post_id="1010",
        title="Solo Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/solo-user/post/1010",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "solo.jpg",
                "remote_url": "https://n1.kemono.cr/path/solo.jpg",
                "local_path": f"post_{post_id}/solo.jpg",
                "kind": "attachment",
            }
        ],
    )

    client = app.test_client()
    response = client.get("/attachments")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    creator_nodes = soup.select("details.attachment-tree-creator")
    assert len(creator_nodes) == 1
    assert creator_nodes[0].has_attr("open")

    second_creator_id = db.create_creator("Second Creator")
    second_post_id = db.upsert_post(
        creator_id=second_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="second-user",
        external_post_id="1011",
        title="Second Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/second-user/post/1011",
    )
    db.replace_attachments(
        second_post_id,
        [
            {
                "name": "second.jpg",
                "remote_url": "https://n1.kemono.cr/path/second.jpg",
                "local_path": f"post_{second_post_id}/second.jpg",
                "kind": "attachment",
            }
        ],
    )

    response = client.get("/attachments")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    creator_nodes = soup.select("details.attachment-tree-creator")
    assert len(creator_nodes) == 2
    assert all(not node.has_attr("open") for node in creator_nodes)


def test_attachment_manager_hides_retry_actions_when_no_files_are_missing(tmp_path):
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
    creator_id = db.create_creator("Healthy Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="healthy-user",
        external_post_id="1008",
        title="Healthy Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/healthy-user/post/1008",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "saved.jpg",
                "remote_url": "https://n1.kemono.cr/path/saved.jpg",
                "local_path": f"post_{post_id}/saved.jpg",
                "kind": "attachment",
            }
        ],
    )
    saved_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "saved.jpg"
    saved_file.parent.mkdir(parents=True, exist_ok=True)
    saved_file.write_bytes(b"ok")

    response = app.test_client().get("/attachments")
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "Retry All Missing" not in html
    assert "Retry Missing" not in html
    assert ">Retry<" not in html


def test_attachment_manager_size_sort_orders_tree_by_aggregate_sizes(tmp_path):
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

    small_creator_id = db.create_creator("Small Creator")
    small_post_id = db.upsert_post(
        creator_id=small_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="small-user",
        external_post_id="1012",
        title="Small Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/small-user/post/1012",
    )
    db.replace_attachments(
        small_post_id,
        [
            {
                "name": "small-a.bin",
                "remote_url": "https://n1.kemono.cr/path/small-a.bin",
                "local_path": f"post_{small_post_id}/small-a.bin",
                "kind": "attachment",
            },
            {
                "name": "small-b.bin",
                "remote_url": "https://n1.kemono.cr/path/small-b.bin",
                "local_path": f"post_{small_post_id}/small-b.bin",
                "kind": "attachment",
            },
        ],
    )
    small_dir = Path(app.config["FILES_DIR"]) / f"post_{small_post_id}"
    small_dir.mkdir(parents=True, exist_ok=True)
    (small_dir / "small-a.bin").write_bytes(b"a" * 5)
    (small_dir / "small-b.bin").write_bytes(b"b" * 4)

    large_creator_id = db.create_creator("Large Creator")
    large_post_id = db.upsert_post(
        creator_id=large_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="large-user",
        external_post_id="1013",
        title="Large Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/large-user/post/1013",
    )
    db.replace_attachments(
        large_post_id,
        [
            {
                "name": "large.bin",
                "remote_url": "https://n1.kemono.cr/path/large.bin",
                "local_path": f"post_{large_post_id}/large.bin",
                "kind": "attachment",
            }
        ],
    )
    large_dir = Path(app.config["FILES_DIR"]) / f"post_{large_post_id}"
    large_dir.mkdir(parents=True, exist_ok=True)
    (large_dir / "large.bin").write_bytes(b"c" * 20)

    response = app.test_client().get("/attachments?sort=size")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")

    creator_nodes = soup.select("details.attachment-tree-creator")
    assert len(creator_nodes) == 2
    assert "Large Creator" in creator_nodes[0].get_text(" ", strip=True)
    assert "Small Creator" in creator_nodes[1].get_text(" ", strip=True)

    first_post = creator_nodes[0].select_one("details.attachment-tree-post")
    assert first_post is not None
    assert "Large Post" in first_post.get_text(" ", strip=True)

    attachment_names = [
        node.get_text("", strip=True)
        for node in creator_nodes[1].select(".attachment-file-meta strong")
    ]
    assert attachment_names[:2] == ["small-a.bin", "small-b.bin"]


def test_attachment_manager_alphabetical_sort_orders_tree_layers(tmp_path):
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
    creator_id = db.create_creator("Creator Z")
    post_b_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="alpha-user",
        external_post_id="1014",
        title="Post B",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/alpha-user/post/1014",
    )
    post_a_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="alpha-user",
        external_post_id="1015",
        title="Post A",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/alpha-user/post/1015",
    )
    db.replace_attachments(
        post_b_id,
        [
            {
                "name": "zeta.bin",
                "remote_url": "https://n1.kemono.cr/path/zeta.bin",
                "local_path": f"post_{post_b_id}/zeta.bin",
                "kind": "attachment",
            }
        ],
    )
    db.replace_attachments(
        post_a_id,
        [
            {
                "name": "alpha.bin",
                "remote_url": "https://n1.kemono.cr/path/alpha.bin",
                "local_path": f"post_{post_a_id}/alpha.bin",
                "kind": "attachment",
            }
        ],
    )

    response = app.test_client().get("/attachments?sort=name")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    post_titles = [node.get_text(" ", strip=True) for node in soup.select("details.attachment-tree-post > summary strong a")]
    assert post_titles[:2] == ["Post A", "Post B"]


def test_attachment_manager_tree_order_preserves_natural_post_order(tmp_path):
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
    creator_id = db.create_creator("Creator Tree")
    later_creator_id = db.create_creator("A Creator Later")
    later_post_id = db.upsert_post(
        creator_id=later_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="tree-user-later",
        external_post_id="1020",
        title="Later Creator Post",
        content="",
        metadata={},
        published_at="2026-01-01T00:00:00",
        source_url="https://kemono.cr/fanbox/user/tree-user-later/post/1020",
    )
    db.replace_attachments(
        later_post_id,
        [
            {
                "name": "later.bin",
                "remote_url": "https://n1.kemono.cr/path/later.bin",
                "local_path": f"post_{later_post_id}/later.bin",
                "kind": "attachment",
            }
        ],
    )

    older_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="tree-user",
        external_post_id="1018",
        title="Post A",
        content="",
        metadata={},
        published_at="2024-01-01T00:00:00",
        source_url="https://kemono.cr/fanbox/user/tree-user/post/1018",
    )
    newer_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="tree-user",
        external_post_id="1019",
        title="Post B",
        content="",
        metadata={},
        published_at="2025-01-01T00:00:00",
        source_url="https://kemono.cr/fanbox/user/tree-user/post/1019",
    )
    db.replace_attachments(
        older_post_id,
        [
            {
                "name": "older.bin",
                "remote_url": "https://n1.kemono.cr/path/older.bin",
                "local_path": f"post_{older_post_id}/older.bin",
                "kind": "attachment",
            }
        ],
    )
    db.replace_attachments(
        newer_post_id,
        [
            {
                "name": "newer.bin",
                "remote_url": "https://n1.kemono.cr/path/newer.bin",
                "local_path": f"post_{newer_post_id}/newer.bin",
                "kind": "attachment",
            }
        ],
    )

    response = app.test_client().get("/attachments?sort=creator")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    creator_nodes = soup.select("details.attachment-tree-creator")
    assert len(creator_nodes) == 2
    assert "Creator Tree" in creator_nodes[0].get_text(" ", strip=True)
    assert "A Creator Later" in creator_nodes[1].get_text(" ", strip=True)
    post_titles = [node.get_text(" ", strip=True) for node in soup.select("details.attachment-tree-post > summary strong a")]
    assert post_titles[:3] == ["Post B", "Post A", "Later Creator Post"]


def test_attachment_manager_recent_sort_orders_tree_layers(tmp_path):
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
    older_creator_id = db.create_creator("Older Creator")
    older_post_id = db.upsert_post(
        creator_id=older_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="recent-user",
        external_post_id="1016",
        title="Older Post",
        content="",
        metadata={},
        published_at="2024-01-01T00:00:00",
        source_url="https://kemono.cr/fanbox/user/recent-user/post/1016",
    )
    db.replace_attachments(
        older_post_id,
        [
            {
                "name": "older.bin",
                "remote_url": "https://n1.kemono.cr/path/older.bin",
                "local_path": f"post_{older_post_id}/older.bin",
                "kind": "attachment",
            }
        ],
    )
    newer_creator_id = db.create_creator("Newer Creator")
    newer_post_id = db.upsert_post(
        creator_id=newer_creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="recent-user",
        external_post_id="1017",
        title="Newer Post",
        content="",
        metadata={},
        published_at="2025-01-01T00:00:00",
        source_url="https://kemono.cr/fanbox/user/recent-user/post/1017",
    )
    db.replace_attachments(
        newer_post_id,
        [
            {
                "name": "newer.bin",
                "remote_url": "https://n1.kemono.cr/path/newer.bin",
                "local_path": f"post_{newer_post_id}/newer.bin",
                "kind": "attachment",
            }
        ],
    )

    response = app.test_client().get("/attachments?sort=recent")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    creator_nodes = soup.select("details.attachment-tree-creator")
    assert len(creator_nodes) == 2
    assert "Newer Creator" in creator_nodes[0].get_text(" ", strip=True)
    assert "Older Creator" in creator_nodes[1].get_text(" ", strip=True)


def test_attachment_manager_batch_retry_uses_scope_and_recovers_missing_files(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Batch Retry Creator")
    first_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="batch-user",
        external_post_id="1006",
        title="First Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/batch-user/post/1006",
    )
    second_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="batch-user",
        external_post_id="1007",
        title="Second Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/batch-user/post/1007",
    )
    db.replace_attachments(
        first_post_id,
        [
            {
                "name": "first.jpg",
                "remote_url": "https://n1.kemono.cr/path/first.jpg",
                "local_path": f"post_{first_post_id}/first.jpg",
                "kind": "attachment",
            }
        ],
    )
    db.replace_attachments(
        second_post_id,
        [
            {
                "name": "second.jpg",
                "remote_url": "https://n1.kemono.cr/path/second.jpg",
                "local_path": f"post_{second_post_id}/second.jpg",
                "kind": "attachment",
            }
        ],
    )

    attempted_urls: list[str] = []

    def fake_download(remote_url, destination):  # noqa: ARG001
        attempted_urls.append(remote_url)
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    response = app.test_client().post(
        "/attachments/retry-missing",
        data={
            "scope": "post",
            "scope_id": str(first_post_id),
            "return_to": "/attachments",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/attachments")
    assert attempted_urls == ["https://n1.kemono.cr/path/first.jpg"]

    first_saved = Path(app.config["FILES_DIR"]) / f"post_{first_post_id}" / "first.jpg"
    second_saved = Path(app.config["FILES_DIR"]) / f"post_{second_post_id}" / "second.jpg"
    assert first_saved.exists()
    assert not second_saved.exists()


def test_attachment_retry_scope_skips_inline_alias_missing_when_local_exists(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Retry Alias Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-alias-user",
        external_post_id="1006",
        title="Retry Alias Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-alias-user/post/1006",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "1.jpg",
                "remote_url": "https://kemono.cr/data/hash/1.jpg",
                "local_path": f"post_{post_id}/1.jpg",
                "kind": "attachment",
            },
            {
                "name": "1.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/1006/inline-random.jpeg",
                "local_path": None,
                "kind": "inline_only",
            },
        ],
    )
    local_file = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "1.jpg"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"img")
    attempted_urls: list[str] = []

    def fake_download(remote_url, destination):  # noqa: ARG001
        attempted_urls.append(remote_url)

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    response = app.test_client().post(
        "/attachments/retry-missing",
        data={
            "scope": "post",
            "scope_id": str(post_id),
            "return_to": "/attachments",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert attempted_urls == []
    assert b"No missing attachments matched this retry scope." in response.data


def test_attachment_retry_job_reports_live_progress_until_complete(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Retry Progress Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-progress-user",
        external_post_id="1009",
        title="Retry Progress Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-progress-user/post/1009",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "9f86d081884c7d659a2feaa0c55ad015.jpg",
                "remote_url": "https://n1.kemono.cr/path/9f86d081884c7d659a2feaa0c55ad015.jpg",
                "local_path": f"post_{post_id}/9f86d081884c7d659a2feaa0c55ad015.jpg",
                "kind": "attachment",
            },
            {
                "name": "e4d909c290d0fb1ca068ffaddf22cbd0.jpg",
                "remote_url": "https://n1.kemono.cr/path/e4d909c290d0fb1ca068ffaddf22cbd0.jpg",
                "local_path": f"post_{post_id}/e4d909c290d0fb1ca068ffaddf22cbd0.jpg",
                "kind": "attachment",
            },
        ],
    )

    first_download_started = threading.Event()
    allow_downloads = threading.Event()

    def fake_download(remote_url, destination):  # noqa: ARG001
        first_download_started.set()
        if not allow_downloads.wait(timeout=2):
            raise AssertionError("timed out waiting to release retry job download")
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    client = app.test_client()
    start = client.post(
        "/attachments/retry-missing/start",
        data={
            "scope": "post",
            "scope_id": str(post_id),
            "return_to": "/attachments",
        },
    )
    assert start.status_code == 200
    start_payload = start.get_json()
    assert isinstance(start_payload, dict)
    status_url = start_payload["status_url"]

    assert first_download_started.wait(timeout=2)

    running_seen = False
    for _ in range(50):
        status_response = client.get(status_url)
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert isinstance(status_payload, dict)
        if status_payload["status"] == "running":
            running_seen = True
            assert status_payload["total"] == 2
            assert status_payload["completed"] in {0, 1}
            assert status_payload["failure_count"] == 0
            current_file = str(status_payload["current_file"])
            assert "Retry Progress Creator" in current_file
            assert "Retry Progress Post" in current_file
            assert "Original" in current_file
            assert current_file.endswith(
                (
                    "9f86d081884c7d659a2feaa0c55ad015.jpg",
                    "e4d909c290d0fb1ca068ffaddf22cbd0.jpg",
                )
            )
            break
        time.sleep(0.01)

    assert running_seen is True
    allow_downloads.set()

    final_status: dict[str, object] | None = None
    for _ in range(150):
        status_response = client.get(status_url)
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert isinstance(status_payload, dict)
        if status_payload["status"] == "completed":
            final_status = status_payload
            break
        if status_payload["status"] == "failed":
            raise AssertionError(status_payload["error"])
        time.sleep(0.01)

    assert final_status is not None
    assert final_status["completed"] == 2
    assert final_status["total"] == 2
    assert final_status["success_count"] == 2
    assert final_status["failure_count"] == 0
    assert final_status["failure_examples"] == []
    results = final_status["results"]
    assert isinstance(results, list)
    assert len(results) == 2
    assert all(isinstance(entry.get("name"), str) and entry["name"] for entry in results)
    assert all(isinstance(entry.get("display_name"), str) and entry["display_name"] for entry in results)
    assert final_status["redirect_url"] == "/attachments"


def test_attachment_retry_job_uses_three_worker_concurrency(tmp_path, monkeypatch):
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
    creator_id = db.create_creator("Retry Concurrency Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="retry-concurrency-user",
        external_post_id="1012",
        title="Retry Concurrency Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/retry-concurrency-user/post/1012",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": f"{idx}.jpg",
                "remote_url": f"https://n1.kemono.cr/path/{idx}.jpg",
                "local_path": f"post_{post_id}/{idx}.jpg",
                "kind": "attachment",
            }
            for idx in range(8)
        ],
    )

    active_downloads = 0
    max_active_downloads = 0
    guard = threading.Lock()

    def fake_download(remote_url, destination):  # noqa: ARG001
        nonlocal active_downloads, max_active_downloads
        with guard:
            active_downloads += 1
            max_active_downloads = max(max_active_downloads, active_downloads)
        try:
            time.sleep(0.03)
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            Path(destination).write_bytes(b"ok")
        finally:
            with guard:
                active_downloads -= 1

    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    client = app.test_client()
    start = client.post(
        "/attachments/retry-missing/start",
        data={
            "scope": "post",
            "scope_id": str(post_id),
            "return_to": "/attachments",
        },
    )
    assert start.status_code == 200
    payload = start.get_json()
    assert isinstance(payload, dict)
    status_url = payload["status_url"]

    final_status: dict[str, object] | None = None
    for _ in range(200):
        status_response = client.get(status_url)
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert isinstance(status_payload, dict)
        if status_payload["status"] == "completed":
            final_status = status_payload
            break
        if status_payload["status"] == "failed":
            raise AssertionError(status_payload["error"])
        time.sleep(0.01)

    assert final_status is not None
    assert final_status["success_count"] == 8
    assert final_status["failure_count"] == 0
    assert 2 <= max_active_downloads <= 3


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
    assert _page_title(response) == _expected_page_title("Edit Me", "Edit Post")
    assert b"<textarea" in response.data
    assert b'id="content"' in response.data
    assert b'name="content"' in response.data
    assert b'rows="18"' in response.data
    assert b"&lt;p&gt;" in response.data
    assert b"&lt;strong&gt;" in response.data
    assert b"Hello" in response.data
    assert b'data-nav-replace-redirect' in response.data
    assert b'data-nav-replace-href' in response.data
    assert b"/static/transient_navigation.js" in response.data


def test_post_edit_version_actions_use_replace_navigation_attributes(tmp_path):
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
    creator_id = db.create_creator("Replace Nav Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="replace-user",
        external_post_id="replace-post",
        title="Replace Me",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/replace-user/post/replace-post",
    )
    source_version = db.get_post_version(post_id)
    assert source_version is not None
    manual_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(source_version["id"]),
        label="Manual",
        language="en",
        set_default=False,
    )

    response = app.test_client().get(f"/posts/{post_id}/edit?version_id={manual_id}")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")

    active_version_select = soup.select_one("select#active-version")
    assert active_version_select is not None
    assert "requestSubmit" in str(active_version_select.get("onchange") or "")
    switch_form = active_version_select.find_parent("form")
    assert switch_form is not None
    assert switch_form.get("data-nav-replace-redirect") is not None

    import_link = soup.find("a", string="Import new version")
    assert import_link is not None
    assert import_link.get("data-nav-replace-href") is not None

    clone_form = soup.find("form", action=f"/posts/{post_id}/versions/clone")
    assert clone_form is not None
    assert clone_form.get("data-nav-replace-redirect") is not None

    set_default_form = soup.find("form", action=f"/posts/{post_id}/versions/{manual_id}/set-default")
    assert set_default_form is not None
    assert set_default_form.get("data-nav-replace-redirect") is not None

    delete_form = soup.find("form", action=f"/posts/{post_id}/versions/{manual_id}/delete")
    assert delete_form is not None
    assert delete_form.get("data-nav-replace-redirect") is not None


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


def test_edit_post_ajax_submit_returns_redirect_json(tmp_path):
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
    creator_id = db.create_creator("Ajax Edit Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="ajax-user",
        external_post_id="ajax-post",
        title="Ajax Title",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/ajax-user/post/ajax-post",
    )
    version_row = db.get_post_version(post_id)
    assert version_row is not None
    version_id = int(version_row["id"])

    response = app.test_client().post(
        f"/posts/{post_id}/edit",
        headers={"X-Requested-With": "XMLHttpRequest"},
        data={
            "version_id": str(version_id),
            "version_label": "Original",
            "version_language": "",
            "title": "Ajax Title Updated",
            "series_id": "",
            "thumbnail_attachment_id": "__keep__",
            "thumbnail_focus_x": "50",
            "thumbnail_focus_y": "50",
            "content": "<p>Body</p>",
        },
        follow_redirects=False,
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["redirect_url"].endswith(f"/posts/{post_id}")


def test_edit_post_thumbnail_selector_keeps_current_when_name_match_is_ambiguous(tmp_path):
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
    creator_id = db.create_creator("Ambiguous Thumb Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="thumb-user",
        external_post_id="thumb-post",
        title="Ambiguous Thumb Post",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/thumb-user/post/thumb-post",
        thumbnail_name="cover.jpg",
        thumbnail_remote_url=None,
        thumbnail_local_path=None,
    )
    version_row = db.get_post_version(post_id)
    assert version_row is not None
    version_id = int(version_row["id"])

    db.replace_attachments(
        post_id,
        [
            {
                "name": "cover.jpg",
                "remote_url": "https://n1.kemono.cr/a/one.jpg",
                "local_path": None,
                "kind": "attachment",
            },
            {
                "name": "cover.jpg",
                "remote_url": "https://n1.kemono.cr/b/two.jpg",
                "local_path": None,
                "kind": "attachment",
            },
        ],
        version_id=version_id,
    )

    response = app.test_client().get(f"/posts/{post_id}/edit?version_id={version_id}")
    assert response.status_code == 200

    soup = BeautifulSoup(response.data.decode("utf-8"), "html.parser")
    selector = soup.find("select", {"id": "thumbnail_attachment_id"})
    assert selector is not None
    selected_values = [option.get("value") for option in selector.find_all("option", selected=True)]
    assert selected_values == ["__keep__"]


def test_edit_post_rejects_series_owned_by_other_creator(tmp_path):
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

    creator_a = db.create_creator("Edit Creator")
    creator_b = db.create_creator("Other Creator")
    foreign_series_id = db.create_series(creator_b, "Foreign Series")
    post_id = db.upsert_post(
        creator_id=creator_a,
        series_id=None,
        service="fanbox",
        external_user_id="111",
        external_post_id="223",
        title="Edit Me",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/111/post/223",
    )
    version_row = db.get_post_version(post_id)
    assert version_row is not None

    response = app.test_client().post(
        f"/posts/{post_id}/edit",
        data={
            "version_id": str(int(version_row["id"])),
            "version_label": "Original",
            "version_language": "",
            "title": "Edit Me",
            "series_id": str(foreign_series_id),
            "thumbnail_attachment_id": "__keep__",
            "thumbnail_focus_x": "50",
            "thumbnail_focus_y": "50",
            "content": "<p>Body</p>",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Selected series was not found for this creator." in response.data
    post = db.get_post(post_id)
    assert post is not None
    assert post["series_id"] is None


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
    assert isinstance(updated_local["local_path"], str)
    assert str(updated_local["local_path"]).endswith("renamed_local.jpg")
    assert not local_abs.exists()
    renamed_abs = Path(tmp_path / "files" / str(updated_local["local_path"]))
    assert renamed_abs.exists()
    assert str(updated_missing["name"]) == "missing-two.txt"
    assert updated_missing["local_path"] is None
    assert str(updated_extra["name"]) == "extra-localized.png"
    assert updated_extra["local_path"] is None

    after_detail = client.get(f"/posts/{post_id}?version_id={manual_version_id}")
    assert after_detail.status_code == 200
    html_after = after_detail.data.decode("utf-8")
    assert f"/files/{local_rel}" not in html_after
    assert f"/files/{str(updated_local['local_path'])}" in html_after


def test_edit_post_rolls_back_db_changes_when_save_fails(tmp_path, monkeypatch):
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

    creator_id = db.create_creator("Rollback Editor")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="u-rb",
        external_post_id="p-rb",
        title="Original Title",
        content="<p>Body</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/u-rb/post/p-rb",
    )
    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])
    db.replace_attachments(
        post_id,
        [
            {
                "name": "missing.txt",
                "remote_url": "https://n1.kemono.cr/a/b/missing.txt",
                "local_path": None,
                "kind": "attachment",
            }
        ],
        version_id=version_id,
    )
    attachment_id = int(db.list_attachments(post_id, version_id=version_id)[0]["id"])

    def failing_update_post_version(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("update version failed")

    monkeypatch.setattr(db, "update_post_version", failing_update_post_version)

    response = app.test_client().post(
        f"/posts/{post_id}/edit",
        data={
            "version_id": str(version_id),
            "version_label": "Original",
            "version_language": "",
            "title": "Changed Title",
            "series_id": "",
            "thumbnail_attachment_id": "__keep__",
            "thumbnail_focus_x": "50",
            "thumbnail_focus_y": "50",
            "content": "<p>Changed</p>",
            f"attachment_keep_id_{attachment_id}": "1",
            f"attachment_name_id_{attachment_id}": "renamed.txt",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Failed to update post: update version failed" in response.data
    unchanged_version = db.get_post_version(post_id, version_id)
    assert unchanged_version is not None
    assert unchanged_version["title"] == "Original Title"
    attachments = db.list_attachments(post_id, version_id=version_id)
    assert len(attachments) == 1
    assert attachments[0]["name"] == "missing.txt"


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
    assert soup.select_one(".post-toolbar .creator-toolbar-main details.creator-sort-popover") is not None
    sort_toggle = soup.select_one(".post-toolbar .creator-sort-toggle")
    assert sort_toggle is not None
    assert sort_toggle.get("aria-label") == "Filter and sort options"
    assert sort_toggle.select_one("svg.creator-toolbar-icon") is not None
    folder_names = [node.get_text(strip=True) for node in soup.select(".folder-explorer-grid .folder-tile-name")]
    assert "Unsorted" not in folder_names
    filter_links = [anchor.get("href") or "" for anchor in soup.select(".creator-sort-bar a")]
    assert any("folder=unsorted" in href for href in filter_links)
    assert any("All posts" in anchor.get_text(strip=True) for anchor in soup.select(".creator-sort-bar a"))
    assert any("Unsorted" in anchor.get_text(strip=True) for anchor in soup.select(".creator-sort-bar a"))

    title_sorted = client.get(f"/creators/{creator_id}?sort=title&direction=asc")
    assert title_sorted.status_code == 200
    title_soup = BeautifulSoup(title_sorted.data, "html.parser")
    title_order = [node.get_text(strip=True) for node in title_soup.select(".creator-post-list .creator-post-body h3 a")]
    assert title_order == ["Alpha Post", "Beta Post", "Gamma Post"]

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
    published_desc_soup = BeautifulSoup(published_desc.data, "html.parser")
    published_desc_order = [
        node.get_text(strip=True)
        for node in published_desc_soup.select(".creator-post-list .creator-post-body h3 a")
    ]
    assert published_desc_order == ["Gamma Post", "Beta Post", "Alpha Post"]


def test_creator_post_search_filters_results_and_preserves_query(tmp_path):
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

    creator_id = db.create_creator("Search Creator")
    main_series_id = db.create_series(creator_id, "Main Series")
    side_series_id = db.create_series(creator_id, "Side Series")

    db.upsert_post(
        creator_id=creator_id,
        series_id=main_series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="200",
        title="Beta Patrol",
        content="General update",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/200",
        published_at="2025-01-02T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=side_series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="201",
        title="Delta Note",
        content="Needle content marker",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/201",
        published_at="2025-01-01T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="202",
        title="Gamma Post",
        content="General notes",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/202",
        published_at="2025-01-03T00:00:00",
    )

    client = app.test_client()
    searched = client.get(f"/creators/{creator_id}?q=needle")
    assert searched.status_code == 200
    searched_soup = BeautifulSoup(searched.data, "html.parser")
    searched_titles = [node.get_text(strip=True) for node in searched_soup.select(".creator-post-list .creator-post-body h3 a")]
    assert searched_titles == ["Delta Note"]
    search_form = searched_soup.select_one("form.creator-post-search")
    assert search_form is not None
    assert search_form.get("autocomplete") == "off"
    query_input = searched_soup.select_one(".creator-post-search input[name='q']")
    assert query_input is not None
    assert query_input.get("value") == "needle"
    assert query_input.get("list") == "creator-post-search-suggestions"
    assert query_input.get("autocomplete") == "off"
    assert query_input.get("autocorrect") == "off"
    assert query_input.get("autocapitalize") == "none"
    assert query_input.get("spellcheck") == "false"
    suggestion_values = [
        option.get("value") or ""
        for option in searched_soup.select("#creator-post-search-suggestions option")
    ]
    assert "Beta Patrol" in suggestion_values
    assert "Delta Note" in suggestion_values
    assert "Gamma Post" in suggestion_values
    sort_links = [anchor.get("href") or "" for anchor in searched_soup.select(".creator-sort-bar a")]
    assert sort_links
    assert all("q=needle" in href for href in sort_links)
    clear_link = searched_soup.select_one(".creator-post-search-clear")
    assert clear_link is not None
    clear_href = clear_link.get("href") or ""
    assert "q=" not in clear_href

    unsorted_only = client.get(f"/creators/{creator_id}?folder=unsorted&q=needle")
    assert unsorted_only.status_code == 200
    assert b"No posts match this search in the current view." in unsorted_only.data

    series_filtered = client.get(f"/creators/{creator_id}?series_id={main_series_id}&q=beta")
    assert series_filtered.status_code == 200
    series_html = series_filtered.data.decode("utf-8")
    assert "Beta Patrol" in series_html
    assert "Delta Note" not in series_html


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


def test_series_settings_apply_default_sort_and_cover_source(tmp_path):
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

    creator_id = db.create_creator("Series Settings Creator")
    series_id = db.create_series(creator_id, "Main Arc")

    cover_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="401",
        title="Zeta Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/401",
        thumbnail_local_path="post_401/zeta.jpg",
        published_at="2025-01-02T00:00:00",
    )
    db.upsert_post(
        creator_id=creator_id,
        series_id=series_id,
        service="fanbox",
        external_user_id="user-1",
        external_post_id="402",
        title="Alpha Post",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/user-1/post/402",
        thumbnail_local_path="post_402/alpha.jpg",
        published_at="2025-01-01T00:00:00",
    )

    client = app.test_client()
    update = client.post(
        f"/creators/{creator_id}/series/{series_id}",
        data={
            "name": "Main Arc",
            "description": "",
            "tags_text": "",
            "default_sort_by": "title",
            "default_sort_direction": "asc",
            "cover_post_id": str(cover_post_id),
        },
        follow_redirects=False,
    )
    assert update.status_code == 302

    series_row = next(row for row in db.list_series(creator_id) if int(row["id"]) == series_id)
    assert series_row["default_sort_by"] == "title"
    assert series_row["default_sort_direction"] == "asc"
    assert int(series_row["cover_post_id"]) == cover_post_id
    assert series_row["cover_thumbnail_local_path"] == "post_401/zeta.jpg"

    creator_view = client.get(f"/creators/{creator_id}")
    assert creator_view.status_code == 200
    assert b"/files/post_401/zeta.jpg" in creator_view.data

    folder_default = client.get(f"/creators/{creator_id}?series_id={series_id}")
    assert folder_default.status_code == 200
    folder_default_html = folder_default.data.decode("utf-8")
    assert folder_default_html.index("Alpha Post") < folder_default_html.index("Zeta Post")

    folder_override = client.get(f"/creators/{creator_id}?series_id={series_id}&sort=published&direction=desc")
    assert folder_override.status_code == 200
    folder_override_html = folder_override.data.decode("utf-8")
    assert folder_override_html.index("Zeta Post") < folder_override_html.index("Alpha Post")


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
    assert _page_title(root) == _expected_page_title("Context Creator")
    expected_root_import = f'href="/import?creator_id={creator_id}"'.encode()
    assert b'btn btn-link btn--ghost creator-import-action' in root.data
    assert expected_root_import in root.data
    assert b"Import here" not in root.data
    assert b'class="folder-explorer-grid"' in root.data

    folder = client.get(f"/creators/{creator_id}?series_id={series_id}")
    assert folder.status_code == 200
    assert _page_title(folder) == _expected_page_title("Folder A", "Context Creator")
    expected_import = f'href="/import?creator_id={creator_id}&amp;series_id={series_id}"'.encode()
    assert b'btn btn-link btn--ghost creator-import-action' in folder.data
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
    assert _page_title(edit_get) == _expected_page_title("Creator Before", "Edit Creator")
    assert b"Edit Creator" in edit_get.data
    assert b"Creator Before" in edit_get.data
    assert b'data-nav-replace-redirect' in edit_get.data
    assert b"/static/transient_navigation.js" in edit_get.data

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

    creator_page = client.get(f"/creators/{creator_id}")
    assert creator_page.status_code == 200
    assert b'data-nav-replace-href' in creator_page.data


def test_creator_edit_ajax_submit_returns_redirect_json(tmp_path):
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
    creator_id = db.create_creator("Creator Ajax Before")

    response = app.test_client().post(
        f"/creators/{creator_id}/edit",
        headers={"X-Requested-With": "XMLHttpRequest"},
        data={
            "name": "Creator Ajax After",
            "description": "Ajax description",
            "tags_text": "ajax-tag",
        },
        follow_redirects=False,
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["redirect_url"].endswith(f"/creators/{creator_id}")
    updated = db.get_creator(creator_id)
    assert updated is not None
    assert updated["name"] == "Creator Ajax After"


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


def test_import_commit_rejects_source_version_already_attached_to_other_post(tmp_path, monkeypatch):
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

    creator_id = db.create_creator("Duplicate Source Creator")
    primary_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="100",
        title="Original",
        content="jp",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/100",
    )
    secondary_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="101",
        title="Second",
        content="jp",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/101",
    )
    db.create_post_version(
        post_id=primary_post_id,
        label="EN TL",
        language="en",
        origin_kind=LibraryDB.VERSION_ORIGIN_SOURCE,
        source_service="fanbox",
        source_user_id="70479526",
        source_post_id="200",
        title="Translated",
        content="translated",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/70479526/post/200",
        set_default=False,
    )

    def fail_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        raise AssertionError("fetch should not run when the source tuple already exists locally")

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fail_fetch)

    response = app.test_client().post(
        "/import/commit",
        data={
            "creator_id": str(creator_id),
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "200",
            "import_target_mode": "existing",
            "target_post_id": str(secondary_post_id),
            "overwrite_matching_version": "0",
            "set_as_default": "0",
            "version_label": "EN TL",
            "version_language": "en",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"This source already exists under local post" in response.data
    assert len(db.list_post_versions(primary_post_id)) == 2
    assert len(db.list_post_versions(secondary_post_id)) == 1


def test_create_post_version_rejects_global_duplicate_source_tuple(tmp_path):
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

    creator_id = db.create_creator("Global Source Creator")
    first_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="700",
        external_post_id="100",
        title="First",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/700/post/100",
    )
    second_post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="700",
        external_post_id="101",
        title="Second",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/700/post/101",
    )
    db.create_post_version(
        post_id=first_post_id,
        label="EN",
        language="en",
        origin_kind=LibraryDB.VERSION_ORIGIN_SOURCE,
        source_service="fanbox",
        source_user_id="700",
        source_post_id="200",
        title="First TL",
        content="",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/700/post/200",
        set_default=False,
    )

    with pytest.raises(ValueError, match=r"source version already exists locally under post #\d+ as version #\d+"):
        db.create_post_version(
            post_id=second_post_id,
            label="Duplicate",
            language="en",
            origin_kind=LibraryDB.VERSION_ORIGIN_SOURCE,
            source_service="fanbox",
            source_user_id="700",
            source_post_id="200",
            title="Duplicate TL",
            content="",
            metadata={},
            source_url="https://kemono.cr/fanbox/user/700/post/200",
            set_default=False,
        )


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
    assert _page_title(detail) == _expected_page_title("English title", "Version Switch Creator")


def test_list_post_versions_keeps_creation_order_when_editing_non_default(tmp_path):
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
    creator_id = db.create_creator("Stable Version Order Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="322",
        external_post_id="654",
        title="JP title",
        content="jp body",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/322/post/654",
    )

    base_version = db.get_post_version(post_id)
    assert base_version is not None
    first_clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(base_version["id"]),
        label="EN",
        language="en",
        set_default=False,
    )
    second_clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(base_version["id"]),
        label="KR",
        language="ko",
        set_default=False,
    )

    initial_versions = db.list_post_versions(post_id)
    assert [int(row["id"]) for row in initial_versions] == [int(base_version["id"]), second_clone_id, first_clone_id]

    first_clone = db.get_post_version(post_id, first_clone_id)
    assert first_clone is not None
    db.update_post_version(
        version_id=first_clone_id,
        label="EN",
        language="en",
        title="Edited EN",
        content="edited body",
        thumbnail_name=first_clone["thumbnail_name"],
        thumbnail_remote_url=first_clone["thumbnail_remote_url"],
        thumbnail_local_path=first_clone["thumbnail_local_path"],
        published_at=first_clone["published_at"],
        edited_at=first_clone["edited_at"],
        next_external_post_id=first_clone["next_external_post_id"],
        prev_external_post_id=first_clone["prev_external_post_id"],
        metadata={},
        source_url=first_clone["source_url"],
    )

    versions_after_edit = db.list_post_versions(post_id)
    assert [int(row["id"]) for row in versions_after_edit] == [int(base_version["id"]), second_clone_id, first_clone_id]


def test_delete_default_post_version_falls_back_to_latest_created_version(tmp_path):
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
    creator_id = db.create_creator("Delete Fallback Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="323",
        external_post_id="654",
        title="JP title",
        content="jp body",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/323/post/654",
    )

    base_version = db.get_post_version(post_id)
    assert base_version is not None
    first_clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(base_version["id"]),
        label="EN",
        language="en",
        set_default=False,
    )
    second_clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(base_version["id"]),
        label="KR",
        language="ko",
        set_default=False,
    )

    db.update_post_version(
        version_id=int(base_version["id"]),
        label=str(base_version["label"]),
        language=base_version["language"],
        title="Edited base",
        content="edited base body",
        thumbnail_name=base_version["thumbnail_name"],
        thumbnail_remote_url=base_version["thumbnail_remote_url"],
        thumbnail_local_path=base_version["thumbnail_local_path"],
        published_at=base_version["published_at"],
        edited_at=base_version["edited_at"],
        next_external_post_id=base_version["next_external_post_id"],
        prev_external_post_id=base_version["prev_external_post_id"],
        metadata={},
        source_url=base_version["source_url"],
    )
    db.set_default_post_version(post_id, first_clone_id)

    assert db.delete_post_version(post_id, first_clone_id) is True

    post = db.get_post(post_id)
    assert post is not None
    assert int(post["default_version_id"]) == second_clone_id
    remaining_versions = db.list_post_versions(post_id)
    assert int(remaining_versions[0]["id"]) == second_clone_id


def test_clone_post_version_records_lineage_and_shows_parent_link(tmp_path):
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
    creator_id = db.create_creator("Lineage Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="600",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/600/post/100",
    )

    source_version = db.get_post_version(post_id)
    assert source_version is not None
    source_version_id = int(source_version["id"])

    clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=source_version_id,
        label="EN",
        language="en",
        set_default=False,
    )
    clone_row = db.get_post_version(post_id, clone_id)
    assert clone_row is not None
    assert int(clone_row["derived_from_version_id"]) == source_version_id
    assert clone_row["derived_from_label"] == source_version["label"]
    assert clone_row["derived_from_title"] == source_version["title"]

    detail = app.test_client().get(f"/posts/{post_id}?version_id={clone_id}")
    assert detail.status_code == 200
    assert b"Based on: Original" in detail.data
    assert f"version_id={source_version_id}".encode() in detail.data


def test_delete_post_version_clears_child_lineage(tmp_path):
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
    creator_id = db.create_creator("Lineage Delete Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="601",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/601/post/100",
    )

    source_version = db.get_post_version(post_id)
    assert source_version is not None
    source_version_id = int(source_version["id"])
    clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=source_version_id,
        label="EN",
        language="en",
        set_default=False,
    )

    assert db.delete_post_version(post_id, source_version_id) is True
    clone_row = db.get_post_version(post_id, clone_id)
    assert clone_row is not None
    assert clone_row["derived_from_version_id"] is None
    assert clone_row["derived_from_label"] is None


def test_post_projection_tracks_default_version_even_without_source_url(tmp_path):
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
    creator_id = db.create_creator("Projection Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="610",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/610/post/100",
    )

    source_version = db.get_post_version(post_id)
    assert source_version is not None
    clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(source_version["id"]),
        label="Local edit",
        language="en",
        set_default=True,
    )

    clone_row = db.get_post_version(post_id, clone_id)
    assert clone_row is not None
    db.update_post_version(
        version_id=clone_id,
        label="Local edit",
        language="en",
        title="Manual title",
        content="manual body",
        thumbnail_name=clone_row["thumbnail_name"],
        thumbnail_remote_url=clone_row["thumbnail_remote_url"],
        thumbnail_local_path=clone_row["thumbnail_local_path"],
        published_at=clone_row["published_at"],
        edited_at=clone_row["edited_at"],
        next_external_post_id=clone_row["next_external_post_id"],
        prev_external_post_id=clone_row["prev_external_post_id"],
        metadata={},
        source_url=None,
    )

    post = db.get_post(post_id)
    assert post is not None
    assert int(post["default_version_id"]) == clone_id
    assert post["title"] == "Manual title"
    assert post["content"] == "manual body"
    assert post["source_url"] == ""


def test_delete_last_post_version_clears_post_projection(tmp_path):
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
    creator_id = db.create_creator("Versionless Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="611",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={"files": [{"path": "/x"}]},
        source_url="https://kemono.cr/fanbox/user/611/post/100",
    )

    source_version = db.get_post_version(post_id)
    assert source_version is not None

    assert db.delete_post_version(post_id, int(source_version["id"])) is True

    post = db.get_post(post_id)
    assert post is not None
    assert post["default_version_id"] is None
    assert post["title"] == ""
    assert post["content"] is None
    assert post["metadata_json"] == "{}"
    assert post["source_url"] == ""


def test_version_origin_kind_distinguishes_source_and_clone(tmp_path):
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
    creator_id = db.create_creator("Origin Kind Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="612",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/612/post/100",
    )

    source_version = db.get_post_version(post_id)
    assert source_version is not None
    assert source_version["origin_kind"] == LibraryDB.VERSION_ORIGIN_SOURCE

    clone_id = db.clone_post_version(
        post_id=post_id,
        source_version_id=int(source_version["id"]),
        label="Clone",
        language="en",
        set_default=False,
    )
    clone_row = db.get_post_version(post_id, clone_id)
    assert clone_row is not None
    assert clone_row["origin_kind"] == LibraryDB.VERSION_ORIGIN_CLONE


def test_create_post_version_validates_origin_kind_shape(tmp_path):
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
    creator_id = db.create_creator("Origin Validation Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="613",
        external_post_id="100",
        title="Original title",
        content="original",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/613/post/100",
    )

    with pytest.raises(ValueError, match="Source versions require service, user id, and post id."):
        db.create_post_version(
            post_id=post_id,
            label="Broken source",
            language=None,
            origin_kind=LibraryDB.VERSION_ORIGIN_SOURCE,
            source_service=None,
            source_user_id=None,
            source_post_id=None,
            title="Broken",
            content="",
            metadata={},
            source_url=None,
            set_default=False,
        )

    with pytest.raises(ValueError, match="Clone versions require a parent version."):
        db.create_post_version(
            post_id=post_id,
            label="Broken clone",
            language="en",
            origin_kind=LibraryDB.VERSION_ORIGIN_CLONE,
            source_service=None,
            source_user_id=None,
            source_post_id=None,
            title="Broken",
            content="",
            metadata={},
            source_url=None,
            set_default=False,
        )

    with pytest.raises(ValueError, match="Manual versions cannot store a source tuple."):
        db.create_post_version(
            post_id=post_id,
            label="Broken manual",
            language=None,
            origin_kind=LibraryDB.VERSION_ORIGIN_MANUAL,
            source_service="fanbox",
            source_user_id="613",
            source_post_id="200",
            title="Broken",
            content="",
            metadata={},
            source_url="https://kemono.cr/fanbox/user/613/post/200",
            set_default=False,
        )


def test_update_post_version_records_revision_snapshot(tmp_path):
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
    creator_id = db.create_creator("Revision Edit Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="614",
        external_post_id="100",
        title="Original title",
        content="original body",
        metadata={"note": "first"},
        source_url="https://kemono.cr/fanbox/user/614/post/100",
    )

    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])
    assert version["revision_count"] == 0

    db.update_post_version(
        version_id=version_id,
        label=str(version["label"]),
        language=version["language"],
        title="Edited title",
        content="edited body",
        thumbnail_name=version["thumbnail_name"],
        thumbnail_remote_url=version["thumbnail_remote_url"],
        thumbnail_local_path=version["thumbnail_local_path"],
        published_at=version["published_at"],
        edited_at=version["edited_at"],
        next_external_post_id=version["next_external_post_id"],
        prev_external_post_id=version["prev_external_post_id"],
        metadata={"note": "second"},
        source_url=version["source_url"],
    )

    revisions = db.list_post_version_revisions(version_id)
    assert len(revisions) == 1
    assert revisions[0]["revision_number"] == 1
    assert revisions[0]["capture_kind"] == "edit"
    assert revisions[0]["title"] == "Original title"
    assert revisions[0]["content"] == "original body"
    assert revisions[0]["metadata_json"] == '{"note": "first"}'

    updated = db.get_post_version(post_id, version_id)
    assert updated is not None
    assert updated["title"] == "Edited title"
    assert updated["revision_count"] == 1


def test_upsert_post_records_source_refresh_revision_for_existing_version(tmp_path):
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
    creator_id = db.create_creator("Revision Import Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="615",
        external_post_id="100",
        title="Original title",
        content="original body",
        metadata={"note": "first"},
        source_url="https://kemono.cr/fanbox/user/615/post/100",
    )

    db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="615",
        external_post_id="100",
        title="Imported title",
        content="imported body",
        metadata={"note": "refreshed"},
        source_url="https://kemono.cr/fanbox/user/615/post/100",
    )

    version = db.get_post_version(post_id)
    assert version is not None
    revisions = db.list_post_version_revisions(int(version["id"]))
    assert len(revisions) == 1
    assert revisions[0]["capture_kind"] == "source_refresh"
    assert revisions[0]["title"] == "Original title"
    assert revisions[0]["content"] == "original body"
    assert version["title"] == "Imported title"
    assert version["revision_count"] == 1


def test_update_post_version_content_metadata_records_revision_snapshot(tmp_path):
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
    creator_id = db.create_creator("Revision Sync Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="616",
        external_post_id="100",
        title="Original title",
        content="inline old.png",
        metadata={"files": [{"path": "/old.png"}]},
        source_url="https://kemono.cr/fanbox/user/616/post/100",
    )

    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])

    db.update_post_version_content_metadata(
        version_id=version_id,
        content="inline new.png",
        metadata={"files": [{"path": "/new.png"}]},
    )

    revisions = db.list_post_version_revisions(version_id)
    assert len(revisions) == 1
    assert revisions[0]["capture_kind"] == "content_sync"
    assert revisions[0]["content"] == "inline old.png"
    assert revisions[0]["metadata_json"] == '{"files": [{"path": "/old.png"}]}'

    updated = db.get_post_version(post_id, version_id)
    assert updated is not None
    assert updated["content"] == "inline new.png"
    assert updated["revision_count"] == 1


def test_revision_snapshot_includes_version_assets(tmp_path):
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
    creator_id = db.create_creator("Revision Asset Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="617",
        external_post_id="100",
        title="Original title",
        content="original body",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/617/post/100",
    )

    version = db.get_post_version(post_id)
    assert version is not None
    version_id = int(version["id"])

    db.replace_attachments(
        post_id,
        [
            {
                "name": "cover.jpg",
                "remote_url": "https://cdn.example.test/cover.jpg",
                "local_path": f"post_{post_id}/cover.jpg",
                "kind": "image",
            }
        ],
        version_id=version_id,
    )
    db.replace_tags(post_id, ["alpha", "beta"], version_id=version_id)
    db.replace_previews(
        post_id,
        [
            {
                "type": "cover",
                "server": "img",
                "name": "preview.jpg",
                "path": "/preview.jpg",
            }
        ],
        version_id=version_id,
    )

    version = db.get_post_version(post_id, version_id)
    assert version is not None
    db.update_post_version(
        version_id=version_id,
        label=str(version["label"]),
        language=version["language"],
        title="Edited title",
        content="edited body",
        thumbnail_name=version["thumbnail_name"],
        thumbnail_remote_url=version["thumbnail_remote_url"],
        thumbnail_local_path=version["thumbnail_local_path"],
        published_at=version["published_at"],
        edited_at=version["edited_at"],
        next_external_post_id=version["next_external_post_id"],
        prev_external_post_id=version["prev_external_post_id"],
        metadata={},
        source_url=version["source_url"],
    )

    revision_row = db.list_post_version_revisions(version_id)[0]
    revision = LibraryDB.load_post_version_revision_snapshot(revision_row)
    assert revision is not None
    assert revision["attachments"] == [
        {
            "name": "cover.jpg",
            "remote_url": "https://cdn.example.test/cover.jpg",
            "local_path": f"post_{post_id}/cover.jpg",
            "kind": "image",
        }
    ]
    assert revision["tags"] == ["alpha", "beta"]
    assert revision["previews"] == [
        {
            "type": "cover",
            "server": "img",
            "name": "preview.jpg",
            "path": "/preview.jpg",
        }
    ]


def test_init_schema_adds_lineage_column_to_legacy_post_versions_table(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE creators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            service TEXT,
            external_user_id TEXT,
            icon_remote_url TEXT,
            icon_local_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (creator_id, name),
            FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE CASCADE
        );

        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_id INTEGER NOT NULL,
            series_id INTEGER,
            service TEXT NOT NULL,
            external_user_id TEXT NOT NULL,
            external_post_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            source_url TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (service, external_user_id, external_post_id),
            FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE CASCADE,
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE SET NULL
        );

        CREATE TABLE post_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            label TEXT NOT NULL DEFAULT 'Original',
            language TEXT,
            is_manual INTEGER NOT NULL DEFAULT 0,
            source_service TEXT,
            source_user_id TEXT,
            source_post_id TEXT,
            title TEXT NOT NULL,
            content TEXT,
            thumbnail_name TEXT,
            thumbnail_remote_url TEXT,
            thumbnail_local_path TEXT,
            published_at TEXT,
            edited_at TEXT,
            next_external_post_id TEXT,
            prev_external_post_id TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            source_url TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (post_id, source_service, source_user_id, source_post_id),
            FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()
    conn.close()

    db = LibraryDB(db_path)
    db.init_schema()

    verify_conn = sqlite3.connect(db_path)
    column_names = {
        row[1]
        for row in verify_conn.execute("PRAGMA table_info(post_versions)").fetchall()
    }
    revision_tables = {
        row[0]
        for row in verify_conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'post_version_revisions'"
        ).fetchall()
    }
    revision_column_names = {
        row[1]
        for row in verify_conn.execute("PRAGMA table_info(post_version_revisions)").fetchall()
    }
    verify_conn.close()
    assert "derived_from_version_id" in column_names
    assert "post_version_revisions" in revision_tables
    assert "attachments_json" in revision_column_names
    assert "tags_json" in revision_column_names
    assert "previews_json" in revision_column_names
    assert "origin_kind" in column_names
    assert "version_rank" in column_names


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


def test_resolve_link_missing_page_uses_dynamic_title(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
            "ICONS_DIR": str(tmp_path / "icons"),
        }
    )

    response = app.test_client().get("/links/resolve?service=fanbox&post=404&user=700")
    assert response.status_code == 200
    assert _page_title(response) == _expected_page_title("fanbox/404", "Missing Local Post")


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
        origin_kind=LibraryDB.VERSION_ORIGIN_SOURCE,
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
