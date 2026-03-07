from pathlib import Path

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
