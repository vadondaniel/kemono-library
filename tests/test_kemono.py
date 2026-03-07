from kemono_library.kemono import extract_attachments, parse_kemono_post_url


def test_parse_full_kemono_url():
    ref = parse_kemono_post_url("https://kemono.cr/fanbox/user/70479526/post/10791194")
    assert ref.service == "fanbox"
    assert ref.user_id == "70479526"
    assert ref.post_id == "10791194"
    assert ref.api_url() == "https://kemono.cr/api/v1/fanbox/user/70479526/post/10791194"


def test_parse_short_kemono_url():
    ref = parse_kemono_post_url("https://kemono.cr/fanbox/post/9581944")
    assert ref.service == "fanbox"
    assert ref.user_id is None
    assert ref.post_id == "9581944"


def test_extract_attachments_collects_main_and_list():
    payload = {
        "file": {"name": "cover.png", "path": "/data/abc/cover.png"},
        "attachments": [
            {"name": "page-1.jpg", "path": "/data/abc/page-1.jpg"},
            {"name": "doc.zip", "url": "https://files.example/doc.zip"},
        ],
    }
    items = extract_attachments(payload)
    assert len(items) == 3
    assert items[0].name == "cover.png"
    assert items[0].remote_url == "https://kemono.cr/data/abc/cover.png"
