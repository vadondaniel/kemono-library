from pathlib import Path

import pytest
import requests

from kemono_library.kemono import (
    COOMER_BASE,
    KEMONO_BASE,
    KemonoPostRef,
    _archive_root_host,
    _creator_icon_candidates,
    _download_attachment_with_curl,
    _image_extension_from_content_type,
    _normalize_archive_base,
    _normalize_archive_host,
    _payload_archive_base,
    _should_try_curl_fallback,
    creator_icon_url,
    download_attachment,
    extract_attachments,
    fetch_post_json,
    normalize_post_payload,
    parse_kemono_post_url,
    to_absolute_kemono_url,
)


def test_archive_normalization_helpers_cover_supported_and_invalid_hosts():
    assert _normalize_archive_host(None) is None
    assert _normalize_archive_host("user@WWW.Coomer.St:443") == "coomer.st"
    assert _archive_root_host("assets.kemono.cr") == "kemono.cr"
    assert _archive_root_host("example.com") is None
    assert _normalize_archive_base("coomer.st/onlyfans/user/name") == COOMER_BASE
    assert _normalize_archive_base("https://example.com/path") is None


def test_payload_archive_base_uses_service_or_nested_base():
    assert _payload_archive_base({"service": "onlyfans"}) == COOMER_BASE
    assert _payload_archive_base({"post": {"__archive_base__": "https://coomer.st/path"}}) == COOMER_BASE
    assert _payload_archive_base({"post": {"service": "fansly"}}) == COOMER_BASE


def test_creator_icon_candidates_handle_dedupe_and_hard_fallback(monkeypatch):
    candidates = _creator_icon_candidates("fansly", "abc", preferred_base_url=COOMER_BASE)
    assert candidates[0] == creator_icon_url("fansly", "abc", base_url=COOMER_BASE)
    assert creator_icon_url("fansly", "abc", base_url=KEMONO_BASE) in candidates

    monkeypatch.setattr("kemono_library.kemono._normalize_archive_base", lambda _base: None)
    fallback = _creator_icon_candidates("fanbox", "123")
    assert fallback == [creator_icon_url("fanbox", "123", base_url=KEMONO_BASE)]


def test_post_ref_and_parse_error_paths():
    ref = KemonoPostRef(service="fanbox", post_id="1", host="WWW.COOMER.ST")
    assert ref.base_url == COOMER_BASE
    assert ref.canonical_url == "https://coomer.st/fanbox/post/1"

    with pytest.raises(ValueError, match="user_id is required"):
        ref.api_url()
    with pytest.raises(ValueError, match="Post URL is required"):
        parse_kemono_post_url("   ")
    with pytest.raises(ValueError, match="Unsupported archive URL shape"):
        parse_kemono_post_url("https://kemono.cr/fanbox/user/1")


def test_fetch_post_json_rejects_non_object_payload(monkeypatch):
    class FakeResponse:
        text = "[]"

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return []

    def fake_get(*_args, **_kwargs):
        return FakeResponse()

    monkeypatch.setattr("kemono_library.kemono.requests.get", fake_get)
    with pytest.raises(ValueError, match="Expected JSON object"):
        fetch_post_json(KemonoPostRef(service="fanbox", user_id="1", post_id="2"))


def test_normalize_payload_without_post_and_relative_url_join():
    payload = {"title": "already normalized"}
    assert normalize_post_payload(payload) is payload
    assert to_absolute_kemono_url("folder/item.jpg", base_url="coomer.st") == "https://coomer.st/folder/item.jpg"


def test_extract_attachments_includes_shared_file_entries():
    items = extract_attachments(
        {
            "shared_file": {
                "name": "shared.png",
                "path": "/data/shared.png",
            }
        }
    )
    assert len(items) == 1
    assert items[0].kind == "shared_file"
    assert items[0].remote_url == "https://kemono.cr/data/shared.png"


def test_download_attachment_retries_insecure_for_external_ssl_error(tmp_path, monkeypatch):
    call_verify_flags: list[bool] = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def iter_content(chunk_size=65536):  # noqa: ARG002
            yield b"insecure-ok"

    def fake_get(url, stream, timeout, headers, verify=True):  # noqa: ARG001
        call_verify_flags.append(bool(verify))
        if verify:
            raise requests.exceptions.SSLError("tls failure")
        return FakeResponse()

    monkeypatch.setattr("kemono_library.kemono.requests.get", fake_get)

    destination = tmp_path / "asset.bin"
    download_attachment("https://cdn.example/asset.bin", destination)

    assert destination.read_bytes() == b"insecure-ok"
    assert call_verify_flags[0] is True
    assert False in call_verify_flags


def test_download_attachment_does_not_insecure_retry_on_supported_host(tmp_path, monkeypatch):
    call_verify_flags: list[bool] = []

    def fake_get(url, stream, timeout, headers, verify=True):  # noqa: ARG001
        call_verify_flags.append(bool(verify))
        raise requests.exceptions.SSLError("tls failure")

    monkeypatch.setattr("kemono_library.kemono.requests.get", fake_get)

    destination = tmp_path / "asset.bin"
    with pytest.raises(requests.exceptions.SSLError):
        download_attachment("https://kemono.cr/data/asset.bin", destination)
    assert call_verify_flags
    assert all(flag is True for flag in call_verify_flags)


def test_download_attachment_cleans_temp_file_when_stream_fails(tmp_path, monkeypatch):
    class BrokenStreamResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def iter_content(chunk_size=65536):  # noqa: ARG002
            yield b"partial"
            raise RuntimeError("stream interrupted")

    def fake_get(url, stream, timeout, headers, verify=True):  # noqa: ARG001
        return BrokenStreamResponse()

    monkeypatch.setattr("kemono_library.kemono.requests.get", fake_get)

    destination = tmp_path / "asset.bin"
    with pytest.raises(RuntimeError, match="stream interrupted"):
        download_attachment("https://kemono.cr/data/asset.bin", destination)
    assert list(destination.parent.glob(".http_*.part")) == []


def test_should_try_curl_fallback_guard_paths():
    response = requests.Response()
    response.status_code = 403
    http_error = requests.exceptions.HTTPError("403", response=response)

    assert not _should_try_curl_fallback("https://cdn.example/file.bin", None)
    assert not _should_try_curl_fallback("https://kemono.cr/file.bin", http_error)
    assert not _should_try_curl_fallback("https://cdn.example/file.bin", RuntimeError("other"))
    assert _should_try_curl_fallback("https://cdn.example/file.bin", http_error)


def test_download_attachment_with_curl_requires_available_binary(monkeypatch, tmp_path):
    monkeypatch.setattr("kemono_library.kemono.shutil.which", lambda _name: None)
    with pytest.raises(RuntimeError, match="curl is not available"):
        _download_attachment_with_curl("https://cdn.example/file.bin", tmp_path / "file.bin")


def test_image_extension_from_content_type_defaults_for_unknown_types():
    assert _image_extension_from_content_type(None) == ".img"
    assert _image_extension_from_content_type("image/png; charset=utf-8") == ".png"
