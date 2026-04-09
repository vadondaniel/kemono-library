import os
from pathlib import Path

from kemono_library.web import _load_env_file, create_app


def test_load_env_file_sets_missing_values_only(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "# comment",
                "KEMONO_LIBRARY_SECRET_KEY=from-env-file",
                "export KEMONO_LIBRARY_DATABASE=custom.db",
                'KEMONO_LIBRARY_FILES_DIR="custom files"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("KEMONO_LIBRARY_SECRET_KEY", raising=False)
    monkeypatch.setenv("KEMONO_LIBRARY_DATABASE", "existing.db")
    monkeypatch.delenv("KEMONO_LIBRARY_FILES_DIR", raising=False)

    _load_env_file(env_path)

    assert os.environ["KEMONO_LIBRARY_SECRET_KEY"] == "from-env-file"
    assert os.environ["KEMONO_LIBRARY_DATABASE"] == "existing.db"
    assert os.environ["KEMONO_LIBRARY_FILES_DIR"] == "custom files"


def test_create_app_uses_env_config(tmp_path, monkeypatch):
    monkeypatch.setenv("KEMONO_LIBRARY_SECRET_KEY", "stable-secret")
    monkeypatch.setenv("KEMONO_LIBRARY_DATABASE", str(tmp_path / "library.db"))
    monkeypatch.setenv("KEMONO_LIBRARY_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.setenv("KEMONO_LIBRARY_ICONS_DIR", str(tmp_path / "icons"))

    app = create_app({"TESTING": True})

    assert app.config["SECRET_KEY"] == "stable-secret"
    assert Path(app.config["DATABASE"]) == tmp_path / "library.db"
    assert Path(app.config["FILES_DIR"]) == tmp_path / "files"
    assert Path(app.config["ICONS_DIR"]) == tmp_path / "icons"
