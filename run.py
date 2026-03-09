from pathlib import Path

from kemono_library import create_app

app = create_app()


def _exclude_dir_patterns(path: Path) -> list[str]:
    resolved = path.resolve()
    posix = resolved.as_posix()
    windows = str(resolved)
    return [
        posix,
        f"{posix}/*",
        f"{posix}/**",
        windows,
        f"{windows}\\*",
        f"{windows}\\**",
    ]


_PROJECT_ROOT = Path(__file__).resolve().parent
_RELOADER_EXCLUDES: list[str] = []
for name in ("data", ".venv", "tests"):
    _RELOADER_EXCLUDES.extend(_exclude_dir_patterns(_PROJECT_ROOT / name))
for path in (
    _PROJECT_ROOT / ".venv" / "Lib" / "site-packages",
    _PROJECT_ROOT / ".venv" / "Lib" / "test",
    _PROJECT_ROOT / "__pycache__",
    _PROJECT_ROOT / ".pytest_cache",
):
    _RELOADER_EXCLUDES.extend(_exclude_dir_patterns(path))


if __name__ == "__main__":
    app.run(
        debug=True,
        # Use the stat reloader so downloaded archives and data files don't trip
        # watchdog's broad file event patterns on Windows.
        reloader_type="stat",
        # Keep live-reload for app source changes, but ignore env/runtime and test noise.
        exclude_patterns=_RELOADER_EXCLUDES,
    )
