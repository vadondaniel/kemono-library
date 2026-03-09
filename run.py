from kemono_library import create_app

app = create_app()

_RELOADER_EXCLUDES = [
    "data/*",
    "data/**",
    "*/data/*",
    "*/data/**",
    "data\\*",
    "data\\**",
    "*\\data\\*",
    "*\\data\\**",
    ".venv/*",
    ".venv/**",
    "*/.venv/*",
    "*/.venv/**",
    ".venv\\*",
    ".venv\\**",
    "*\\.venv\\*",
    "*\\.venv\\**",
    "__pycache__/*",
    "__pycache__/**",
    "*/__pycache__/*",
    "*/__pycache__/**",
    "__pycache__\\*",
    "__pycache__\\**",
    "*\\__pycache__\\*",
    "*\\__pycache__\\**",
    ".pytest_cache/*",
    ".pytest_cache/**",
    "*/.pytest_cache/*",
    "*/.pytest_cache/**",
    ".pytest_cache\\*",
    ".pytest_cache\\**",
    "*\\.pytest_cache\\*",
    "*\\.pytest_cache\\**",
    "*/site-packages/*",
    "*/site-packages/**",
    "*\\site-packages\\*",
    "*\\site-packages\\**",
    "*/Lib/test/*",
    "*/Lib/test/**",
    "*\\Lib\\test\\*",
    "*\\Lib\\test\\**",
]


if __name__ == "__main__":
    app.run(
        debug=True,
        # Keep live-reload for app source changes, but ignore env/runtime noise.
        exclude_patterns=_RELOADER_EXCLUDES,
    )
