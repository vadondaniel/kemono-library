# Kemono Local Library

Local Flask app for building a personal library of imported Kemono posts, attachments, and creator metadata.

## Features

- Create creators and creator-specific series or groups.
- Import Kemono posts from:
  - `https://kemono.cr/{service}/user/{user_id}/post/{post_id}`
  - `https://kemono.cr/{service}/post/{post_id}`
- Preview imports and choose which files to download.
- Persist post content, attachments, previews, and source metadata in SQLite.
- View saved posts with sanitized HTML formatting.
- Edit saved post titles, content, tags, and series assignment.
- Rewrite Kemono links in imported content so saved links resolve locally when possible.

## Requirements

- Python 3.10+
- Network access for remote Kemono imports

## Quick Start

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Start the app:
   - `python run.py`
4. Open:
   - `http://127.0.0.1:5000`

Runtime data is stored locally under `data/` by default and is ignored by git.

## Configuration

The app reads configuration from environment variables when they are present:

- `KEMONO_LIBRARY_SECRET_KEY`
  Use this if you want stable Flask sessions across restarts.
- `KEMONO_LIBRARY_DATA_DIR`
  Base directory for local runtime data. Defaults to `data/`.
- `KEMONO_LIBRARY_DATABASE`
  Override the SQLite database path.
- `KEMONO_LIBRARY_FILES_DIR`
  Override the attachment storage directory.
- `KEMONO_LIBRARY_ICONS_DIR`
  Override the creator icon storage directory.

If `KEMONO_LIBRARY_SECRET_KEY` is not set, the app generates a fresh local secret on startup instead of relying on a hardcoded development value.

## Tests

Run the test suite with:

```bash
pytest -q
```

## Repo Notes

- Local databases, downloads, debug folders, and editor settings are excluded from version control.
- No sample content is committed with the repository.
- If you plan to publish the repo publicly, review existing git history before pushing in case earlier commits contain personal metadata you do not want exposed.
