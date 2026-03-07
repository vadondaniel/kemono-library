# Kemono Local Library

Local web app to build your own library of Kemono posts.

## Features

- Create creators and creator-specific series/groups.
- Import Kemono posts from:
  - `https://kemono.cr/{service}/user/{user_id}/post/{post_id}`
  - `https://kemono.cr/{service}/post/{post_id}` (resolved with creator context when possible)
- Preview post import and choose which files to download.
- Save post content + full metadata JSON into SQLite.
- View saved posts with sanitized HTML formatting support.
- Edit saved post title/content and series assignment.
- Rewrite Kemono links inside post content to local resolver links:
  - Redirects to local saved post if found.
  - Otherwise offers one-click import of the linked post.

## Run

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Start app:
   - `python run.py`
3. Open:
   - `http://127.0.0.1:5000`

## Tests

- Run all tests:
  - `pytest -q`

## Data Location

- Database: `data/library.db`
- Downloaded files: `data/files/`
