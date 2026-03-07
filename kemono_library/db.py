from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable


class LibraryDB:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS creators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    service TEXT,
                    external_user_id TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS series (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    creator_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (creator_id, name),
                    FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    creator_id INTEGER NOT NULL,
                    series_id INTEGER,
                    service TEXT NOT NULL,
                    external_user_id TEXT NOT NULL,
                    external_post_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT,
                    thumbnail_name TEXT,
                    thumbnail_remote_url TEXT,
                    thumbnail_local_path TEXT,
                    published_at TEXT,
                    edited_at TEXT,
                    next_external_post_id TEXT,
                    prev_external_post_id TEXT,
                    metadata_json TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (service, external_user_id, external_post_id),
                    FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE CASCADE,
                    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    remote_url TEXT NOT NULL,
                    local_path TEXT,
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (post_id, remote_url),
                    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS post_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER NOT NULL,
                    tag TEXT NOT NULL,
                    UNIQUE (post_id, tag),
                    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS post_previews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER NOT NULL,
                    preview_type TEXT,
                    server TEXT NOT NULL DEFAULT '',
                    name TEXT,
                    path TEXT NOT NULL,
                    UNIQUE (post_id, path, server),
                    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
                );
                """
            )
            self._ensure_post_columns(conn)

    def create_creator(self, name: str) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO creators (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
                (name.strip(),),
            )
            if cursor.lastrowid:
                return int(cursor.lastrowid)
            row = conn.execute("SELECT id FROM creators WHERE name = ?", (name.strip(),)).fetchone()
            return int(row["id"])

    def list_creators(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.*,
                       (SELECT COUNT(*) FROM posts p WHERE p.creator_id = c.id) AS post_count
                FROM creators c
                ORDER BY c.name COLLATE NOCASE
                """
            ).fetchall()
            return list(rows)

    def get_creator(self, creator_id: int) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM creators WHERE id = ?", (creator_id,)).fetchone()

    def attach_creator_external(self, creator_id: int, service: str, external_user_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE creators
                SET service = COALESCE(service, ?),
                    external_user_id = COALESCE(external_user_id, ?)
                WHERE id = ?
                """,
                (service, external_user_id, creator_id),
            )

    def create_series(self, creator_id: int, name: str) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO series (creator_id, name)
                VALUES (?, ?)
                ON CONFLICT(creator_id, name) DO NOTHING
                """,
                (creator_id, name.strip()),
            )
            if cursor.lastrowid:
                return int(cursor.lastrowid)
            row = conn.execute(
                "SELECT id FROM series WHERE creator_id = ? AND name = ?",
                (creator_id, name.strip()),
            ).fetchone()
            return int(row["id"])

    def list_series(self, creator_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.*,
                       (SELECT COUNT(*) FROM posts p WHERE p.series_id = s.id) AS post_count
                FROM series s
                WHERE s.creator_id = ?
                ORDER BY s.name COLLATE NOCASE
                """,
                (creator_id,),
            ).fetchall()
            return list(rows)

    def upsert_post(
        self,
        creator_id: int,
        series_id: int | None,
        service: str,
        external_user_id: str,
        external_post_id: str,
        title: str,
        content: str,
        metadata: dict,
        source_url: str,
        thumbnail_name: str | None = None,
        thumbnail_remote_url: str | None = None,
        thumbnail_local_path: str | None = None,
        published_at: str | None = None,
        edited_at: str | None = None,
        next_external_post_id: str | None = None,
        prev_external_post_id: str | None = None,
    ) -> int:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO posts (
                    creator_id, series_id, service, external_user_id, external_post_id,
                    title, content, thumbnail_name, thumbnail_remote_url, thumbnail_local_path,
                    published_at, edited_at, next_external_post_id,
                    prev_external_post_id, metadata_json, source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service, external_user_id, external_post_id) DO UPDATE SET
                    creator_id = excluded.creator_id,
                    series_id = excluded.series_id,
                    title = excluded.title,
                    content = excluded.content,
                    thumbnail_name = excluded.thumbnail_name,
                    thumbnail_remote_url = excluded.thumbnail_remote_url,
                    thumbnail_local_path = excluded.thumbnail_local_path,
                    published_at = excluded.published_at,
                    edited_at = excluded.edited_at,
                    next_external_post_id = excluded.next_external_post_id,
                    prev_external_post_id = excluded.prev_external_post_id,
                    metadata_json = excluded.metadata_json,
                    source_url = excluded.source_url,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    creator_id,
                    series_id,
                    service,
                    external_user_id,
                    external_post_id,
                    title,
                    content,
                    thumbnail_name,
                    thumbnail_remote_url,
                    thumbnail_local_path,
                    published_at,
                    edited_at,
                    next_external_post_id,
                    prev_external_post_id,
                    json.dumps(metadata, ensure_ascii=True),
                    source_url,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM posts
                WHERE service = ? AND external_user_id = ? AND external_post_id = ?
                """,
                (service, external_user_id, external_post_id),
            ).fetchone()
            return int(row["id"])

    def replace_attachments(self, post_id: int, attachments: list[dict]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM attachments WHERE post_id = ?", (post_id,))
            for attachment in attachments:
                conn.execute(
                    """
                    INSERT INTO attachments (post_id, name, remote_url, local_path, kind)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        post_id,
                        attachment["name"],
                        attachment["remote_url"],
                        attachment.get("local_path"),
                        attachment["kind"],
                    ),
                )

    def replace_tags(self, post_id: int, tags: list[str]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM post_tags WHERE post_id = ?", (post_id,))
            deduped = []
            seen: set[str] = set()
            for tag in tags:
                normalized = tag.strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                deduped.append(normalized)
            for tag in deduped:
                conn.execute(
                    "INSERT INTO post_tags (post_id, tag) VALUES (?, ?)",
                    (post_id, tag),
                )

    def list_tags(self, post_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM post_tags WHERE post_id = ? ORDER BY tag COLLATE NOCASE",
                (post_id,),
            ).fetchall()
            return list(rows)

    def replace_previews(self, post_id: int, previews: list[dict]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM post_previews WHERE post_id = ?", (post_id,))
            for preview in previews:
                path = str(preview.get("path", "")).strip()
                if not path:
                    continue
                conn.execute(
                    """
                    INSERT INTO post_previews (post_id, preview_type, server, name, path)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        post_id,
                        str(preview.get("type", "")).strip() or None,
                        str(preview.get("server", "")).strip(),
                        str(preview.get("name", "")).strip() or None,
                        path,
                    ),
                )

    def list_previews(self, post_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM post_previews WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            return list(rows)

    def list_recent_posts(self, limit: int = 25) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*, c.name AS creator_name, s.name AS series_name
                FROM posts p
                JOIN creators c ON c.id = p.creator_id
                LEFT JOIN series s ON s.id = p.series_id
                ORDER BY p.updated_at DESC, p.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return list(rows)

    def list_posts_for_creator(self, creator_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*, s.name AS series_name
                FROM posts p
                LEFT JOIN series s ON s.id = p.series_id
                WHERE p.creator_id = ?
                ORDER BY p.updated_at DESC, p.id DESC
                """,
                (creator_id,),
            ).fetchall()
            return list(rows)

    def get_post(self, post_id: int) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT p.*, c.name AS creator_name, s.name AS series_name
                FROM posts p
                JOIN creators c ON c.id = p.creator_id
                LEFT JOIN series s ON s.id = p.series_id
                WHERE p.id = ?
                """,
                (post_id,),
            ).fetchone()

    def list_attachments(self, post_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM attachments WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            return list(rows)

    def update_post(self, post_id: int, title: str, content: str, series_id: int | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE posts
                SET title = ?, content = ?, series_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (title, content, series_id, post_id),
            )

    def update_post_thumbnail(self, post_id: int, thumbnail_local_path: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE posts
                SET thumbnail_local_path = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (thumbnail_local_path, post_id),
            )

    def find_local_post(
        self, service: str, external_post_id: str, external_user_id: str | None = None
    ) -> sqlite3.Row | None:
        with self._connect() as conn:
            if external_user_id:
                return conn.execute(
                    """
                    SELECT id
                    FROM posts
                    WHERE service = ? AND external_user_id = ? AND external_post_id = ?
                    """,
                    (service, external_user_id, external_post_id),
                ).fetchone()
            return conn.execute(
                """
                SELECT id
                FROM posts
                WHERE service = ? AND external_post_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (service, external_post_id),
            ).fetchone()

    def _ensure_post_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(posts)").fetchall()
        }
        required = {
            "thumbnail_name": "TEXT",
            "thumbnail_remote_url": "TEXT",
            "thumbnail_local_path": "TEXT",
            "published_at": "TEXT",
            "edited_at": "TEXT",
            "next_external_post_id": "TEXT",
            "prev_external_post_id": "TEXT",
        }
        for column, column_type in required.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE posts ADD COLUMN {column} {column_type}")
