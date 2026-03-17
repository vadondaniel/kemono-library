from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from typing import Iterator


class LibraryDB:
    VERSION_ORIGIN_SOURCE = "source"
    VERSION_ORIGIN_CLONE = "clone"
    VERSION_ORIGIN_MANUAL = "manual"
    SERIES_COVER_POST_AUTO_FIRST = -2
    VERSION_ORIGIN_KINDS = {
        VERSION_ORIGIN_SOURCE,
        VERSION_ORIGIN_CLONE,
        VERSION_ORIGIN_MANUAL,
    }

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = self._open_connection()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._open_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
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
                    icon_remote_url TEXT,
                    icon_local_path TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS series (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    creator_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    tags_text TEXT,
                    default_sort_by TEXT,
                    default_sort_direction TEXT,
                    cover_post_id INTEGER,
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
            self._ensure_creator_columns(conn)
            self._ensure_series_columns(conn)
            self._ensure_post_columns(conn)
            self._ensure_version_schema(conn)
            self._backfill_post_versions(conn)

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

    def update_creator(
        self,
        creator_id: int,
        *,
        name: str,
        description: str | None = None,
        tags_text: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE creators
                SET name = ?, description = ?, tags_text = ?
                WHERE id = ?
                """,
                (
                    name.strip(),
                    self._normalize_optional_text(description),
                    self._normalize_optional_text(tags_text),
                    creator_id,
                ),
            )

    def list_post_ids_for_creator(self, creator_id: int) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM posts WHERE creator_id = ?",
                (creator_id,),
            ).fetchall()
            return [int(row["id"]) for row in rows]

    def delete_creator(self, creator_id: int) -> int:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM creators WHERE id = ?", (creator_id,))
            return int(cursor.rowcount or 0)

    def attach_creator_external(
        self,
        creator_id: int,
        service: str,
        external_user_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.attach_creator_external(
                    creator_id,
                    service,
                    external_user_id,
                    conn=own_conn,
                )
        conn.execute(
            """
            UPDATE creators
            SET service = COALESCE(service, ?),
                external_user_id = COALESCE(external_user_id, ?)
            WHERE id = ?
            """,
            (service, external_user_id, creator_id),
        )

    def update_creator_icon(
        self,
        creator_id: int,
        *,
        icon_remote_url: str | None,
        icon_local_path: str | None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.update_creator_icon(
                    creator_id,
                    icon_remote_url=icon_remote_url,
                    icon_local_path=icon_local_path,
                    conn=own_conn,
                )
        conn.execute(
            """
            UPDATE creators
            SET icon_remote_url = ?, icon_local_path = ?
            WHERE id = ?
            """,
            (icon_remote_url, icon_local_path, creator_id),
        )

    def create_series(
        self,
        creator_id: int,
        name: str,
        *,
        description: str | None = None,
        tags_text: str | None = None,
        default_sort_by: str | None = None,
        default_sort_direction: str | None = None,
        cover_post_id: int | None = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO series (
                    creator_id,
                    name,
                    description,
                    tags_text,
                    default_sort_by,
                    default_sort_direction,
                    cover_post_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(creator_id, name) DO UPDATE SET
                    description = excluded.description,
                    tags_text = excluded.tags_text,
                    default_sort_by = excluded.default_sort_by,
                    default_sort_direction = excluded.default_sort_direction,
                    cover_post_id = excluded.cover_post_id
                """,
                (
                    creator_id,
                    name.strip(),
                    self._normalize_optional_text(description),
                    self._normalize_optional_text(tags_text),
                    self._normalize_series_sort_by(default_sort_by),
                    self._normalize_series_sort_direction(default_sort_direction),
                    cover_post_id,
                ),
            )
            if cursor.lastrowid:
                return int(cursor.lastrowid)
            row = conn.execute(
                "SELECT id FROM series WHERE creator_id = ? AND name = ?",
                (creator_id, name.strip()),
            ).fetchone()
            return int(row["id"])

    def update_series(
        self,
        series_id: int,
        *,
        name: str,
        description: str | None = None,
        tags_text: str | None = None,
        default_sort_by: str | None = None,
        default_sort_direction: str | None = None,
        cover_post_id: int | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE series
                SET name = ?,
                    description = ?,
                    tags_text = ?,
                    default_sort_by = ?,
                    default_sort_direction = ?,
                    cover_post_id = ?
                WHERE id = ?
                """,
                (
                    name.strip(),
                    self._normalize_optional_text(description),
                    self._normalize_optional_text(tags_text),
                    self._normalize_series_sort_by(default_sort_by),
                    self._normalize_series_sort_direction(default_sort_direction),
                    cover_post_id,
                    series_id,
                ),
            )

    def list_series(self, creator_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            first_cover_sentinel = int(self.SERIES_COVER_POST_AUTO_FIRST)
            thumbnail_available_sql = (
                "(p.thumbnail_local_path IS NOT NULL AND TRIM(p.thumbnail_local_path) <> '') "
                "OR (p.thumbnail_remote_url IS NOT NULL AND TRIM(p.thumbnail_remote_url) <> '')"
            )
            fallback_cover_order_latest_sql = (
                "CASE WHEN p.published_at IS NULL OR p.published_at = '' THEN 1 ELSE 0 END ASC, "
                "p.published_at DESC, "
                "p.id DESC"
            )
            fallback_cover_order_first_sql = (
                "CASE WHEN p.published_at IS NULL OR p.published_at = '' THEN 1 ELSE 0 END ASC, "
                "p.published_at ASC, "
                "p.id ASC"
            )
            rows = conn.execute(
                f"""
                SELECT s.*,
                       (SELECT COUNT(*) FROM posts p WHERE p.series_id = s.id) AS post_count,
                       COALESCE(
                           (
                               SELECT p.thumbnail_local_path
                               FROM posts p
                               WHERE p.id = s.cover_post_id AND p.series_id = s.id
                               LIMIT 1
                           ),
                           CASE
                               WHEN s.cover_post_id = {first_cover_sentinel}
                               THEN (
                                   SELECT p.thumbnail_local_path
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_first_sql}
                                   LIMIT 1
                               )
                               ELSE (
                                   SELECT p.thumbnail_local_path
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_latest_sql}
                                   LIMIT 1
                               )
                           END
                       ) AS cover_thumbnail_local_path,
                       COALESCE(
                           (
                               SELECT p.thumbnail_remote_url
                               FROM posts p
                               WHERE p.id = s.cover_post_id AND p.series_id = s.id
                               LIMIT 1
                           ),
                           CASE
                               WHEN s.cover_post_id = {first_cover_sentinel}
                               THEN (
                                   SELECT p.thumbnail_remote_url
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_first_sql}
                                   LIMIT 1
                               )
                               ELSE (
                                   SELECT p.thumbnail_remote_url
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_latest_sql}
                                   LIMIT 1
                               )
                           END
                       ) AS cover_thumbnail_remote_url,
                       COALESCE(
                           (
                               SELECT p.metadata_json
                               FROM posts p
                               WHERE p.id = s.cover_post_id AND p.series_id = s.id
                               LIMIT 1
                           ),
                           CASE
                               WHEN s.cover_post_id = {first_cover_sentinel}
                               THEN (
                                   SELECT p.metadata_json
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_first_sql}
                                   LIMIT 1
                               )
                               ELSE (
                                   SELECT p.metadata_json
                                   FROM posts p
                                   WHERE p.series_id = s.id
                                     AND ({thumbnail_available_sql})
                                   ORDER BY {fallback_cover_order_latest_sql}
                                   LIMIT 1
                               )
                           END
                       ) AS cover_metadata_json
                FROM series s
                WHERE s.creator_id = ?
                ORDER BY s.name COLLATE NOCASE
                """,
                (creator_id,),
            ).fetchall()
            return list(rows)

    def get_series(self, series_id: int) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM series
                WHERE id = ?
                """,
                (series_id,),
            ).fetchone()

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
        conn: sqlite3.Connection | None = None,
    ) -> int:
        if conn is None:
            with self._connect() as own_conn:
                return self.upsert_post(
                    creator_id=creator_id,
                    series_id=series_id,
                    service=service,
                    external_user_id=external_user_id,
                    external_post_id=external_post_id,
                    title=title,
                    content=content,
                    metadata=metadata,
                    source_url=source_url,
                    thumbnail_name=thumbnail_name,
                    thumbnail_remote_url=thumbnail_remote_url,
                    thumbnail_local_path=thumbnail_local_path,
                    published_at=published_at,
                    edited_at=edited_at,
                    next_external_post_id=next_external_post_id,
                    prev_external_post_id=prev_external_post_id,
                    conn=own_conn,
                )

        row = conn.execute(
            """
            SELECT id
            FROM posts
            WHERE service = ? AND external_user_id = ? AND external_post_id = ?
            """,
            (service, external_user_id, external_post_id),
        ).fetchone()
        if row:
            post_id = int(row["id"])
            conn.execute(
                """
                UPDATE posts
                SET creator_id = ?,
                    series_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (creator_id, series_id, post_id),
            )
        else:
            cursor = conn.execute(
                """
                INSERT INTO posts (
                    creator_id, series_id, service, external_user_id, external_post_id,
                    title, content, thumbnail_name, thumbnail_remote_url, thumbnail_local_path,
                    published_at, edited_at, next_external_post_id,
                    prev_external_post_id, metadata_json, source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            if cursor.lastrowid is None:
                raise RuntimeError("Failed to create post row.")
            post_id = int(cursor.lastrowid)

        existing_version = conn.execute(
            """
            SELECT *
            FROM post_versions
            WHERE post_id = ?
              AND source_service = ?
              AND source_user_id = ?
              AND source_post_id = ?
            LIMIT 1
            """,
            (post_id, service, external_user_id, external_post_id),
        ).fetchone()
        if existing_version:
            next_metadata_json = json.dumps(metadata, ensure_ascii=True)
            next_source_url = self._normalize_optional_text(source_url)
            if (
                str(existing_version["title"]) != title
                or str(existing_version["content"] or "") != content
                or self._normalize_optional_text(existing_version["thumbnail_name"])
                != self._normalize_optional_text(thumbnail_name)
                or self._normalize_optional_text(existing_version["thumbnail_remote_url"])
                != self._normalize_optional_text(thumbnail_remote_url)
                or self._normalize_optional_text(existing_version["thumbnail_local_path"])
                != self._normalize_optional_text(thumbnail_local_path)
                or self._normalize_optional_text(existing_version["published_at"])
                != self._normalize_optional_text(published_at)
                or self._normalize_optional_text(existing_version["edited_at"])
                != self._normalize_optional_text(edited_at)
                or self._normalize_optional_text(existing_version["next_external_post_id"])
                != self._normalize_optional_text(next_external_post_id)
                or self._normalize_optional_text(existing_version["prev_external_post_id"])
                != self._normalize_optional_text(prev_external_post_id)
                or str(existing_version["metadata_json"]) != next_metadata_json
                or self._normalize_optional_text(existing_version["source_url"]) != next_source_url
            ):
                self._record_post_version_revision_conn(
                    conn,
                    int(existing_version["id"]),
                    capture_kind="source_refresh",
                    row=existing_version,
                )
            conn.execute(
                """
                UPDATE post_versions
                SET title = ?,
                    content = ?,
                    thumbnail_name = ?,
                    thumbnail_remote_url = ?,
                    thumbnail_local_path = ?,
                    published_at = ?,
                    edited_at = ?,
                    next_external_post_id = ?,
                    prev_external_post_id = ?,
                    metadata_json = ?,
                    source_url = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    title,
                    content,
                    thumbnail_name,
                    thumbnail_remote_url,
                    thumbnail_local_path,
                    published_at,
                    edited_at,
                    next_external_post_id,
                    prev_external_post_id,
                    next_metadata_json,
                    next_source_url,
                    int(existing_version["id"]),
                ),
            )
            if not self._get_default_version_id_conn(conn, post_id):
                self._set_default_post_version_conn(conn, post_id=post_id, version_id=int(existing_version["id"]))
        else:
            self.create_post_version(
                post_id=post_id,
                label="Original",
                language=None,
                origin_kind=self.VERSION_ORIGIN_SOURCE,
                source_service=service,
                source_user_id=external_user_id,
                source_post_id=external_post_id,
                title=title,
                content=content,
                metadata=metadata,
                source_url=source_url,
                thumbnail_name=thumbnail_name,
                thumbnail_remote_url=thumbnail_remote_url,
                thumbnail_local_path=thumbnail_local_path,
                published_at=published_at,
                edited_at=edited_at,
                next_external_post_id=next_external_post_id,
                prev_external_post_id=prev_external_post_id,
                set_default=not self._get_default_version_id_conn(conn, post_id),
                conn=conn,
            )
        self._sync_post_from_default_version_conn(conn, post_id)
        return post_id

    def list_post_versions(self, post_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT v.*,
                       parent.label AS derived_from_label,
                       parent.language AS derived_from_language,
                       parent.title AS derived_from_title,
                       (
                           SELECT COUNT(*)
                           FROM post_version_revisions r
                           WHERE r.version_id = v.id
                       ) AS revision_count,
                       CASE WHEN p.default_version_id = v.id THEN 1 ELSE 0 END AS is_default
                FROM post_versions v
                JOIN posts p ON p.id = v.post_id
                LEFT JOIN post_versions parent ON parent.id = v.derived_from_version_id
                WHERE v.post_id = ?
                ORDER BY
                    CASE WHEN p.default_version_id = v.id THEN 0 ELSE 1 END ASC,
                    v.version_rank DESC,
                    v.id DESC
                """,
                (post_id,),
            ).fetchall()
            return list(rows)

    def get_post_version(self, post_id: int, version_id: int | None = None) -> sqlite3.Row | None:
        with self._connect() as conn:
            resolved_version_id = version_id or self._get_default_version_id_conn(conn, post_id)
            if not resolved_version_id:
                return None
            return conn.execute(
                """
                SELECT v.*,
                       parent.label AS derived_from_label,
                       parent.language AS derived_from_language,
                       parent.title AS derived_from_title,
                       (
                           SELECT COUNT(*)
                           FROM post_version_revisions r
                           WHERE r.version_id = v.id
                       ) AS revision_count,
                       CASE WHEN p.default_version_id = v.id THEN 1 ELSE 0 END AS is_default
                FROM post_versions v
                JOIN posts p ON p.id = v.post_id
                LEFT JOIN post_versions parent ON parent.id = v.derived_from_version_id
                WHERE v.id = ? AND v.post_id = ?
                """,
                (resolved_version_id, post_id),
            ).fetchone()

    def list_post_version_revisions(self, version_id: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM post_version_revisions
                WHERE version_id = ?
                ORDER BY revision_number DESC, id DESC
                """,
                (version_id,),
            ).fetchall()
            return list(rows)

    @staticmethod
    def load_post_version_revision_snapshot(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        snapshot = dict(row)
        for key, empty_default in (
            ("attachments_json", []),
            ("tags_json", []),
            ("previews_json", []),
        ):
            raw = snapshot.get(key)
            if not isinstance(raw, str) or not raw.strip():
                snapshot[key.removesuffix("_json")] = list(empty_default)
                continue
            try:
                loaded = json.loads(raw)
            except json.JSONDecodeError:
                snapshot[key.removesuffix("_json")] = list(empty_default)
                continue
            snapshot[key.removesuffix("_json")] = loaded if isinstance(loaded, list) else list(empty_default)
        return snapshot

    def create_post_version(
        self,
        *,
        post_id: int,
        label: str,
        language: str | None,
        origin_kind: str,
        source_service: str | None,
        source_user_id: str | None,
        source_post_id: str | None,
        title: str,
        content: str,
        metadata: dict[str, Any],
        source_url: str | None,
        thumbnail_name: str | None = None,
        thumbnail_remote_url: str | None = None,
        thumbnail_local_path: str | None = None,
        published_at: str | None = None,
        edited_at: str | None = None,
        next_external_post_id: str | None = None,
        prev_external_post_id: str | None = None,
        derived_from_version_id: int | None = None,
        set_default: bool = False,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        if conn is not None:
            version_id = self._insert_post_version_conn(
                conn=conn,
                post_id=post_id,
                label=label,
                language=language,
                origin_kind=origin_kind,
                source_service=source_service,
                source_user_id=source_user_id,
                source_post_id=source_post_id,
                title=title,
                content=content,
                metadata=metadata,
                source_url=source_url,
                thumbnail_name=thumbnail_name,
                thumbnail_remote_url=thumbnail_remote_url,
                thumbnail_local_path=thumbnail_local_path,
                published_at=published_at,
                edited_at=edited_at,
                next_external_post_id=next_external_post_id,
                prev_external_post_id=prev_external_post_id,
                derived_from_version_id=derived_from_version_id,
            )
            if set_default or not self._get_default_version_id_conn(conn, post_id):
                self._set_default_post_version_conn(conn, post_id=post_id, version_id=version_id)
            return version_id

        with self._connect() as own_conn:
            return self.create_post_version(
                post_id=post_id,
                label=label,
                language=language,
                origin_kind=origin_kind,
                source_service=source_service,
                source_user_id=source_user_id,
                source_post_id=source_post_id,
                title=title,
                content=content,
                metadata=metadata,
                source_url=source_url,
                thumbnail_name=thumbnail_name,
                thumbnail_remote_url=thumbnail_remote_url,
                thumbnail_local_path=thumbnail_local_path,
                published_at=published_at,
                edited_at=edited_at,
                next_external_post_id=next_external_post_id,
                prev_external_post_id=prev_external_post_id,
                derived_from_version_id=derived_from_version_id,
                set_default=set_default,
                conn=own_conn,
            )

    def clone_post_version(
        self,
        *,
        post_id: int,
        source_version_id: int,
        label: str,
        language: str | None,
        set_default: bool,
    ) -> int:
        with self._connect() as conn:
            source = conn.execute(
                "SELECT * FROM post_versions WHERE id = ? AND post_id = ?",
                (source_version_id, post_id),
            ).fetchone()
            if not source:
                raise ValueError("Source version not found.")

            version_id = self._insert_post_version_conn(
                conn=conn,
                post_id=post_id,
                label=label,
                language=language,
                origin_kind=self.VERSION_ORIGIN_CLONE,
                source_service=None,
                source_user_id=None,
                source_post_id=None,
                title=str(source["title"]),
                content=str(source["content"] or ""),
                metadata=self._safe_json_load(source["metadata_json"]),
                source_url=source["source_url"],
                thumbnail_name=source["thumbnail_name"],
                thumbnail_remote_url=source["thumbnail_remote_url"],
                thumbnail_local_path=source["thumbnail_local_path"],
                published_at=source["published_at"],
                edited_at=source["edited_at"],
                next_external_post_id=source["next_external_post_id"],
                prev_external_post_id=source["prev_external_post_id"],
                derived_from_version_id=source_version_id,
            )

            attachments = conn.execute(
                """
                SELECT name, remote_url, local_path, kind
                FROM post_version_attachments
                WHERE version_id = ?
                ORDER BY id
                """,
                (source_version_id,),
            ).fetchall()
            for item in attachments:
                conn.execute(
                    """
                    INSERT INTO post_version_attachments (version_id, name, remote_url, local_path, kind)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (version_id, item["name"], item["remote_url"], item["local_path"], item["kind"]),
                )

            tags = conn.execute(
                "SELECT tag FROM post_version_tags WHERE version_id = ? ORDER BY id",
                (source_version_id,),
            ).fetchall()
            for tag in tags:
                conn.execute(
                    "INSERT OR IGNORE INTO post_version_tags (version_id, tag) VALUES (?, ?)",
                    (version_id, tag["tag"]),
                )

            previews = conn.execute(
                """
                SELECT preview_type, server, name, path
                FROM post_version_previews
                WHERE version_id = ?
                ORDER BY id
                """,
                (source_version_id,),
            ).fetchall()
            for preview in previews:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO post_version_previews (version_id, preview_type, server, name, path)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (version_id, preview["preview_type"], preview["server"], preview["name"], preview["path"]),
                )

            if set_default or not self._get_default_version_id_conn(conn, post_id):
                self._set_default_post_version_conn(conn, post_id=post_id, version_id=version_id)
            return version_id

    def update_post_version(
        self,
        *,
        version_id: int,
        label: str,
        language: str | None,
        title: str,
        content: str,
        thumbnail_name: str | None,
        thumbnail_remote_url: str | None,
        thumbnail_local_path: str | None,
        published_at: str | None,
        edited_at: str | None,
        next_external_post_id: str | None,
        prev_external_post_id: str | None,
        metadata: dict[str, Any],
        source_url: str | None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.update_post_version(
                    version_id=version_id,
                    label=label,
                    language=language,
                    title=title,
                    content=content,
                    thumbnail_name=thumbnail_name,
                    thumbnail_remote_url=thumbnail_remote_url,
                    thumbnail_local_path=thumbnail_local_path,
                    published_at=published_at,
                    edited_at=edited_at,
                    next_external_post_id=next_external_post_id,
                    prev_external_post_id=prev_external_post_id,
                    metadata=metadata,
                    source_url=source_url,
                    conn=own_conn,
                )

        row = conn.execute("SELECT * FROM post_versions WHERE id = ?", (version_id,)).fetchone()
        if not row:
            return
        next_language = self._normalize_optional_text(language)
        next_thumbnail_name = self._normalize_optional_text(thumbnail_name)
        next_thumbnail_remote_url = self._normalize_optional_text(thumbnail_remote_url)
        next_thumbnail_local_path = self._normalize_optional_text(thumbnail_local_path)
        next_published_at = self._normalize_optional_text(published_at)
        next_edited_at = self._normalize_optional_text(edited_at)
        next_next_external_post_id = self._normalize_optional_text(next_external_post_id)
        next_prev_external_post_id = self._normalize_optional_text(prev_external_post_id)
        next_metadata_json = json.dumps(metadata, ensure_ascii=True)
        next_source_url = self._normalize_optional_text(source_url)
        if (
            str(row["label"]) != self._normalize_required_label(label)
            or self._normalize_optional_text(row["language"]) != next_language
            or str(row["title"]) != title
            or str(row["content"] or "") != content
            or self._normalize_optional_text(row["thumbnail_name"]) != next_thumbnail_name
            or self._normalize_optional_text(row["thumbnail_remote_url"]) != next_thumbnail_remote_url
            or self._normalize_optional_text(row["thumbnail_local_path"]) != next_thumbnail_local_path
            or self._normalize_optional_text(row["published_at"]) != next_published_at
            or self._normalize_optional_text(row["edited_at"]) != next_edited_at
            or self._normalize_optional_text(row["next_external_post_id"]) != next_next_external_post_id
            or self._normalize_optional_text(row["prev_external_post_id"]) != next_prev_external_post_id
            or str(row["metadata_json"]) != next_metadata_json
            or self._normalize_optional_text(row["source_url"]) != next_source_url
        ):
            self._record_post_version_revision_conn(
                conn,
                version_id,
                capture_kind="edit",
                row=row,
            )
        conn.execute(
            """
            UPDATE post_versions
            SET label = ?,
                language = ?,
                title = ?,
                content = ?,
                thumbnail_name = ?,
                thumbnail_remote_url = ?,
                thumbnail_local_path = ?,
                published_at = ?,
                edited_at = ?,
                next_external_post_id = ?,
                prev_external_post_id = ?,
                metadata_json = ?,
                source_url = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                self._normalize_required_label(label),
                next_language,
                title,
                content,
                next_thumbnail_name,
                next_thumbnail_remote_url,
                next_thumbnail_local_path,
                next_published_at,
                next_edited_at,
                next_next_external_post_id,
                next_prev_external_post_id,
                next_metadata_json,
                next_source_url,
                version_id,
            ),
        )
        self._sync_post_from_default_version_conn(conn, int(row["post_id"]))

    def set_default_post_version(
        self,
        post_id: int,
        version_id: int,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is not None:
            self._set_default_post_version_conn(conn, post_id=post_id, version_id=version_id)
            return
        with self._connect() as own_conn:
            self._set_default_post_version_conn(own_conn, post_id=post_id, version_id=version_id)

    def delete_post_version(self, post_id: int, version_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM post_versions WHERE id = ? AND post_id = ?",
                (version_id, post_id),
            ).fetchone()
            if not row:
                return False
            conn.execute(
                "UPDATE post_versions SET derived_from_version_id = NULL WHERE derived_from_version_id = ?",
                (version_id,),
            )
            conn.execute("DELETE FROM post_versions WHERE id = ?", (version_id,))
            fallback = conn.execute(
                """
                SELECT id
                FROM post_versions
                WHERE post_id = ?
                ORDER BY version_rank DESC, id DESC
                LIMIT 1
                """,
                (post_id,),
            ).fetchone()
            if fallback:
                self._set_default_post_version_conn(conn, post_id=post_id, version_id=int(fallback["id"]))
            else:
                conn.execute(
                    """
                    UPDATE posts
                    SET default_version_id = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (post_id,),
                )
                self._sync_post_from_default_version_conn(conn, post_id)
            return True

    def find_post_by_source(self, service: str, external_user_id: str, external_post_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM posts
                WHERE service = ? AND external_user_id = ? AND external_post_id = ?
                LIMIT 1
                """,
                (service, external_user_id, external_post_id),
            ).fetchone()

    def find_version_by_source(
        self,
        *,
        post_id: int,
        service: str,
        external_user_id: str,
        external_post_id: str,
        conn: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        if conn is not None:
            return conn.execute(
                """
                SELECT *
                FROM post_versions
                WHERE post_id = ?
                  AND source_service = ?
                  AND source_user_id = ?
                  AND source_post_id = ?
                LIMIT 1
                """,
                (post_id, service, external_user_id, external_post_id),
            ).fetchone()
        with self._connect() as own_conn:
            return self.find_version_by_source(
                post_id=post_id,
                service=service,
                external_user_id=external_user_id,
                external_post_id=external_post_id,
                conn=own_conn,
            )

    def find_version_by_source_global(
        self,
        *,
        service: str,
        external_user_id: str,
        external_post_id: str,
        conn: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        if conn is not None:
            return conn.execute(
                """
                SELECT v.*,
                       p.creator_id,
                       CASE WHEN p.default_version_id = v.id THEN 1 ELSE 0 END AS is_default
                FROM post_versions v
                JOIN posts p ON p.id = v.post_id
                WHERE v.source_service = ?
                  AND v.source_user_id = ?
                  AND v.source_post_id = ?
                ORDER BY
                    CASE WHEN p.default_version_id = v.id THEN 0 ELSE 1 END ASC,
                    v.version_rank DESC,
                    v.id DESC
                LIMIT 1
                """,
                (service, external_user_id, external_post_id),
            ).fetchone()
        with self._connect() as own_conn:
            return self.find_version_by_source_global(
                service=service,
                external_user_id=external_user_id,
                external_post_id=external_post_id,
                conn=own_conn,
            )

    def replace_attachments(
        self,
        post_id: int,
        attachments: list[dict[str, Any]],
        *,
        version_id: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.replace_attachments(
                    post_id,
                    attachments,
                    version_id=version_id,
                    conn=own_conn,
                )
        resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
        if not resolved_version:
            return
        self._replace_version_attachments_conn(conn, resolved_version, attachments)
        self._sync_post_from_default_version_conn(conn, post_id)

    def list_attachments(self, post_id: int, *, version_id: int | None = None) -> list[sqlite3.Row]:
        with self._connect() as conn:
            resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
            if not resolved_version:
                return []
            rows = conn.execute(
                """
                SELECT *
                FROM post_version_attachments
                WHERE version_id = ?
                ORDER BY id
                """,
                (resolved_version,),
            ).fetchall()
            return list(rows)

    def list_all_attachments_for_post(
        self,
        post_id: int,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> list[sqlite3.Row]:
        if conn is not None:
            rows = conn.execute(
                """
                SELECT a.*
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                WHERE v.post_id = ?
                ORDER BY a.id
                """,
                (post_id,),
            ).fetchall()
            return list(rows)
        with self._connect() as own_conn:
            return self.list_all_attachments_for_post(post_id, conn=own_conn)

    def list_all_attachments_for_posts(self, post_ids: list[int]) -> list[sqlite3.Row]:
        if not post_ids:
            return []
        placeholders = ", ".join("?" for _ in post_ids)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT a.*, v.post_id AS post_id
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                WHERE v.post_id IN ({placeholders})
                ORDER BY v.post_id, a.id
                """,
                post_ids,
            ).fetchall()
            return list(rows)

    def list_attachment_inventory(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    a.id,
                    a.version_id,
                    a.name,
                    a.remote_url,
                    a.local_path,
                    a.kind,
                    a.created_at,
                    v.post_id,
                    v.label AS version_label,
                    v.language AS version_language,
                    v.origin_kind,
                    CASE WHEN p.default_version_id = v.id THEN 1 ELSE 0 END AS is_default_version,
                    p.creator_id,
                    p.series_id,
                    p.title AS post_title,
                    p.published_at AS post_published_at,
                    c.name AS creator_name,
                    s.name AS series_name
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                JOIN posts p ON p.id = v.post_id
                JOIN creators c ON c.id = p.creator_id
                LEFT JOIN series s ON s.id = p.series_id
                ORDER BY
                    p.creator_id ASC,
                    CASE WHEN p.series_id IS NULL THEN 1 ELSE 0 END ASC,
                    p.series_id ASC,
                    p.id DESC,
                    v.version_rank DESC,
                    a.id ASC
                """
            ).fetchall()
            return list(rows)

    def get_attachment_inventory_overview(self) -> sqlite3.Row:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(a.id) AS file_count,
                    COUNT(DISTINCT p.creator_id) AS creator_count,
                    SUM(CASE WHEN a.local_path IS NULL OR TRIM(a.local_path) = '' THEN 1 ELSE 0 END) AS missing_count
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                JOIN posts p ON p.id = v.post_id
                """
            ).fetchone()
            return cast(sqlite3.Row, row)

    def list_attachment_creator_summaries(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.creator_id AS id,
                    c.name AS name,
                    COUNT(a.id) AS file_count,
                    SUM(CASE WHEN a.local_path IS NULL OR TRIM(a.local_path) = '' THEN 1 ELSE 0 END) AS missing_count
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                JOIN posts p ON p.id = v.post_id
                JOIN creators c ON c.id = p.creator_id
                GROUP BY p.creator_id, c.name
                ORDER BY p.creator_id ASC
                """
            ).fetchall()
            return list(rows)

    def update_attachment_local_path(self, attachment_id: int, local_path: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE post_version_attachments
                SET local_path = ?
                WHERE id = ?
                """,
                (local_path, attachment_id),
            )
            row = conn.execute(
                """
                SELECT v.post_id
                FROM post_versions v
                JOIN post_version_attachments a ON a.version_id = v.id
                WHERE a.id = ?
                """,
                (attachment_id,),
            ).fetchone()
            if row:
                self._sync_post_from_default_version_conn(conn, int(row["post_id"]))

    def sync_attachment_local_refs_for_post(
        self,
        post_id: int,
        *,
        old_local_path: str,
        new_local_path: str | None,
        new_name: str | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        if conn is None:
            with self._connect() as own_conn:
                return self.sync_attachment_local_refs_for_post(
                    post_id,
                    old_local_path=old_local_path,
                    new_local_path=new_local_path,
                    new_name=new_name,
                    conn=own_conn,
                )
        if new_name is None:
            cursor = conn.execute(
                """
                UPDATE post_version_attachments
                SET local_path = ?
                WHERE id IN (
                    SELECT a.id
                    FROM post_version_attachments a
                    JOIN post_versions v ON v.id = a.version_id
                    WHERE v.post_id = ? AND a.local_path = ?
                )
                """,
                (new_local_path, post_id, old_local_path),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE post_version_attachments
                SET local_path = ?, name = ?
                WHERE id IN (
                    SELECT a.id
                    FROM post_version_attachments a
                    JOIN post_versions v ON v.id = a.version_id
                    WHERE v.post_id = ? AND a.local_path = ?
                )
                """,
                (new_local_path, new_name, post_id, old_local_path),
            )
        self._sync_post_from_default_version_conn(conn, post_id)
        return int(cursor.rowcount or 0)

    def sync_attachment_name_by_remote_for_post(
        self,
        post_id: int,
        *,
        remote_url: str,
        new_name: str,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        if conn is None:
            with self._connect() as own_conn:
                return self.sync_attachment_name_by_remote_for_post(
                    post_id,
                    remote_url=remote_url,
                    new_name=new_name,
                    conn=own_conn,
                )
        cursor = conn.execute(
            """
            UPDATE post_version_attachments
            SET name = ?
            WHERE id IN (
                SELECT a.id
                FROM post_version_attachments a
                JOIN post_versions v ON v.id = a.version_id
                WHERE v.post_id = ? AND a.remote_url = ?
            )
            """,
            (new_name, post_id, remote_url),
        )
        self._sync_post_from_default_version_conn(conn, post_id)
        return int(cursor.rowcount or 0)

    def update_post_version_content_metadata(
        self,
        *,
        version_id: int,
        content: str,
        metadata: dict[str, Any],
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.update_post_version_content_metadata(
                    version_id=version_id,
                    content=content,
                    metadata=metadata,
                    conn=own_conn,
                )
        row = conn.execute("SELECT * FROM post_versions WHERE id = ?", (version_id,)).fetchone()
        if not row:
            return
        next_metadata_json = json.dumps(metadata, ensure_ascii=True)
        if str(row["content"] or "") != content or str(row["metadata_json"]) != next_metadata_json:
            self._record_post_version_revision_conn(
                conn,
                version_id,
                capture_kind="content_sync",
                row=row,
            )
        conn.execute(
            """
            UPDATE post_versions
            SET content = ?,
                metadata_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                content,
                next_metadata_json,
                version_id,
            ),
        )
        self._sync_post_from_default_version_conn(conn, int(row["post_id"]))
            

    def replace_tags(
        self,
        post_id: int,
        tags: list[str],
        *,
        version_id: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.replace_tags(
                    post_id,
                    tags,
                    version_id=version_id,
                    conn=own_conn,
                )
        resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
        if not resolved_version:
            return
        self._replace_version_tags_conn(conn, resolved_version, tags)

    def list_tags(self, post_id: int, *, version_id: int | None = None) -> list[sqlite3.Row]:
        with self._connect() as conn:
            resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
            if not resolved_version:
                return []
            rows = conn.execute(
                "SELECT * FROM post_version_tags WHERE version_id = ? ORDER BY tag COLLATE NOCASE",
                (resolved_version,),
            ).fetchall()
            return list(rows)

    def replace_previews(
        self,
        post_id: int,
        previews: list[dict],
        *,
        version_id: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.replace_previews(
                    post_id,
                    previews,
                    version_id=version_id,
                    conn=own_conn,
                )
        resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
        if not resolved_version:
            return
        self._replace_version_previews_conn(conn, resolved_version, previews)

    def list_previews(self, post_id: int, *, version_id: int | None = None) -> list[sqlite3.Row]:
        with self._connect() as conn:
            resolved_version = version_id or self._get_default_version_id_conn(conn, post_id)
            if not resolved_version:
                return []
            rows = conn.execute(
                "SELECT * FROM post_version_previews WHERE version_id = ? ORDER BY id",
                (resolved_version,),
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

    def list_posts_for_creator(
        self,
        creator_id: int,
        *,
        series_id: int | None = None,
        unsorted_only: bool = False,
        sort_by: str = "published",
        sort_direction: str = "desc",
        search_text: str = "",
    ) -> list[sqlite3.Row]:
        normalized_sort = sort_by.lower().strip()
        if normalized_sort not in {"published", "title"}:
            normalized_sort = "published"

        normalized_direction = sort_direction.lower().strip()
        if normalized_direction not in {"asc", "desc"}:
            normalized_direction = "desc"
        normalized_search = search_text.strip().lower()

        if normalized_sort == "title":
            order_sql = f"LOWER(p.title) {normalized_direction.upper()}, p.id {normalized_direction.upper()}"
        else:
            # Keep posts with no publish date at the end regardless of direction.
            order_sql = (
                f"CASE WHEN p.published_at IS NULL OR p.published_at = '' THEN 1 ELSE 0 END ASC, "
                f"p.published_at {normalized_direction.upper()}, "
                f"p.id {normalized_direction.upper()}"
            )

        where_clauses = ["p.creator_id = ?"]
        params: list[object] = [creator_id]
        if series_id is not None:
            where_clauses.append("p.series_id = ?")
            params.append(series_id)
        elif unsorted_only:
            where_clauses.append("p.series_id IS NULL")
        if normalized_search:
            like_value = f"%{normalized_search}%"
            where_clauses.append(
                "("
                "LOWER(COALESCE(p.title, '')) LIKE ? "
                "OR LOWER(COALESCE(p.content, '')) LIKE ? "
                "OR LOWER(COALESCE(s.name, '')) LIKE ? "
                "OR LOWER(COALESCE(p.external_post_id, '')) LIKE ?"
                ")"
            )
            params.extend([like_value, like_value, like_value, like_value])

        where_sql = " AND ".join(where_clauses)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT p.*, s.name AS series_name
                FROM posts p
                LEFT JOIN series s ON s.id = p.series_id
                WHERE {where_sql}
                ORDER BY {order_sql}
                """,
                params,
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

    def update_post_series(
        self,
        post_id: int,
        series_id: int | None,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        if conn is None:
            with self._connect() as own_conn:
                return self.update_post_series(post_id, series_id, conn=own_conn)
        conn.execute(
            """
            UPDATE posts
            SET series_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (series_id, post_id),
        )

    def delete_post(self, post_id: int) -> int:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
            return int(cursor.rowcount or 0)

    def update_post(self, post_id: int, title: str, content: str, series_id: int | None) -> None:
        with self._connect() as conn:
            default_version_id = self._get_default_version_id_conn(conn, post_id)
            if not default_version_id:
                raise ValueError("Post has no default version.")
            conn.execute(
                """
                UPDATE posts
                SET series_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (series_id, post_id),
            )
            conn.execute(
                """
                UPDATE post_versions
                SET title = ?, content = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (title, content, default_version_id),
            )
            self._sync_post_from_default_version_conn(conn, post_id)

    def update_post_thumbnail(self, post_id: int, thumbnail_local_path: str | None) -> None:
        with self._connect() as conn:
            default_version_id = self._get_default_version_id_conn(conn, post_id)
            if not default_version_id:
                raise ValueError("Post has no default version.")
            conn.execute(
                """
                UPDATE post_versions
                SET thumbnail_local_path = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (thumbnail_local_path, default_version_id),
            )
            self._sync_post_from_default_version_conn(conn, post_id)

    def find_local_post(
        self, service: str, external_post_id: str, external_user_id: str | None = None
    ) -> sqlite3.Row | None:
        with self._connect() as conn:
            if external_user_id:
                version_match = conn.execute(
                    """
                    SELECT p.id
                    FROM post_versions v
                    JOIN posts p ON p.id = v.post_id
                    WHERE v.source_service = ?
                      AND v.source_user_id = ?
                      AND v.source_post_id = ?
                    ORDER BY
                        CASE WHEN p.default_version_id = v.id THEN 0 ELSE 1 END ASC,
                        v.version_rank DESC,
                        v.id DESC
                    LIMIT 1
                    """,
                    (service, external_user_id, external_post_id),
                ).fetchone()
                if version_match:
                    return version_match
                return conn.execute(
                    """
                    SELECT id
                    FROM posts
                    WHERE service = ? AND external_user_id = ? AND external_post_id = ?
                    """,
                    (service, external_user_id, external_post_id),
                ).fetchone()
            version_match = conn.execute(
                """
                SELECT p.id
                FROM post_versions v
                JOIN posts p ON p.id = v.post_id
                WHERE v.source_service = ?
                  AND v.source_post_id = ?
                ORDER BY
                    CASE WHEN p.default_version_id = v.id THEN 0 ELSE 1 END ASC,
                    v.version_rank DESC,
                    v.id DESC
                LIMIT 1
                """,
                (service, external_post_id),
            ).fetchone()
            if version_match:
                return version_match
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

    def sync_post_from_default_version(self, post_id: int, *, conn: sqlite3.Connection | None = None) -> None:
        if conn is not None:
            self._sync_post_from_default_version_conn(conn, post_id)
            return
        with self._connect() as own_conn:
            self._sync_post_from_default_version_conn(own_conn, post_id)

    def _ensure_version_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS post_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                label TEXT NOT NULL DEFAULT 'Original',
                language TEXT,
                is_manual INTEGER NOT NULL DEFAULT 0,
                origin_kind TEXT NOT NULL DEFAULT 'source',
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
                derived_from_version_id INTEGER,
                version_rank INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                source_url TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (post_id, source_service, source_user_id, source_post_id),
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
                FOREIGN KEY (derived_from_version_id) REFERENCES post_versions(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS post_version_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                remote_url TEXT NOT NULL,
                local_path TEXT,
                kind TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (version_id, remote_url),
                FOREIGN KEY (version_id) REFERENCES post_versions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS post_version_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                UNIQUE (version_id, tag),
                FOREIGN KEY (version_id) REFERENCES post_versions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS post_version_previews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                preview_type TEXT,
                server TEXT NOT NULL DEFAULT '',
                name TEXT,
                path TEXT NOT NULL,
                UNIQUE (version_id, path, server),
                FOREIGN KEY (version_id) REFERENCES post_versions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS post_version_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                revision_number INTEGER NOT NULL,
                capture_kind TEXT NOT NULL DEFAULT 'edit',
                label TEXT NOT NULL,
                language TEXT,
                title TEXT NOT NULL,
                content TEXT,
                thumbnail_name TEXT,
                thumbnail_remote_url TEXT,
                thumbnail_local_path TEXT,
                published_at TEXT,
                edited_at TEXT,
                next_external_post_id TEXT,
                prev_external_post_id TEXT,
                attachments_json TEXT NOT NULL DEFAULT '[]',
                tags_json TEXT NOT NULL DEFAULT '[]',
                previews_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                source_url TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (version_id, revision_number),
                FOREIGN KEY (version_id) REFERENCES post_versions(id) ON DELETE CASCADE
            );
            """
        )
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(post_versions)").fetchall()
        }
        if "derived_from_version_id" not in existing_columns:
            conn.execute("ALTER TABLE post_versions ADD COLUMN derived_from_version_id INTEGER")
        if "version_rank" not in existing_columns:
            conn.execute("ALTER TABLE post_versions ADD COLUMN version_rank INTEGER NOT NULL DEFAULT 0")
        if "origin_kind" not in existing_columns:
            conn.execute(
                f"ALTER TABLE post_versions ADD COLUMN origin_kind TEXT NOT NULL DEFAULT '{self.VERSION_ORIGIN_SOURCE}'"
            )
        revision_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(post_version_revisions)").fetchall()
        }
        if revision_columns and "attachments_json" not in revision_columns:
            conn.execute("ALTER TABLE post_version_revisions ADD COLUMN attachments_json TEXT NOT NULL DEFAULT '[]'")
        if revision_columns and "tags_json" not in revision_columns:
            conn.execute("ALTER TABLE post_version_revisions ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]'")
        if revision_columns and "previews_json" not in revision_columns:
            conn.execute("ALTER TABLE post_version_revisions ADD COLUMN previews_json TEXT NOT NULL DEFAULT '[]'")
        conn.execute(
            """
            UPDATE post_versions
            SET origin_kind = CASE
                WHEN source_service IS NOT NULL AND source_user_id IS NOT NULL AND source_post_id IS NOT NULL
                    THEN 'source'
                WHEN derived_from_version_id IS NOT NULL
                    THEN 'clone'
                ELSE 'manual'
            END
            WHERE origin_kind IS NULL
               OR TRIM(origin_kind) = ''
               OR origin_kind NOT IN ('source', 'clone', 'manual')
            """
        )
        self._ensure_post_version_indexes(conn)

    def _backfill_post_versions(self, conn: sqlite3.Connection) -> None:
        posts = conn.execute("SELECT * FROM posts ORDER BY id").fetchall()
        for post in posts:
            post_id = int(post["id"])
            existing_versions = conn.execute(
                "SELECT id FROM post_versions WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            if existing_versions:
                for index, version_row in enumerate(existing_versions, start=1):
                    conn.execute(
                        "UPDATE post_versions SET version_rank = ? WHERE id = ?",
                        (index, int(version_row["id"])),
                    )
                if not self._get_default_version_id_conn(conn, post_id):
                    default_version_id = int(existing_versions[0]["id"])
                    conn.execute(
                        "UPDATE posts SET default_version_id = ? WHERE id = ?",
                        (default_version_id, post_id),
                    )
                    self._sync_post_from_default_version_conn(conn, post_id)
                continue

            version_id = self._insert_post_version_conn(
                conn=conn,
                post_id=post_id,
                label="Original",
                language=None,
                origin_kind=self.VERSION_ORIGIN_SOURCE,
                source_service=str(post["service"]),
                source_user_id=str(post["external_user_id"]),
                source_post_id=str(post["external_post_id"]),
                title=str(post["title"]),
                content=str(post["content"] or ""),
                metadata=self._safe_json_load(post["metadata_json"]),
                source_url=post["source_url"],
                thumbnail_name=post["thumbnail_name"],
                thumbnail_remote_url=post["thumbnail_remote_url"],
                thumbnail_local_path=post["thumbnail_local_path"],
                published_at=post["published_at"],
                edited_at=post["edited_at"],
                next_external_post_id=post["next_external_post_id"],
                prev_external_post_id=post["prev_external_post_id"],
                derived_from_version_id=None,
            )
            conn.execute("UPDATE posts SET default_version_id = ? WHERE id = ?", (version_id, post_id))

            attachments = conn.execute(
                "SELECT name, remote_url, local_path, kind FROM attachments WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            for item in attachments:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO post_version_attachments (version_id, name, remote_url, local_path, kind)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (version_id, item["name"], item["remote_url"], item["local_path"], item["kind"]),
                )

            tags = conn.execute(
                "SELECT tag FROM post_tags WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            for tag in tags:
                conn.execute(
                    "INSERT OR IGNORE INTO post_version_tags (version_id, tag) VALUES (?, ?)",
                    (version_id, tag["tag"]),
                )

            previews = conn.execute(
                "SELECT preview_type, server, name, path FROM post_previews WHERE post_id = ? ORDER BY id",
                (post_id,),
            ).fetchall()
            for preview in previews:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO post_version_previews (version_id, preview_type, server, name, path)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (version_id, preview["preview_type"], preview["server"], preview["name"], preview["path"]),
                )
            self._sync_post_from_default_version_conn(conn, post_id)

    def _insert_post_version_conn(
        self,
        *,
        conn: sqlite3.Connection,
        post_id: int,
        label: str,
        language: str | None,
        origin_kind: str,
        source_service: str | None,
        source_user_id: str | None,
        source_post_id: str | None,
        title: str,
        content: str,
        metadata: dict[str, Any],
        source_url: str | None,
        thumbnail_name: str | None,
        thumbnail_remote_url: str | None,
        thumbnail_local_path: str | None,
        published_at: str | None,
        edited_at: str | None,
        next_external_post_id: str | None,
        prev_external_post_id: str | None,
        derived_from_version_id: int | None,
        version_rank: int | None = None,
    ) -> int:
        normalized_origin_kind = self._normalize_post_version_origin_kind(origin_kind)
        normalized_source_service = self._normalize_optional_text(source_service)
        normalized_source_user_id = self._normalize_optional_text(source_user_id)
        normalized_source_post_id = self._normalize_optional_text(source_post_id)
        has_any_source = any((normalized_source_service, normalized_source_user_id, normalized_source_post_id))
        has_full_source = all((normalized_source_service, normalized_source_user_id, normalized_source_post_id))
        if has_any_source and not has_full_source:
            raise ValueError("Source tuple must include service, user id, and post id.")
        normalized_derived_from_version_id = int(derived_from_version_id) if derived_from_version_id else None
        if normalized_derived_from_version_id is not None:
            parent_row = conn.execute(
                "SELECT id FROM post_versions WHERE id = ? AND post_id = ?",
                (normalized_derived_from_version_id, post_id),
            ).fetchone()
            if not parent_row:
                raise ValueError("Parent version not found on this post.")
        if normalized_origin_kind == self.VERSION_ORIGIN_SOURCE:
            if not has_full_source:
                raise ValueError("Source versions require service, user id, and post id.")
            source_service = normalized_source_service
            source_user_id = normalized_source_user_id
            source_post_id = normalized_source_post_id
            if source_service is None or source_user_id is None or source_post_id is None:
                raise ValueError("Source versions require service, user id, and post id.")
        else:
            if has_any_source:
                raise ValueError(f"{normalized_origin_kind.title()} versions cannot store a source tuple.")
        if normalized_origin_kind == self.VERSION_ORIGIN_CLONE and normalized_derived_from_version_id is None:
            raise ValueError("Clone versions require a parent version.")
        if normalized_origin_kind == self.VERSION_ORIGIN_SOURCE:
            existing_version = self.find_version_by_source_global(
                service=cast(str, source_service),
                external_user_id=cast(str, source_user_id),
                external_post_id=cast(str, source_post_id),
                conn=conn,
            )
            if existing_version:
                raise ValueError(
                    "This source version already exists locally under "
                    f"post #{int(existing_version['post_id'])} as version #{int(existing_version['id'])}."
                )
        resolved_version_rank = version_rank or self._next_post_version_rank_conn(conn, post_id)
        cursor = conn.execute(
            """
            INSERT INTO post_versions (
                post_id,
                label,
                language,
                is_manual,
                origin_kind,
                source_service,
                source_user_id,
                source_post_id,
                title,
                content,
                thumbnail_name,
                thumbnail_remote_url,
                thumbnail_local_path,
                published_at,
                edited_at,
                next_external_post_id,
                prev_external_post_id,
                derived_from_version_id,
                version_rank,
                metadata_json,
                source_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                post_id,
                self._normalize_required_label(label),
                self._normalize_optional_text(language),
                0 if normalized_origin_kind == self.VERSION_ORIGIN_SOURCE else 1,
                normalized_origin_kind,
                normalized_source_service,
                normalized_source_user_id,
                normalized_source_post_id,
                title,
                content,
                self._normalize_optional_text(thumbnail_name),
                self._normalize_optional_text(thumbnail_remote_url),
                self._normalize_optional_text(thumbnail_local_path),
                self._normalize_optional_text(published_at),
                self._normalize_optional_text(edited_at),
                self._normalize_optional_text(next_external_post_id),
                self._normalize_optional_text(prev_external_post_id),
                normalized_derived_from_version_id,
                resolved_version_rank,
                json.dumps(metadata, ensure_ascii=True),
                self._normalize_optional_text(source_url),
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("Failed to create post version row.")
        return int(cursor.lastrowid)

    def _replace_version_attachments_conn(
        self,
        conn: sqlite3.Connection,
        version_id: int,
        attachments: list[dict[str, Any]],
    ) -> None:
        conn.execute("DELETE FROM post_version_attachments WHERE version_id = ?", (version_id,))
        for attachment in attachments:
            conn.execute(
                """
                INSERT INTO post_version_attachments (version_id, name, remote_url, local_path, kind)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    attachment["name"],
                    attachment["remote_url"],
                    attachment.get("local_path"),
                    attachment["kind"],
                ),
            )

    def _replace_version_tags_conn(self, conn: sqlite3.Connection, version_id: int, tags: list[str]) -> None:
        conn.execute("DELETE FROM post_version_tags WHERE version_id = ?", (version_id,))
        deduped: list[str] = []
        seen: set[str] = set()
        for tag in tags:
            normalized = tag.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        for tag in deduped:
            conn.execute(
                "INSERT INTO post_version_tags (version_id, tag) VALUES (?, ?)",
                (version_id, tag),
            )

    def _replace_version_previews_conn(
        self,
        conn: sqlite3.Connection,
        version_id: int,
        previews: list[dict[str, Any]],
    ) -> None:
        conn.execute("DELETE FROM post_version_previews WHERE version_id = ?", (version_id,))
        seen_keys: set[tuple[str, str]] = set()
        for preview in previews:
            path = str(preview.get("path", "")).strip()
            if not path:
                continue
            server = str(preview.get("server", "")).strip()
            key = (server, path)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            conn.execute(
                """
                INSERT OR IGNORE INTO post_version_previews (version_id, preview_type, server, name, path)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    str(preview.get("type", "")).strip() or None,
                    server,
                    str(preview.get("name", "")).strip() or None,
                    path,
                ),
            )

    def _list_version_attachments_snapshot_conn(
        self,
        conn: sqlite3.Connection,
        version_id: int,
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT name, remote_url, local_path, kind
            FROM post_version_attachments
            WHERE version_id = ?
            ORDER BY id
            """,
            (version_id,),
        ).fetchall()
        return [
            {
                "name": str(row["name"]),
                "remote_url": str(row["remote_url"]),
                "local_path": self._normalize_optional_text(row["local_path"]),
                "kind": str(row["kind"]),
            }
            for row in rows
        ]

    def _list_version_tags_snapshot_conn(self, conn: sqlite3.Connection, version_id: int) -> list[str]:
        rows = conn.execute(
            """
            SELECT tag
            FROM post_version_tags
            WHERE version_id = ?
            ORDER BY tag COLLATE NOCASE, id
            """,
            (version_id,),
        ).fetchall()
        return [str(row["tag"]) for row in rows]

    def _list_version_previews_snapshot_conn(
        self,
        conn: sqlite3.Connection,
        version_id: int,
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT preview_type, server, name, path
            FROM post_version_previews
            WHERE version_id = ?
            ORDER BY id
            """,
            (version_id,),
        ).fetchall()
        return [
            {
                "type": self._normalize_optional_text(row["preview_type"]),
                "server": str(row["server"]),
                "name": self._normalize_optional_text(row["name"]),
                "path": str(row["path"]),
            }
            for row in rows
        ]

    def _record_post_version_revision_conn(
        self,
        conn: sqlite3.Connection,
        version_id: int,
        *,
        capture_kind: str,
        row: sqlite3.Row | None = None,
    ) -> None:
        snapshot = row or conn.execute(
            "SELECT * FROM post_versions WHERE id = ?",
            (version_id,),
        ).fetchone()
        if not snapshot:
            return
        conn.execute(
            """
            INSERT INTO post_version_revisions (
                version_id,
                revision_number,
                capture_kind,
                label,
                language,
                title,
                content,
                thumbnail_name,
                thumbnail_remote_url,
                thumbnail_local_path,
                published_at,
                edited_at,
                next_external_post_id,
                prev_external_post_id,
                attachments_json,
                tags_json,
                previews_json,
                metadata_json,
                source_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                self._next_post_version_revision_number_conn(conn, version_id),
                capture_kind.strip().lower() or "edit",
                self._normalize_required_label(str(snapshot["label"])),
                self._normalize_optional_text(snapshot["language"]),
                str(snapshot["title"]),
                snapshot["content"],
                self._normalize_optional_text(snapshot["thumbnail_name"]),
                self._normalize_optional_text(snapshot["thumbnail_remote_url"]),
                self._normalize_optional_text(snapshot["thumbnail_local_path"]),
                self._normalize_optional_text(snapshot["published_at"]),
                self._normalize_optional_text(snapshot["edited_at"]),
                self._normalize_optional_text(snapshot["next_external_post_id"]),
                self._normalize_optional_text(snapshot["prev_external_post_id"]),
                json.dumps(self._list_version_attachments_snapshot_conn(conn, version_id), ensure_ascii=True),
                json.dumps(self._list_version_tags_snapshot_conn(conn, version_id), ensure_ascii=True),
                json.dumps(self._list_version_previews_snapshot_conn(conn, version_id), ensure_ascii=True),
                str(snapshot["metadata_json"]),
                self._normalize_optional_text(snapshot["source_url"]),
            ),
        )

    def _get_default_version_id_conn(self, conn: sqlite3.Connection, post_id: int) -> int | None:
        row = conn.execute(
            "SELECT default_version_id FROM posts WHERE id = ?",
            (post_id,),
        ).fetchone()
        if not row:
            return None
        return int(row["default_version_id"]) if row["default_version_id"] is not None else None

    def _next_post_version_rank_conn(self, conn: sqlite3.Connection, post_id: int) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(version_rank), 0) AS max_rank FROM post_versions WHERE post_id = ?",
            (post_id,),
        ).fetchone()
        if not row:
            return 1
        return int(row["max_rank"] or 0) + 1

    def _next_post_version_revision_number_conn(self, conn: sqlite3.Connection, version_id: int) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(revision_number), 0) AS max_revision FROM post_version_revisions WHERE version_id = ?",
            (version_id,),
        ).fetchone()
        if not row:
            return 1
        return int(row["max_revision"] or 0) + 1

    def _ensure_post_version_indexes(self, conn: sqlite3.Connection) -> None:
        existing_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(post_versions)").fetchall()
        }
        unique_source_index = "ux_post_versions_global_source"
        lookup_source_index = "ix_post_versions_source_lookup"
        if unique_source_index not in existing_indexes and lookup_source_index not in existing_indexes:
            duplicate = conn.execute(
                """
                SELECT source_service, source_user_id, source_post_id
                FROM post_versions
                WHERE source_service IS NOT NULL
                  AND source_user_id IS NOT NULL
                  AND source_post_id IS NOT NULL
                GROUP BY source_service, source_user_id, source_post_id
                HAVING COUNT(*) > 1
                LIMIT 1
                """
            ).fetchone()
            if duplicate:
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS ix_post_versions_source_lookup
                    ON post_versions (source_service, source_user_id, source_post_id)
                    """
                )
            else:
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_post_versions_global_source
                    ON post_versions (source_service, source_user_id, source_post_id)
                    WHERE source_service IS NOT NULL
                      AND source_user_id IS NOT NULL
                      AND source_post_id IS NOT NULL
                    """
                )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_post_versions_derived_from
            ON post_versions (derived_from_version_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_post_version_revisions_version
            ON post_version_revisions (version_id, revision_number DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_posts_inventory_tree
            ON posts (creator_id, series_id, id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_post_versions_inventory_tree
            ON post_versions (post_id, version_rank DESC, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_post_version_attachments_inventory_tree
            ON post_version_attachments (version_id, id)
            """
        )

    def _set_default_post_version_conn(self, conn: sqlite3.Connection, *, post_id: int, version_id: int) -> None:
        row = conn.execute(
            "SELECT id FROM post_versions WHERE id = ? AND post_id = ?",
            (version_id, post_id),
        ).fetchone()
        if not row:
            raise ValueError("Version does not belong to this post.")
        conn.execute(
            "UPDATE posts SET default_version_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (version_id, post_id),
        )
        self._sync_post_from_default_version_conn(conn, post_id)

    def _clear_post_projection_conn(self, conn: sqlite3.Connection, post_id: int) -> None:
        conn.execute(
            """
            UPDATE posts
            SET title = '',
                content = NULL,
                thumbnail_name = NULL,
                thumbnail_remote_url = NULL,
                thumbnail_local_path = NULL,
                published_at = NULL,
                edited_at = NULL,
                next_external_post_id = NULL,
                prev_external_post_id = NULL,
                metadata_json = '{}',
                source_url = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (post_id,),
        )

    def _sync_post_from_default_version_conn(self, conn: sqlite3.Connection, post_id: int) -> None:
        row = conn.execute(
            """
            SELECT v.*
            FROM posts p
            JOIN post_versions v ON v.id = p.default_version_id
            WHERE p.id = ?
            """,
            (post_id,),
        ).fetchone()
        if not row:
            self._clear_post_projection_conn(conn, post_id)
            return
        conn.execute(
            """
            UPDATE posts
            SET title = ?,
                content = ?,
                thumbnail_name = ?,
                thumbnail_remote_url = ?,
                thumbnail_local_path = ?,
                published_at = ?,
                edited_at = ?,
                next_external_post_id = ?,
                prev_external_post_id = ?,
                metadata_json = ?,
                source_url = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                row["title"],
                row["content"],
                row["thumbnail_name"],
                row["thumbnail_remote_url"],
                row["thumbnail_local_path"],
                row["published_at"],
                row["edited_at"],
                row["next_external_post_id"],
                row["prev_external_post_id"],
                row["metadata_json"],
                self._normalize_optional_text(row["source_url"]) or "",
                post_id,
            ),
        )

    @staticmethod
    def _safe_json_load(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def _normalize_post_version_origin_kind(self, origin_kind: str) -> str:
        normalized = str(origin_kind or "").strip().lower()
        if normalized not in self.VERSION_ORIGIN_KINDS:
            allowed = ", ".join(sorted(self.VERSION_ORIGIN_KINDS))
            raise ValueError(f"Invalid version origin kind '{origin_kind}'. Expected one of: {allowed}.")
        return normalized

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
            "default_version_id": "INTEGER",
        }
        for column, column_type in required.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE posts ADD COLUMN {column} {column_type}")

    def _ensure_creator_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(creators)").fetchall()
        }
        required = {
            "icon_remote_url": "TEXT",
            "icon_local_path": "TEXT",
            "description": "TEXT",
            "tags_text": "TEXT",
        }
        for column, column_type in required.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE creators ADD COLUMN {column} {column_type}")

    def _ensure_series_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(series)").fetchall()
        }
        required = {
            "description": "TEXT",
            "tags_text": "TEXT",
            "default_sort_by": "TEXT",
            "default_sort_direction": "TEXT",
            "cover_post_id": "INTEGER",
        }
        for column, column_type in required.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE series ADD COLUMN {column} {column_type}")

    @staticmethod
    def _normalize_optional_text(value: str | None) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _normalize_required_label(value: str | None) -> str:
        if not isinstance(value, str):
            return "Version"
        cleaned = value.strip()
        return cleaned or "Version"

    @staticmethod
    def _normalize_series_sort_by(value: str | None) -> str:
        if not isinstance(value, str):
            return "published"
        normalized = value.strip().lower()
        if normalized not in {"published", "title"}:
            return "published"
        return normalized

    @staticmethod
    def _normalize_series_sort_direction(value: str | None) -> str:
        if not isinstance(value, str):
            return "desc"
        normalized = value.strip().lower()
        if normalized not in {"asc", "desc"}:
            return "desc"
        return normalized
