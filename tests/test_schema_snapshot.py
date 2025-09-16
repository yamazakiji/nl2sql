from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from nl2sql.jobs.tasks import _generate_sqlite_schema


@pytest.mark.asyncio
async def test_generate_sqlite_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "schema.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE authors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE books (
                id INTEGER PRIMARY KEY,
                author_id INTEGER NOT NULL,
                title TEXT,
                FOREIGN KEY(author_id) REFERENCES authors(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX idx_authors_name ON authors(name);
            CREATE INDEX idx_books_author ON books(author_id);
            """
        )

    payload = await _generate_sqlite_schema(f"sqlite+aiosqlite:///{db_path}")

    assert payload["database"]["dialect"] == "sqlite"
    assert payload["relationships"], "Expected at least one relationship in the schema"

    tables = {table["name"]: table for table in payload["tables"]}
    assert {"authors", "books"}.issubset(tables)

    authors = tables["authors"]
    assert authors["primary_key"] == ["id"]
    assert any(not column["nullable"] for column in authors["columns"] if column["name"] == "name")
    author_indexes = {index["name"]: index for index in authors["indexes"]}
    assert author_indexes["idx_authors_name"]["unique"] is True

    books = tables["books"]
    assert any(column["name"] == "author_id" for column in books["columns"])
    book_indexes = {index["name"]: index for index in books["indexes"]}
    assert book_indexes["idx_books_author"]["columns"] == ["author_id"]
    assert books["foreign_keys"], "Expected foreign keys for books table"
    book_fk = books["foreign_keys"][0]
    assert book_fk["columns"] == ["author_id"]
    assert book_fk["references"]["table"] == "authors"
    assert book_fk["references"]["columns"] == ["id"]
    assert book_fk["on_delete"] == "CASCADE"

    relationship_targets = {
        (rel["from"]["table"], rel["to"]["table"])
        for rel in payload["relationships"]
    }
    assert ("books", "authors") in relationship_targets
