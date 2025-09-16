from __future__ import annotations

from nl2sql.service.inference import _format_schema_summary


def test_format_schema_summary_detailed() -> None:
    artifact = {
        "tables": [
            {
                "name": "authors",
                "type": "table",
                "columns": [
                    {"name": "id", "data_type": "INTEGER", "primary_key": True, "nullable": False},
                    {"name": "name", "data_type": "TEXT", "nullable": False},
                ],
                "foreign_keys": [],
            },
            {
                "name": "books",
                "type": "table",
                "columns": [
                    {"name": "id", "data_type": "INTEGER", "primary_key": True, "nullable": False},
                    {"name": "author_id", "data_type": "INTEGER", "nullable": False},
                    {"name": "title", "data_type": "TEXT", "nullable": True},
                ],
                "foreign_keys": [
                    {
                        "columns": ["author_id"],
                        "references": {"table": "authors", "columns": ["id"]},
                        "on_delete": "CASCADE",
                    }
                ],
            },
        ],
        "relationships": [
            {
                "from": {"table": "books", "columns": ["author_id"]},
                "to": {"table": "authors", "columns": ["id"]},
                "on_delete": "CASCADE",
            }
        ],
    }

    summary = _format_schema_summary(artifact)
    assert summary is not None
    assert "Tables:" in summary
    assert "authors" in summary and "name TEXT" in summary
    assert "books" in summary and "author_id" in summary
    assert "Relationships:" in summary
    assert "books(author_id) -> authors(id)" in summary


def test_format_schema_summary_legacy() -> None:
    artifact = {
        "tables": [
            {"table_name": "users"},
            {"table_name": "orders"},
        ]
    }

    summary = _format_schema_summary(artifact)
    assert summary == "- users\n- orders"
