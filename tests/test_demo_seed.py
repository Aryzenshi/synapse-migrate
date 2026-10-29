from __future__ import annotations

from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


def test_demo_fixtures_are_repeatable_and_non_destructive() -> None:
    mysql_seed = (PROJECT / "tests/fixtures/mysql_sample.sql").read_text(encoding="utf-8").upper()
    mongo_seed = (PROJECT / "scripts/seed_mongo.js").read_text(encoding="utf-8")

    assert "DROP TABLE" not in mysql_seed
    assert mysql_seed.count("CREATE TABLE IF NOT EXISTS") == 5
    assert mysql_seed.count("INSERT IGNORE INTO") == 5
    assert "DELETE(" not in mongo_seed
    assert "countDocuments() === 0" in mongo_seed
    assert "insertMany" in mongo_seed
