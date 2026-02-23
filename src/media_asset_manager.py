"""
BR Media Asset Manager - Digital media asset organization and tagging system.
SQLite persistence at ~/.blackroad/media_assets.db
"""
import argparse
import csv
import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

GREEN = "\033[0;32m"
RED = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
BLUE = "\033[0;34m"
BOLD = "\033[1m"
RESET = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "media_assets.db"


@dataclass
class Tag:
    id: Optional[int]
    name: str
    color: str = "cyan"


@dataclass
class MediaAsset:
    id: Optional[int]
    name: str
    file_path: str
    asset_type: str
    size_bytes: int
    tags: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())


def _fmt_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}PB"


def _print_assets(assets: List[dict]) -> None:
    if not assets:
        print(f"{YELLOW}No assets found.{RESET}")
        return
    header = f"{BOLD}{CYAN}{'ID':<5} {'Name':<30} {'Type':<12} {'Size':<10} {'Tags':<25} Created{RESET}"
    print(header)
    print(f"{CYAN}{'-'*95}{RESET}")
    for a in assets:
        tags_str = ", ".join(a.get("tags", [])) or "-"
        created = a.get("created_at", "")[:10]
        print(
            f"{GREEN}{a['id']:<5}{RESET} {a['name']:<30} {YELLOW}{a['asset_type']:<12}{RESET} "
            f"{BLUE}{_fmt_size(a['size_bytes']):<10}{RESET} {CYAN}{tags_str:<25}{RESET} {created}"
        )


class MediaAssetManager:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    asset_type TEXT DEFAULT 'unknown',
                    size_bytes INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    color TEXT DEFAULT 'cyan'
                );
                CREATE TABLE IF NOT EXISTS asset_tags (
                    asset_id INTEGER REFERENCES assets(id) ON DELETE CASCADE,
                    tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
                    PRIMARY KEY (asset_id, tag_id)
                );
            """)

    def _get_or_create_tag(self, conn: sqlite3.Connection, tag_name: str) -> int:
        row = conn.execute("SELECT id FROM tags WHERE name=?", (tag_name,)).fetchone()
        if row:
            return row["id"]
        cur = conn.execute("INSERT INTO tags (name) VALUES (?)", (tag_name,))
        return cur.lastrowid

    def _load_tags(self, conn: sqlite3.Connection, asset_id: int) -> List[str]:
        rows = conn.execute(
            "SELECT t.name FROM tags t JOIN asset_tags at2 ON t.id=at2.tag_id WHERE at2.asset_id=?",
            (asset_id,),
        ).fetchall()
        return [r["name"] for r in rows]

    def add_asset(self, name: str, file_path: str, asset_type: str = "unknown",
                  size_bytes: int = 0, tags: Optional[List[str]] = None) -> MediaAsset:
        now = datetime.now().isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO assets (name, file_path, asset_type, size_bytes, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?)",
                (name, file_path, asset_type, size_bytes, now, now),
            )
            asset_id = cur.lastrowid
            for tag in (tags or []):
                tid = self._get_or_create_tag(conn, tag.strip())
                conn.execute("INSERT OR IGNORE INTO asset_tags (asset_id, tag_id) VALUES (?,?)", (asset_id, tid))
        return MediaAsset(id=asset_id, name=name, file_path=file_path, asset_type=asset_type,
                          size_bytes=size_bytes, tags=tags or [], created_at=now, updated_at=now)

    def list_assets(self, asset_type: Optional[str] = None, tag: Optional[str] = None) -> List[dict]:
        with self._conn() as conn:
            query = "SELECT DISTINCT a.* FROM assets a"
            params: list = []
            if tag:
                query += " JOIN asset_tags at2 ON a.id=at2.asset_id JOIN tags t ON at2.tag_id=t.id"
            query += " WHERE 1=1"
            if asset_type:
                query += " AND a.asset_type=?"
                params.append(asset_type)
            if tag:
                query += " AND t.name=?"
                params.append(tag)
            rows = conn.execute(query, params).fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["tags"] = self._load_tags(conn, row["id"])
                result.append(d)
            return result

    def tag_asset(self, asset_id: int, tags: List[str]) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT id FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not row:
                return False
            for tag in tags:
                tid = self._get_or_create_tag(conn, tag.strip())
                conn.execute("INSERT OR IGNORE INTO asset_tags (asset_id, tag_id) VALUES (?,?)", (asset_id, tid))
            conn.execute("UPDATE assets SET updated_at=? WHERE id=?", (datetime.now().isoformat(), asset_id))
        return True

    def get_status(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) as c FROM assets").fetchone()["c"]
            types = conn.execute("SELECT asset_type, COUNT(*) as c FROM assets GROUP BY asset_type").fetchall()
            tags_count = conn.execute("SELECT COUNT(*) as c FROM tags").fetchone()["c"]
            size_row = conn.execute("SELECT SUM(size_bytes) as s FROM assets").fetchone()
            total_size = size_row["s"] or 0
        return {"total_assets": total, "total_tags": tags_count,
                "total_size": total_size, "by_type": {r["asset_type"]: r["c"] for r in types}}

    def export(self, output_path: str, fmt: str = "json") -> None:
        assets = self.list_assets()
        if fmt == "json":
            with open(output_path, "w") as f:
                json.dump(assets, f, indent=2)
        else:
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "name", "file_path", "asset_type", "size_bytes",
                                                        "tags", "created_at", "updated_at"])
                writer.writeheader()
                for a in assets:
                    a["tags"] = "|".join(a["tags"])
                    writer.writerow(a)


def main():
    parser = argparse.ArgumentParser(description="BR Media Asset Manager")
    sub = parser.add_subparsers(dest="cmd")

    p_list = sub.add_parser("list", help="List assets")
    p_list.add_argument("--type", dest="asset_type", help="Filter by type")
    p_list.add_argument("--tag", help="Filter by tag")

    p_add = sub.add_parser("add", help="Add asset")
    p_add.add_argument("name")
    p_add.add_argument("file_path")
    p_add.add_argument("--type", dest="asset_type", default="unknown")
    p_add.add_argument("--size", dest="size_bytes", type=int, default=0)
    p_add.add_argument("--tags", nargs="+", default=[])

    sub.add_parser("status", help="Show system status")

    p_exp = sub.add_parser("export", help="Export assets")
    p_exp.add_argument("output")
    p_exp.add_argument("--format", dest="fmt", choices=["json", "csv"], default="json")

    p_tag = sub.add_parser("tag", help="Tag an asset")
    p_tag.add_argument("asset_id", type=int)
    p_tag.add_argument("tags", nargs="+")

    args = parser.parse_args()
    mgr = MediaAssetManager()

    if args.cmd == "list":
        assets = mgr.list_assets(asset_type=args.asset_type, tag=args.tag)
        _print_assets(assets)
    elif args.cmd == "add":
        asset = mgr.add_asset(args.name, args.file_path, args.asset_type, args.size_bytes, args.tags)
        print(f"{GREEN}✓ Added asset '{asset.name}' (ID: {asset.id}){RESET}")
    elif args.cmd == "status":
        s = mgr.get_status()
        print(f"{BOLD}{CYAN}Media Asset Manager Status{RESET}")
        print(f"  {BLUE}Total Assets :{RESET} {GREEN}{s['total_assets']}{RESET}")
        print(f"  {BLUE}Total Tags   :{RESET} {GREEN}{s['total_tags']}{RESET}")
        print(f"  {BLUE}Total Size   :{RESET} {GREEN}{_fmt_size(s['total_size'])}{RESET}")
        for t, c in s["by_type"].items():
            print(f"    {YELLOW}{t:<15}{RESET} {c}")
    elif args.cmd == "export":
        mgr.export(args.output, args.fmt)
        print(f"{GREEN}✓ Exported to {args.output}{RESET}")
    elif args.cmd == "tag":
        ok = mgr.tag_asset(args.asset_id, args.tags)
        if ok:
            print(f"{GREEN}✓ Tagged asset {args.asset_id} with: {', '.join(args.tags)}{RESET}")
        else:
            print(f"{RED}✗ Asset {args.asset_id} not found{RESET}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
