"""本地文件存储原语：原子写、文件锁、JSONL。"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
from collections.abc import Iterator

from .errors import LakehouseError


class FileStorage:
    """以目录为根的本地存储，提供原子写与跨进程互斥锁。"""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)

    def ensure_dir(self, relative: str = "") -> str:
        """确保目录存在并返回绝对路径。"""
        target = self.path(relative)
        os.makedirs(target, exist_ok=True)
        return target

    def path(self, relative: str) -> str:
        """拼接根目录下的相对路径。"""
        if not relative:
            return self.root
        return os.path.join(self.root, relative)

    def exists(self, relative: str) -> bool:
        return os.path.exists(self.path(relative))

    def atomic_write_bytes(self, relative: str, data: bytes) -> None:
        """同目录临时文件 + os.replace 原子落盘（并 fsync 文件与目录）。

        保证读者要么看到旧文件、要么看到新文件，绝不会读到半个文件。
        """
        target = self.path(relative)
        parent = os.path.dirname(target) or "."
        os.makedirs(parent, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{os.path.basename(target)}.", suffix=".tmp", dir=parent
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, target)
            self._fsync_dir(parent)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    def atomic_write_text(self, relative: str, text: str) -> None:
        self.atomic_write_bytes(relative, text.encode("utf-8"))

    def write_json(self, relative: str, obj: object) -> None:
        """原子写入 JSON（带换行、键稳定排序）。"""
        text = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        self.atomic_write_text(relative, text)

    def read_json(self, relative: str) -> object:
        with open(self.path(relative), encoding="utf-8") as f:
            return json.load(f)

    def write_jsonl(self, relative: str, rows: list[dict]) -> None:
        """原子写入 JSON Lines 文件。"""
        lines = [json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows]
        payload = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
        self.atomic_write_bytes(relative, payload)

    def read_jsonl(self, relative: str) -> list[dict]:
        """读取 JSON Lines 文件；文件不存在返回空列表。"""
        if not self.exists(relative):
            return []
        rows: list[dict] = []
        with open(self.path(relative), encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise LakehouseError(
                        f"corrupt data file {relative}:{lineno}: {exc}"
                    ) from exc
        return rows

    def remove(self, relative: str) -> None:
        """删除文件；不存在视为成功。"""
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path(relative))

    @contextlib.contextmanager
    def lock(self, name: str = "commit.lock") -> Iterator[None]:
        """跨线程/跨进程互斥锁（fcntl.flock，排他）。

        每次都以独立 fd 打开锁文件：同一进程内的不同线程也会因此互斥。
        """
        lock_dir = self.ensure_dir("")
        lock_path = os.path.join(lock_dir, name)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def list_dir(self, relative: str) -> list[str]:
        target = self.path(relative)
        if not os.path.isdir(target):
            return []
        return sorted(os.listdir(target))

    @staticmethod
    def _fsync_dir(dir_path: str) -> None:
        # 目录 fsync，保证 rename 元数据落盘
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


__all__ = ["FileStorage"]
