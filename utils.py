from typing import Iterator
from pathlib import Path


def file_order_generator() -> Iterator[str]:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    for first in alphabet:
        for second in alphabet:
            for third in alphabet:
                yield first + second + third


def clear_dir(directory: Path) -> None:
    if directory.exists():
        for file in directory.rglob("*"):
            if file.is_file():
                file.unlink()
