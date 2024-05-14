"""Module containing utility functions for other modules."""

from collections.abc import Iterator
from pathlib import Path


def file_order_generator() -> Iterator[str]:
    """Generate a three-letter string in alphabetical order."""
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    for first in alphabet:
        for second in alphabet:
            for third in alphabet:
                yield first + second + third


def clear_dir(directory: Path) -> None:
    """Remove all files in the given directory."""
    if directory.exists():
        for file in directory.rglob("*"):
            if file.is_file():
                file.unlink()
