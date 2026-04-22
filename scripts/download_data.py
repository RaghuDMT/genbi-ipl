from __future__ import annotations

from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve
import zipfile


RAW_DIR = Path("data/raw")

# Each tournament is a separate Cricsheet competition with its own zip.
# IPL is men's-only; WPL is the women's competition (started 2023).
TOURNAMENTS: list[dict[str, object]] = [
    {
        "name": "IPL",
        "url": "https://cricsheet.org/downloads/ipl_male_json.zip",
        "zip_path": RAW_DIR / "ipl_male_json.zip",
        "extract_dir": RAW_DIR / "ipl_json_men",
    },
    {
        "name": "WPL",
        "url": "https://cricsheet.org/downloads/wpl_female_json.zip",
        "zip_path": RAW_DIR / "wpl_female_json.zip",
        "extract_dir": RAW_DIR / "wpl_json_women",
    },
]


def download_file(url: str, destination: Path) -> bool:
    """Download a URL to disk. Returns True on success, False on failure.

    Partial failure is tolerated so one broken URL doesn't block the rest.
    """
    print(f"Downloading: {url}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        urlretrieve(url, destination)
    except (HTTPError, URLError) as exc:
        print(f"  FAILED: {exc}")
        return False
    print(f"  Saved: {destination}")
    return True


def extract_zip(archive_path: Path, target_dir: Path) -> None:
    print(f"Extracting: {archive_path} -> {target_dir}")
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(target_dir)


def count_json_files(target_dir: Path) -> int:
    return sum(1 for path in target_dir.rglob("*.json") if path.is_file())


def main() -> None:
    results: list[tuple[str, int]] = []

    for tournament in TOURNAMENTS:
        name = tournament["name"]
        url = tournament["url"]
        zip_path = tournament["zip_path"]
        extract_dir = tournament["extract_dir"]

        if not download_file(url, zip_path):
            print(f"Skipping {name} (download failed)")
            continue

        extract_zip(zip_path, extract_dir)
        count = count_json_files(extract_dir)
        results.append((name, count))

    print()
    print("Summary:")
    total = 0
    for name, count in results:
        print(f"  {name}: {count} JSON files")
        total += count
    print(f"  Total: {total} JSON files")


if __name__ == "__main__":
    main()