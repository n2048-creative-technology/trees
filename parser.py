#!/usr/bin/env python3
import requests
import csv
import os
import sys
import argparse
from time import sleep

# ------------ Settings ------------
CSV_PATH = "amsterdam_trees.csv"
REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_CALLS = 0.1
RELOAD_EXISTING_EVERY = 500   # refresh in-memory IDs every N iterations to notice other writers

FIELDNAMES = [
    "id", "latitude","longitude", "soortnaam", "soortnaamtop", "jaarvanaanleg",
    "typeobject", "typeeigenaarplus", "typebeheerderplus", "boomhoogteklasseactueel"
]

# ------------ Cross-platform file locking ------------
_ON_WINDOWS = os.name == "nt"
if _ON_WINDOWS:
    import msvcrt
else:
    import fcntl

def lock_file(f, exclusive=True):
    """Lock an open file handle. Exclusive for write, shared for read (POSIX only)."""
    if _ON_WINDOWS:
        # Windows msvcrt: only exclusive-style locks; lock 1 byte from start
        # (CSV is append-only for our use; this suffices to serialize writers)
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)

def unlock_file(f):
    if _ON_WINDOWS:
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

# ------------ Helpers ------------

def load_existing_ids(csv_path: str) -> set:
    """Read the existing CSV (if any) and return a set of IDs already stored."""
    existing_ids = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return existing_ids

    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        # On POSIX we could take a shared lock; not strictly needed for read-only
        # since we re-check under an exclusive lock before writing.
        reader = csv.DictReader(f)
        if reader.fieldnames:
            for row in reader:
                try:
                    rid = row.get("id")
                    if rid:
                        existing_ids.add(int(rid))
                except (ValueError, TypeError):
                    continue
    return existing_ids


def ensure_csv_has_header(csv_path: str):
    """Create the CSV with header if it doesn't exist or is empty."""
    needs_header = (not os.path.exists(csv_path)) or (os.path.getsize(csv_path) == 0)
    if needs_header:
        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def parse_tree(tree_id: int):
    print(f"Retrieving data for {tree_id}")

    url = f"https://bomen.amsterdam.nl/features.data?type=tree&id={tree_id}&filters=&_routes=routes%2Ffeatures"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://bomen.amsterdam.nl/",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        def find_value(key):
            try:
                return data[data.index(key) + 1]
            except ValueError:
                return None
            except Exception:
                return None

        try:
            lat = data[data.index('coordinates') + 2]
            lon = data[data.index('coordinates') + 3]
        except Exception:
            lat, lon = None, None

        tree_info = {
            "id": find_value("id"),
            "latitude": lat,
            "longitude": lon,
            "soortnaam": str(find_value('soortnaam')),
            "soortnaamtop": str(find_value('soortnaamtop')),
            "jaarvanaanleg": find_value('jaarvanaanleg'),
            "typeobject": str(find_value('typeobject')),
            "typeeigenaarplus": str(find_value('typeeigenaarplus')),
            "typebeheerderplus": str(find_value('typebeheerderplus')),
            "boomhoogteklasseactueel": str(find_value('boomhoogteklasseactueel')),
        }

        if tree_info["id"] is None:
            return None
        try:
            tree_info["id"] = int(tree_info["id"])
        except (ValueError, TypeError):
            return None

        print(f"OK: id={tree_info['id']} lat={tree_info['latitude']} lon={tree_info['longitude']}")
        return tree_info

    except requests.RequestException as e:
        print(f"Request error for {tree_id}: {e}")
        return None
    except ValueError:
        print(f"Bad JSON/structure for {tree_id}")
        return None
    except Exception as e:
        print(f"Unexpected error for {tree_id}: {e}")
        return None


def id_exists_in_file_locked(tid: int, f) -> bool:
    """
    With f already exclusively locked and opened in a+r mode, scan to see if tid exists.
    This is our race-proof check before appending.
    """
    f.seek(0)
    reader = csv.DictReader(f)
    if not reader.fieldnames:
        return False
    for row in reader:
        try:
            rid = row.get("id")
            if rid and int(rid) == tid:
                return True
        except (ValueError, TypeError):
            continue
    return False


# ------------ Main ------------

def main():
    parser = argparse.ArgumentParser(description="Amsterdam trees parser")
    parser.add_argument("start_id", type=int, nargs="?", help="Inclusive start tree ID")
    parser.add_argument("end_id", type=int, nargs="?", help="Exclusive end tree ID")
    args = parser.parse_args()

    # Defaults if not provided on CLI (kept only for convenience)
    START_ID = args.start_id if args.start_id is not None else 1_100_001
    END_ID   = args.end_id   if args.end_id   is not None else 1_200_000

    if START_ID >= END_ID:
        print("Error: start_id must be < end_id")
        sys.exit(1)

    ensure_csv_has_header(CSV_PATH)

    # Initial snapshot of existing IDs to skip calls quickly.
    existing_ids = load_existing_ids(CSV_PATH)
    print(f"Loaded {len(existing_ids)} existing IDs from {CSV_PATH}")

    # We'll refresh occasionally so parallel writers are noticed without restarting.
    reload_counter = 0

    # We open the file in a+r mode so we can both read & append beneath a single lock.
    # Note: each write operation will lock, re-check for duplicates, append, flush, fsync, unlock.
    for tree_id in range(START_ID, END_ID):
        # Periodically refresh the in-memory set to reduce duplicate API calls when running in parallel.
        if reload_counter % RELOAD_EXISTING_EVERY == 0 and reload_counter != 0:
            existing_ids = load_existing_ids(CSV_PATH)
            print(f"[refresh] now tracking {len(existing_ids)} IDs")
        reload_counter += 1

        if tree_id in existing_ids:
            continue

        tree_info = parse_tree(tree_id)
        if tree_info is None:
            sleep(SLEEP_BETWEEN_CALLS)
            continue

        tid = tree_info.get("id")
        if not isinstance(tid, int):
            sleep(SLEEP_BETWEEN_CALLS)
            continue

        # Race-proof write:
        # 1) Open file in a+r
        # 2) Exclusive lock
        # 3) Re-check if tid exists (another process may have added it)
        # 4) Append if absent
        # 5) Flush + fsync
        with open(CSV_PATH, mode="a+", newline="", encoding="utf-8") as f:
            try:
                lock_file(f, exclusive=True)

                # Re-check inside the lock
                if id_exists_in_file_locked(tid, f):
                    pass  # already present, skip write
                else:
                    # Move to end and write
                    f.seek(0, os.SEEK_END)
                    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                    writer.writerow(tree_info)
                    f.flush()
                    os.fsync(f.fileno())
                    # Keep our local set updated to reduce next calls this run
                    existing_ids.add(tid)
            finally:
                unlock_file(f)

        sleep(SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    main()
