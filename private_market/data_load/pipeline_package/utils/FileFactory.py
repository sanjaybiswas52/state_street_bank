import re
from typing import List

class FileFactory:
    def __init__(self, shutil, src_path: str, dest_path: str):
        self.shutil = shutil
        self.src_path = src_path.rstrip("/")
        self.dest_path = dest_path.rstrip("/")

    def list_files(self) -> List[str]:
        """List all files in the source directory."""
        files = self.shutil.os.listdir(self.src_path)
        return [f"{self.src_path}/{f}" for f in files]

    def filter_files(self, pattern: str) -> List[str]:
        """Filter files based on regex pattern."""
        all_files = self.list_files()
        regex = re.compile(pattern)
        return [f for f in all_files if regex.search(f)]

    def move_files(self, pattern: str):
        """Move files matching the pattern from source to destination."""
        matched_files = self.filter_files(pattern)
        if not matched_files:
            print("No files matched the pattern.")
            return

        for file_path in matched_files:
            file_name = file_path.split("/")[-1]
            target_path = f"{self.dest_path}/{file_name}"
            try:
                print(f"Moving {file_path} -> {target_path}")
                self.dbutils.fs.mv(file_path, target_path)
            except Exception as e:
                print(f"Failed to move {file_path}: {e}")
            print("File move process completed.")

def load_if_exists(self, file_path: str):
    """
    Check if file exists. If yes, load as DataFrame; else return empty DataFrame.
    """
    try:
        self.dbutils.fs.ls(file_path)
        return True
    except Exception:
        return False

def wildcard_exists(self, dir_path: str, pattern: str) -> bool:
    """
    Check if any file in dir_path matches the given wildcard pattern.
    pattern can be regex like "GENERAL_LEDGER_ACTIVITY_INVESMENT_1.4488_20230331.*\.csv"
    """
    try:
        files = self.dbutils.fs.ls(dir_path)
        regex = re.compile(pattern)
        matched = [f.path for f in files if regex.search(f.name)]
        return len(matched) > 0
    except Exception:
        return False
