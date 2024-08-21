import os
import random
from pathlib import Path

from num2words import num2words

DB_PATH = Path(os.getcwd()) / "hash_index_db.txt"


class HashIndexDB:
    def __init__(self) -> None:
        self.hash_index: dict[str, int] = {}

    def insert_into_db(self, key: str, value: int) -> None:
        with open(DB_PATH, "a") as f:
            index = f.tell()
            self.hash_index.update({key: index})
            f.write(f"{key}: {value}\n")

    def read_from_db(self, key_to_retrieve: str) -> str:
        with open(DB_PATH, "r") as f:
            f.seek(self.hash_index[key_to_retrieve])
            return next(f).split(":")[1].strip()


if __name__ == "__main__":
    db = HashIndexDB()
    for i in range(1, 100):
        value = random.randint(1, 100)
        db.insert_into_db(str(value), num2words(value))

    print(f"my_value is: {db.read_from_db(str(value))}")
