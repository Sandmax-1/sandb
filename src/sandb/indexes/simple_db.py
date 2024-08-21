import os
import random
from pathlib import Path

from num2words import num2words

DB_PATH = Path(os.getcwd()) / "simple_db.txt"


def insert_into_db(key: str, value: int) -> None:
    with open(DB_PATH, "a") as f:
        f.write(f"{key}: {value}\n")


def read_from_db(key_to_retrieve: str) -> str:
    values = []
    with open(DB_PATH, "r") as f:
        for line in f:
            stored_key, value = line.split(":")
            if stored_key == key_to_retrieve:
                values.append(value.strip())
    return values[-1]


if __name__ == "__main__":
    for i in range(1, 100):
        value = random.randint(1, 1_000)
        insert_into_db(str(value), num2words(value))
    print(read_from_db(str(value)))
