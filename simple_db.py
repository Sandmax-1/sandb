import os
from pathlib import Path 
from uuid import uuid4
import random

DB_PATH = Path(os.getcwd()) / 'my_db.txt'


def insert_into_db(key: str, value: int) -> None:
    with open(DB_PATH, 'a') as f:
        f.write(f'{key}: {value}\n')
        print(f'{key}: {value}')
    
def read_from_db(key_to_retrieve: str) -> int:
    with open(DB_PATH, 'r') as f:
        for line in f:
            stored_key, value = line.split(':')
            if stored_key == key_to_retrieve:
                return int(value)

if __name__ == "__main__":
    for i in range(1, 100):
        key = uuid4()
        value = random.randint(1, 1_000_000)
        insert_into_db(key, value)
    print(read_from_db(str(key)))

