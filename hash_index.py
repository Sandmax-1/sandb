import os
from pathlib import Path 
from uuid import uuid4
import random

DB_PATH = Path(os.getcwd()) / 'hash_index_db.txt'

class HashIndexDB():
    def __init__(self):
        self.hash_index = {}

    def insert_into_db(self, key: str, value: int) -> None:
        with open(DB_PATH, 'a') as f:
            index = f.tell()
            f.write(f'{key}: {value}\n')
            print(f'{key}: {value}')
            self.hash_index.update({key: index})
        
    def read_from_db(self, key_to_retrieve: str) -> int:
        with open(DB_PATH, 'r') as f:
            f.seek(self.hash_index[key_to_retrieve])
            return(next(f).split()[1])    

if __name__ == "__main__":
    db = HashIndexDB()
    for i in range(1, 100):
        key = str(uuid4())
        value = random.randint(1, 1_000_000)
        db.insert_into_db(key, value)
    print(f'my_value is: {db.read_from_db(key)}')