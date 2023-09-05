from pathlib import Path 

CURRENT_DIR = Path(__file__).absolute()
ROOT_FOLDER = 'index-demo'
ROOT_DIR = next(p for p in CURRENT_DIR.parents if p.parts[-1] == ROOT_FOLDER)