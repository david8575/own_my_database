import json
import os

DB_PATH = "./db/metadata.json"

class Table:
    def __init__(self):
        self.tables = self.load_metadata()

    def load_metadata(self):
        if os.path.exists(DB_PATH):
            with open(DB_PATH, 'r') as f:
                return json.load(f)
            
        return {}

    def save_metadata(self):
        with open(DB_PATH, 'w') as f:
            json.dump(self.tables, f)

    def create_table(self, name, columns):
        if name in self.tables:
            raise Exception("[table already exist]")
        
        self.tables[name] = columns
        with open(f".db/{name}.db", "w") as f:
            pass
        self.save_metadata()
    
    def insert(self, name, values):
        if name not in self.tables:
            raise Exception("[table not found]")
        with open(f".db/{name}.db", "a") as f:
            f.write(",".join(values) + "\n")