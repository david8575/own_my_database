import os

class DatabaseManager:
    def __init__(self, base_path="./storage"):
        self.base_path = base_path
        self.current_db = None

        os.makedirs(self.base_path, exist_ok=True)

    def create_database(self, db_name):
        db_path = os.path.join(self.base_path, db_name)

        if os.path.exists(db_path):
            raise Exception(f"database '{db_name}' already exsits")
        
        os.makedirs(db_path)

        print(f"[OK] database '{db_name}' created")


    def use_database(self, db_name):
        db_path = os.path.join(self.base_path, db_name)

        if not os.path.exists(db_path):
            raise Exception(f"database '{db_name}' does not exist")
        
        self.current_db = db_name
    
    def get_current_db_path(self):
        if not self.current_db:
            raise Exception("No database selected. Use 'USE dbname;' first.")
        return os.path.join(self.base_path, self.current_db)