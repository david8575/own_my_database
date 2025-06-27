import os 
import json

class TableManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def _get_meta_path(self):
        db_path = self.db_manager.get_current_db_path()
        return os.path.join(db_path, "metadata.json")
    
    def _get_load_path(self):
        meta_path = self._get_meta_path()

        if not os.path.exists(meta_path):
            return {}
        
        with open(meta_path, "r") as f:
            try:
                return json.load(f)
            except:
                return{}
            
    def _save_meta(self, meta_data):
        meta_path = self._get_meta_path()
        with open(meta_path, "w") as f:
            json.dump(meta_data, f)

    def create_table(self, table_name, columns):
        db_path = self.db_manager.get_current_db_path()
        metadata = self._get_load_path()

        if table_name in metadata:
            raise Exception(f"Table '{table_name}' already exists.")

        # 테이블 스키마 등록
        metadata[table_name] = columns
        self._save_meta(metadata)

        # 테이블 파일 생성
        table_file = os.path.join(db_path, f"{table_name}.db")
        open(table_file, "w").close()

        print(f"[OK] Table '{table_name}' created.")