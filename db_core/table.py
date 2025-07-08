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
            json.dump(meta_data, f, indent=2)

    def create_table(self, table_name, column_definitions):
        """
        column_definitions: 파싱된 컬럼 정의 리스트
        예: [{"name": "id", "type": "INT", "constraints": ["PRIMARY KEY"]}, ...]
        """
        db_path = self.db_manager.get_current_db_path()
        metadata = self._get_load_path()

        if table_name in metadata:
            raise Exception(f"Table '{table_name}' already exists.")

        # 컬럼 정의 검증
        self._validate_column_definitions(column_definitions)
        
        # 테이블 스키마 등록
        metadata[table_name] = {
            "columns": column_definitions,
            "primary_key": self._find_primary_key(column_definitions),
            "created_at": "2024-01-01"  # 실제로는 현재 시간을 저장
        }
        
        self._save_meta(metadata)

        # 테이블 파일 생성
        table_file = os.path.join(db_path, f"{table_name}.db")
        open(table_file, "w").close()

        print(f"[OK] Table '{table_name}' created with {len(column_definitions)} columns.")
        
        # 생성된 테이블 구조 출력
        print(f"Table structure:")
        for col in column_definitions:
            constraints_str = " ".join(col.get("constraints", []))
            print(f"  - {col['name']} {col['type']} {constraints_str}")

    def _validate_column_definitions(self, column_definitions):
        """컬럼 정의 유효성 검사"""
        if not column_definitions:
            raise Exception("Table must have at least one column")
        
        column_names = []
        for col in column_definitions:
            # 컬럼명 중복 검사
            if col["name"] in column_names:
                raise Exception(f"Duplicate column name: {col['name']}")
            column_names.append(col["name"])
            
            # 데이터 타입 검증
            valid_types = ["INT", "VARCHAR", "TEXT", "BOOLEAN", "FLOAT", "DOUBLE", "DATE", "DATETIME"]
            if col["type"] not in valid_types:
                raise Exception(f"Unsupported data type: {col['type']}")
            
            # VARCHAR 길이 검증
            if col["type"] == "VARCHAR" and col.get("length"):
                if col["length"] <= 0 or col["length"] > 65535:
                    raise Exception(f"Invalid VARCHAR length: {col['length']}")

    def _find_primary_key(self, column_definitions):
        """PRIMARY KEY 컬럼 찾기"""
        for col in column_definitions:
            if "PRIMARY KEY" in col.get("constraints", []):
                return col["name"]
        return None