from db_core.table import TableManager
import os
import re

table_manager = None

def init_parser(db_manager):
    global table_manager
    table_manager = TableManager(db_manager)

def parse_column_definition(column_def):
    """
    컬럼 정의를 파싱하여 컬럼명, 타입, 제약조건을 추출
    예: "id INT PRIMARY KEY" -> {"name": "id", "type": "INT", "constraints": ["PRIMARY KEY"]}
    """
    column_def = column_def.strip()
    
    # 지원하는 데이터 타입 패턴
    type_patterns = {
        r'INT(?:EGER)?': 'INT',
        r'VARCHAR\(\d+\)': 'VARCHAR',
        r'TEXT': 'TEXT',
        r'BOOLEAN': 'BOOLEAN',
        r'FLOAT': 'FLOAT',
        r'DOUBLE': 'DOUBLE',
        r'DATE': 'DATE',
        r'DATETIME': 'DATETIME'
    }
    
    # 제약조건 패턴
    constraint_patterns = [
        r'PRIMARY\s+KEY',
        r'NOT\s+NULL',
        r'UNIQUE',
        r'AUTO_INCREMENT',
        r'DEFAULT\s+\S+'
    ]
    
    # 컬럼명과 나머지 부분 분리 (첫 번째 공백 기준)
    parts = column_def.split(' ', 1)
    if len(parts) < 2:
        raise Exception(f"Invalid column definition: {column_def}")
    
    column_name = parts[0].strip()
    remaining = parts[1].strip()
    
    # 데이터 타입 찾기
    data_type = None
    type_length = None
    
    for pattern, type_name in type_patterns.items():
        match = re.match(pattern, remaining, re.IGNORECASE)
        if match:
            data_type = type_name
            # VARCHAR(255) 같은 경우 길이 추출
            if type_name == 'VARCHAR':
                length_match = re.search(r'\((\d+)\)', match.group())
                if length_match:
                    type_length = int(length_match.group(1))
            break
    
    if not data_type:
        raise Exception(f"Unsupported data type in: {column_def}")
    
    # 타입 부분 제거
    type_end = remaining.find(data_type) + len(data_type)
    if type_name == 'VARCHAR' and type_length:
        type_end = remaining.find(')', type_end) + 1
    
    remaining = remaining[type_end:].strip()
    
    # 제약조건 찾기
    constraints = []
    for pattern in constraint_patterns:
        match = re.search(pattern, remaining, re.IGNORECASE)
        if match:
            constraint = match.group().upper()
            constraints.append(constraint)
            # 찾은 제약조건 제거
            remaining = remaining[:match.start()] + remaining[match.end():]
    
    # DEFAULT 값 처리
    default_match = re.search(r'DEFAULT\s+([^,\s]+)', remaining, re.IGNORECASE)
    if default_match:
        default_value = default_match.group(1)
        constraints.append(f"DEFAULT {default_value}")
    
    return {
        "name": column_name,
        "type": data_type,
        "length": type_length,
        "constraints": constraints
    }

def parse_command(command: str, db_manager):
    cmd = command.strip().rstrip(";")

    if cmd.upper().startswith("CREATE DATABASE"):
        db_name = cmd[len("CREATE DATABASE"):].strip()
        db_manager.create_database(db_name)

    elif cmd.upper().startswith("USE"):
        db_name = cmd[len("USE"):].strip()
        db_manager.use_database(db_name)

    elif cmd.upper().startswith("CREATE TABLE"):
        if table_manager is None:
            raise Exception("Parser not initialized.")
        
        parts = cmd[len("CREATE TABLE"):].strip().split("(", 1)
        if len(parts) != 2:
            raise Exception("Invalid CREATE TABLE syntax. Expected: CREATE TABLE table_name (column_definitions)")
        
        table_name = parts[0].strip()
        columns_def = parts[1].rstrip(")")
        
        # 컬럼 정의들을 파싱
        column_defs = []
        current_def = ""
        paren_count = 0
        
        for char in columns_def:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                if current_def.strip():
                    parsed_col = parse_column_definition(current_def.strip())
                    column_defs.append(parsed_col)
                current_def = ""
                continue
            
            current_def += char
        
        # 마지막 컬럼 정의 처리
        if current_def.strip():
            parsed_col = parse_column_definition(current_def.strip())
            column_defs.append(parsed_col)
        
        if not column_defs:
            raise Exception("No valid column definitions found")
        
        table_manager.create_table(table_name, column_defs)

    elif cmd.upper().startswith("SHOW TABLES"):
        if table_manager is None:
            raise Exception("Parser not initialized.")

        meta_data = table_manager._get_load_path()

        if not meta_data:
            print("[OK] No tables found.")
        else:
            print("[OK] Tables:")
            for table_name in meta_data.keys():
                print(f"  - {table_name}")

    elif cmd.upper().startswith("DROP TABLE"):
        if table_manager is None:
            raise Exception("Parser not initialized.")

        table_name = cmd[len("DROP TABLE"):].strip()
        meta_data = table_manager._get_load_path()

        if table_name not in meta_data:
            raise Exception(f"Table '{table_name}' does not exist.")

        # 테이블 메타데이터 삭제
        del meta_data[table_name]
        table_manager._save_meta(meta_data)

        # 테이블 파일 삭제
        db_path = db_manager.get_current_db_path()
        table_file = os.path.join(db_path, f"{table_name}.db")
        if os.path.exists(table_file):
            os.remove(table_file)

        print(f"[OK] Table '{table_name}' dropped.")

    else:
        print("[ERROR] unsupported command")
