from db_core.table import TableManager
import os
table_manager = None

def init_parser(db_manager):
    global table_manager
    table_manager = TableManager(db_manager)

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
        table_name = parts[0].strip()
        columns = parts[1].rstrip(")").split(",")
        columns = [col.strip() for col in columns]
        table_manager.create_table(table_name, columns)

    elif cmd.upper().startswith("SHOW TABLES"):
        if table_manager is None:
            raise Exception("Parser not initialized.")

        meta_data = table_manager._get_load_path()

        if not meta_data:
            print("[OK] No tables found.")
        
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
