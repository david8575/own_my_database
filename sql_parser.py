from db_core.table import TableManager

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

    else:
        print("[ERROR] unsupported command")
