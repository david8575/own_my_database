def parse_command(command: str, db_manager):
    cmd = command.strip().rstrip(";")

    if cmd.upper().startswith("CREATE DATABASE"):
        db_name = cmd[len("CREATE DATABASE"):].strip()
        db_manager.create_database(db_name)

    elif cmd.upper().startswith("USE"):
        db_name = cmd[len("USE"):].strip()
        db_manager.use_database(db_name)

    else:
        print("[ERROR] unsupported command (only CREATE DATABASE / USE allowed for now)")
