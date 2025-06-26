import time
from sql_parser import parse_command
from db_core.database import DatabaseManager

def main():
    print("[Connectiing MYDB......]")
    time.sleep(1)
    print("[Server version: 1.0-MYDB]")
    print("[Welcome To My Own DataBase!]")
    print("[Type 'exit' to Quit.]\n")

    db_manager = DatabaseManager()

    while True:
        try: 
            command = input("mydb>>> ").strip()

            if command.lower() in ["exit", "quit"]:
                print("[bye!]")
                break

            parse_command(command, db_manager)        
        
        except Exception as e:
            print(f"[error]: {e}")

if __name__=="__main__":
    main()