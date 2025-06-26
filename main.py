from engine.table import Table

db = Table()
db.create_table("user", ["id", "name", "email"])
db.insert("user", ["1", "Alice", "alice@example.com"])
db.insert("user", ["2", "Bob", "bob@example.com"])