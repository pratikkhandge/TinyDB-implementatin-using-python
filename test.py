"""
Description: Call tinydb connection class and use that
"""

# Application imports
from apis import TinyConn

# Initialise tinydb wrapper class and use db instance for db operations
db = TinyConn("test.json")
print db.list_documents()