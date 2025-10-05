import sqlite3

# Initialize database connection
# This file creates a persistent connection to the SQLite database
# Use this connection object in other modules or add initialization logic below
con = sqlite3.connect("../data/example.db")
