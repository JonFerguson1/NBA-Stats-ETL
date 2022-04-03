import mysql.connector

pw = '' # Your MySQL password here

config = {
    'user': '', # MySQL username
    'password': pw,
    'host': 'localhost'
}

db = mysql.connector.connect(**config)
cursor = db.cursor()
