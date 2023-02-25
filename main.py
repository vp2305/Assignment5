import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd

DATABASE_NAME = "SoccerLeague"


def main():

    print("Soccer League Database!")

    # Connect to the database
    db = connectMySQL()

    inputted_num = 0
    while True:
        print("\n", "-" * 13, "Menu", "-" * 13)
        print("1. Show Tables")
        print("2. Query Table")
        print("3. Create Table")
        print("4. Drop Table")
        print("5. Populate Table")
        print("6. Quit")

        inputted_num = input("Select an option between 1-6: ")
        print("-" * 10, "Option (", inputted_num, ")", "-" * 10)

        # Available Options
        options = {
            1: showTables,
            2: queryTable,
            3: createTable,
            4: dropTable,
            5: populateTable,
            6: disconnectMySQL,
        }

        try:
            if int(inputted_num) in options.keys():
                # Call the function
                options[int(inputted_num)](db)
            else:
                print("Invalid option!")
        except ValueError:
            print("Invalid option!")


def connectMySQL():
    # Connect to user's MySQL server
    connection = mysql.connect(
        host="localhost",
        database="SoccerLeague",
        user="root",
        password="",
        autocommit=True,
    )

    # Check if connection is successful
    if connection.is_connected():
        # Get the server version
        db_Info = connection.get_server_info()
        print("\nConnected to MySQL Server version ", db_Info)

        # Create a cursor to execute queries
        cursor = connection.cursor()
        # Execute a query
        cursor.execute("SELECT DATABASE();")
        # Fetch the data using cursor object
        record = cursor.fetchone()
        print("You're connected to database: ", record[0])

        cursor.close()

    return connection


def disconnectMySQL(db):
    """
    Disconnect from the active connected database
    """
    db.close()
    print("Connection to MySQL is closed!")
    exit()


def showTables(db):
    """
    Show all the tables in the database
    """
    # Create a cursor to execute queries
    cursor = db.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    print("\nTables in the database are: ")
    for table in tables:
        print(" -", table[0])

    cursor.close()


def queryTable(db):
    """
    Query a table in the database by selecting the table name and printing the table
    """
    selectedTable = input("Enter the table you want to query: ").capitalize()

    # Create a cursor to execute queries
    cursor = db.cursor()

    try:
        # Fetch the table
        cursor.execute("SELECT * FROM " + DATABASE_NAME + "." + selectedTable + ";")
        tables = cursor.fetchall()

        # Fetch the column names for the selected table
        cursor.execute("DESCRIBE " + DATABASE_NAME + "." + selectedTable + ";")
        columns = cursor.fetchall()

        # Print the table
        print("\nTable: ", selectedTable, "\n")
        print(pd.DataFrame(tables, columns=[i[0] for i in columns]))

    except Error as err:
        print("Error: {}".format(err))
    finally:
        cursor.close()


def createTable(db):
    """
    Create a new table in the database by selecting the table name and the attributes
    """
    newTable = input("Enter the name of the new table: ").capitalize()
    attributes = []

    while True:
        print("\n", "-" * 13, "Table Attributes", "-" * 13)
        print("1. Add attribute")
        print("2. Finish table creation")
        inputted_option = input("Select an option between 1-2: ")
        print("-" * 10, "Option (", inputted_option, ")", "-" * 10)

        try:
            if int(inputted_option) > 0 and int(inputted_option) < 3:
                if int(inputted_option) == 1:
                    att = input(
                        "Enter the attribute in a following way (name type constraints):  "
                    )
                    attributes.append(att)
                else:
                    break
            else:
                print("Invalid option!")
        except ValueError:
            print("Invalid option!")

    # Create sql query
    sqlQuery = "CREATE TABLE " + DATABASE_NAME + "." + newTable + "("
    for att in attributes:
        sqlQuery += att + ", "
    sqlQuery = sqlQuery[:-2] + ");"

    # Create a cursor to execute queries
    cursor = db.cursor()
    try:
        # Create the table
        cursor.execute(sqlQuery)
        print("Table ", newTable, "created successfully!")
    except Error as err:
        print("Error: {}".format(err))
    finally:
        cursor.close()


def dropTable(db):
    """
    Drop a table in the database by selecting the table name
    """
    table = input("Enter the name of the table to drop: ").capitalize()

    # Create a cursor to execute queries
    cursor = db.cursor()
    try:
        # Drop the table
        sqlQuery = "DROP TABLE " + DATABASE_NAME + "." + table + ";"
        # print(sqlQuery)
        cursor.execute(sqlQuery)
        print("Table ", table, "dropped successfully!")
    except Error as err:
        print("Error: {}".format(err))
    finally:
        cursor.close()


def populateTable(db):
    """
    Populate a table in the database by asking the user to select the table name and the csv file
    """
    print("Populate table is selected!")
    table = input("Enter the name of the table to populate: ").capitalize()
    csvFile = input("Enter the name of the csv file: ") + ".csv"

    # Populate the table using insert values query
    df = pd.read_csv(csvFile)

    # Get column names
    columns = []
    for col in df.columns:
        columns.append(col)

    # Get columns
    columnNames = "(" + ", ".join(columns) + ")"

    # Create sql query
    sqlQuery = "INSERT INTO " + DATABASE_NAME + "." + table + columnNames + " VALUES "

    # Iterate through the rows of the dataframe
    for index, row in df.iterrows():
        sqlQuery += "("
        for col in df.columns:
            sqlQuery += "'" + str(row[col]) + "', "
        sqlQuery = sqlQuery[:-2] + "), "
    sqlQuery = sqlQuery[:-2] + ";"

    # Create a cursor to execute queries
    cursor = db.cursor()
    try:
        cursor.execute(sqlQuery)
        print("Table ", table, "populated successfully!")
    except Error as err:
        print("Error: {}".format(err))
    finally:
        cursor.close()


main()
