import sys
from prettytable import PrettyTable, from_db_cursor
import os
import getpass
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import time

NO_DB_COMMANDS = [
    "use database;",
    "show databases;",
    "create databases;",
    "delete database;",
    "exit;",
    "help;",
    r"\h;",
    "?;",
    "create user;",
    "reveal user;",
    "delete user;",
    "show default engine;",
    "change default engine;",
    "license;",
    "advanced mode;"
]

DB_COMMANDS = [
    "show tables;",
    "create table;",
    "desc table;",
    "describe table;",
    "delete tables;",
    "show columns;",
    "add columns;",
    "modify columns;",
    "delete columns;",
    "reveal;",
    "search;",
    "insert;",
    "update;",
    "delete;",
    "conditional average;",
    "distinct average;",
    "distinct conditional average;",
    "group insert;",
    "count;",
    "distinct count;",
    "conditional count;",
    "distinct conditional count;",
    "max;",
    "conditional max;",
    "distinct max;",
    "distinct conditional max;",
    "min;",
    "conditional min;",
    "distinct min;",
    "distinct conditional min;",
    "sum;",
    "conditional sum;",
    "distinct sum;",
    "distinct conditional sum;",
    "show table engine;",
    "change table engine;"
]

HELP_COMMANDS = ["help;", r"\h;", "?;"]

PT = PrettyTable()
is_connection = False
db = None

clear_screen = 'cls'
if os.name == 'posix':
    clear_screen = 'clear'
os.system(clear_screen)  # Clearing the screen

for _ in range(3):
    try:
        user_name = input("Enter your username: ")
        password = getpass.getpass(prompt="Enter your password: ")
        host = "localhost"
        connection = mysql.connector.connect(user=user_name,
                                             password=password,
                                             host=host)
        cursor = connection.cursor()
        time_now = datetime.now()
        is_connection = True

        print("Connecting to the server...")
        time.sleep(1)
        os.system(clear_screen)

        print(f"LOGGED IN AS: {user_name}@{host}")
        print(f"TIME: {time_now.strftime('%H:%M%S %p')}")
        print(f"MySQL server version: {connection.get_server_info()}")
        print(f"Connection ID: {connection.connection_id}")
        break

    except EOFError:
        print()
        continue
    except KeyboardInterrupt:
        print("Exiting...")
        time.sleep(1)
        sys.exit()
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Username or password is incorrect\n")
            continue
        print(f"\n{e}\n")
        break
else:
    print("Wrong credentials entered 3 times.\n Exiting...")
    time.sleep(1)
    sys.exit()


def main():
    if is_connection:
        user_info()  # Showing the user info related to the program
        while True:
            print("PySQL|> ", end=" ")
            try:
                command = input().lower()
                command_execute(command)
            except EOFError:
                continue
            except KeyboardInterrupt:
                print("\nSession forcefully closed by the user !\n")
                break
            except ValueError:
                print("\nError! Please enter the values properly")
            except mysql.connector.Error as error:
                error = str(error.msg).split("; ")[0]
                print(f"Error: {error}\n")
        cursor.close()
        connection.close()
    else:
        print("PySQL will exit automatically in 5 seconds.\n")
        time.sleep(5)
        close()


def command_execute(command):
    if len(command) == 0:
        print()
    elif ";" not in command[-1]:
        print("\nError: ';' is missing")
    elif command not in NO_DB_COMMANDS and command not in DB_COMMANDS:
        print("\nError: Command not found!")
    elif command in DB_COMMANDS and not db:
        print("\nError: No database selected!")
    else:
        run(command)


def run(command):
    if command == "use database;":
        use_db()
    elif command == "show databases;":
        show_db()
    elif command == "create databases;":
        create_db()
    elif command == "delete databases;":
        delete_db()
    elif command == "show tables;":
        show_tb()
    elif command == "create table;":
        create_tb()
    elif command in ("describe table;", "desc table;"):
        describe_tb()
    elif command == "delete table;":
        delete_tb()
    elif command == "show columns;":
        show_columns()
    elif command == "add column;":
        add_column()
    elif command == "modify column;":
        modify_column()
    elif command == "delete  column;":
        delete_column()
    elif command == "reveal;":
        reveal()
    elif command == "insert;":
        insert()
    elif command == "search;":
        search()
    elif command == "update;":
        update()
    elif command == "delete;":
        delete()
    elif command == "average;":
        average()
    elif command == "conditional average;":
        conditional_average()
    elif command == "distinct average;":
        distinct_average()
    elif command == "distinct conditional average;":
        distinct_conditional_average()
    elif command == "group insert;":
        group_insert()
    elif command == "count;":
        count()
    elif command == "conditional count;":
        conditional_count()
    elif command == "distinct count;":
        distinct_count()
    elif command == "distinct conditional count;":
        distinct_conditional_count()
    elif command == "max;":
        mysql_max()
    elif command == "conditional max;":
        conditional_mysql_max()
    elif command == "distinct max;":
        distinct_mysql_max()
    elif command == "distinct conditional max;":
        distinct_conditional_mysql_max()
    elif command == "min;":
        mysql_min()
    elif command == "conditional min;":
        conditional_mysql_min()
    elif command == "distinct min;":
        distinct_mysql_min()
    elif command == "distinct conditional min;":
        distinct_conditional_mysql_min()
    elif command == "sum;":
        mysql_sum()
    elif command == "conditional sum;":
        conditional_mysql_sum()
    elif command == "distinct sum;":
        distinct_mysql_sum()
    elif command == "distinct conditional sum;":
        distinct_conditional_mysql_sum()
    elif command == "create user;":
        create_user()
    elif command == "reveal user;":
        reveal_users()
    elif command == "delete user;":
        delete_user()
    elif command == "show default engine;":
        show_default_engine()
    elif command == "change default engine;":
        change_default_engine()
    elif command == "advance mode;":
        advance_mode()
    elif command == "exit;":
        close()
        sys.exit()
    elif command in HELP_COMMANDS:
        program_help()


def get_input(prompt):
    print("     ->", end="")
    return input(f"{prompt}")


def check(user_input):
    """
    Function to check whether the user
    cancelled the input statement or has
    given no input at all.
    """
    take_next_step = True  # Boolean variable to check and process further ahead

    if r"\c" in user_input:
        print("\nQuery cancelled!")
        take_next_step = False
    elif len(user_input) == 0:
        take_next_step = False
        print("\nPlease enter the values properly")

    return take_next_step


def use_db():
    global db

    database_name = get_input("DATABASE NAME: ")
    if check(database_name):
        command = f"USE {database_name}"
        cursor.execute(command)
        db = database_name
        print(f"\nQuery OK, now using database [{database_name}].\n")


def show_db():
    command = "SHOW DATABASES"
    cursor.execute(command)
    row_count = len(cursor.fetchall())
    cursor.execute(command)
    table = from_db_cursor(cursor)
    table.align = "l"
    print(table)
    print(f"Database(s) count: {row_count}\n")


def create_db():
    database_name = get_input("DATABASE NAME: ")
    if check(database_name):
        command = f"CREATE DATABASE {database_name}"
        cursor.execute(command)
        connection.commit()
        print(f"\nQuery OK, Created database [{database_name}].\n")


def delete_db():
    global db

    database_name = get_input("DATABASE NAME: ")
    if check(database_name):
        if db == database_name:
            db = None

        print()
        option = get_input(f"IRREVERSIBLE CHANGE! Do you really want to delete the database "
                           f"[{database_name}]? (y/[n]) ")

        if option.lower() == 'y':
            command = f"DROP DATABASE {database_name}"
            cursor.execute(command)
            connection.commit()
            print(f"\nQuery OK, Deleted database [{database_name}].")
        else:
            print(f"\nQuery cancelled, for deletion of the database [{database_name}].")


def show_tb():
    command = "SHOW TABLES"
    cursor.execute(command)
    row_count = len(cursor.fetchall())
    cursor.execute(command)
    table = from_db_cursor(cursor)
    table.align = 'l'
    print(table)
    print(f"Table(s) count: {row_count}\n")


def create_tb():
    table_name = get_input("ENTER TABLE NAME: ")
    if check(table_name):
        no_of_columns = get_input("No. of columns: ")
        if check(no_of_columns):
            is_query_cancelled = False

            no_of_columns = int(no_of_columns)
            columns = ''
            column_no = 0

            for column_no in range(no_of_columns):
                column_value_type = get_input(f"COLUMN ({column_no}) NAME AND DATA-TYPE: ")
                if check(column_value_type):
                    columns += column_value_type + ', '
                else:
                    is_query_cancelled = True
                    break

            if not is_query_cancelled:
                column_value_type = get_input(f"COLUMN ({column_no + 1}) NAME AND DATA-TYPE: ")
                primary_key = get_input("PRIMARY KEY: ")
                if check(primary_key):
                    command = f"CREATE TABLE {table_name}({column_value_type}, PRIMARY KEY ({primary_key}))"
                    cursor.execute(command)
                    connection.commit()
                    print(f"\nQuery OK, Created table [{table_name}].")


def describe_tb():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        command = f"DESCRIBE {table_name}"
        cursor.execute(command)
        column_count = len(cursor.fetchall())
        cursor.execute(command)
        table = from_db_cursor(cursor)
        table.align = "l"
        print(table)
        print(f"Column(s) count {column_count}\n")


def delete_tb():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        opt = get_input(f"IRREVERSIBLE CHANGE! Do you really want to delete the table [{table_name}]? (y/[n]) ")

        if opt.lower() == 'y':
            command = f"DROP TABLE {table_name}"
            cursor.execute(command)
            connection.commit()
            print(f"\nQuery OK, deleted the table [{table_name}]")
        else:
            print("\nQuery cancelled, for deletion of table.")


def show_columns():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        command = f"SHOW COLUMNS FROM {table_name}"
        cursor.execute(command)
        column_count = len(cursor.fetchall())
        table = from_db_cursor(cursor)
        table.align = 'l'
        print(table)
        print(f"Columns(s) count: {column_count}")


def add_column():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_data = get_input("COLUMN NAME AND DATA-TYPE: ")
        if check(column_data):
            command = f"ALTER TABLE {table_name} ADD {column_data}"
            cursor.execute(command)
            connection.commit()

            column_name = column_data.split()[0]
            data_type = column_data.split()[1]

            print(f"\nQuery OK, added column [{column_name}] with data-type [{data_type}] to the table [{table_name}].")


def modify_column():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_data = get_input("COLUMN NAME AND DATA-TYPE: ")
        if check(column_data):
            command = f"ALTER TABLE {table_name} MODIFY {column_data}"
            cursor.execute(command)
            connection.commit()

            column_name = column_data.split()[0]
            data_type = column_data.split()[1]

            print(f"\nQuery OK, modified column [{column_name}] to new data-type "
                  f"[{data_type}] in table [{table_name}].")


def delete_column():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("NAME OF THE COLUMN TO BE DELETED: ")
        if check(column_name):
            opt = get_input(f"IRREVERSIBLE CHANGE! Do you really want to delete the column [{column_name}]? (y/[n]) ")

            if opt.lower() == 'y':
                command = f"ALTER TABLE {table_name} DROP {column_name}"
                cursor.execute(command)
                connection.commit()
                print(f"\nQuery OK, Deleted column [{column_name}] from table [{table_name}].")
            else:
                print("\nQuery cancelled, for deletion of column.")


def reveal():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            if column_name.lower == "all":
                column_name = "*"
            command = f"SELECT {column_name} FROM {table_name}"
            cursor.execute(command)
            row_count = len(cursor.fetchall())
            cursor.execute(command)

            table = from_db_cursor(cursor)
            table.align = 'l'
            print(table)
            print(f"Row(s) count: {row_count}\n")


def insert():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAMES: ")
        if check(column_name):
            values = get_input("VALUES: ")
            if check(values):
                command = f"INSERT INTO {table_name} ({column_name}) VALUES({values})"
                cursor.execute(command)
                connection.commit()
                row_count = cursor.rowcount
                print(f"\nQuery OK, inserted value(s) [{values}] in column(s) [{column_name}]"
                      f" into table [{table_name}].")
                print(f"Affected rows(s): {row_count}")


def update():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        condition = get_input("CONDITION: ")
        if check(condition):
            column_name = get_input("COLUMNS(s) TO BE UPDATED: ")
            if check(column_name):
                updated_value = get_input("VALUE OF DATA-ITEM TO BE UPDATED: ")
                if check(updated_value):
                    command = f"UPDATE {table_name} SET {column_name} = {updated_value} WHERE {condition}"
                    cursor.execute(command)
                    connection.commit()
                    row_count = cursor.rowcount
                    if row_count == 0:
                        print("\nThe given condition was not satisfied")
                    else:
                        print(f"\nQuery OK, updated the row(s)/record(s) in column/field [{column_name}] "
                              f"to [{updated_value}] where condition [{condition}] was satisfied.")
                    print(f"Affected row(s): {row_count}")


def search():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_names = get_input("COLUMN NAMES: ")
        if check(column_names):
            condition = get_input("CONDITION: ")
            if check(condition):
                if column_names in ("ALL", "all", "All"):
                    column_names = "*"

                command = f"SELECT {column_names} FROM {table_name} WHERE {condition}"
                cursor.execute(command)
                row_count = len(cursor.fetchall())
                cursor.execute()

                table = from_db_cursor(cursor)
                table.align = 'l'
                print(table)
                print(f"\nRow(s) count: {row_count}")


def delete():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        condition = get_input("CONDITION: ")
        if check(condition):
            command = f"DELETE FROM {table_name} WHERE {condition}"
            cursor.execute(command)
            connection.commit()
            row_count = cursor.rowcount
            print(f"\nQuery OK, deleted the row(s)/record(s) where condition [{condition}] was satisfied.")
            print(f"\nAffected row(s): {row_count}")


def group_insert():
    row_count = 0
    try:
        table_name = get_input("TABLE NAME: ")
        if check(table_name):
            no_of_rows = get_input("NO. OF ROWS: ")
            if check(no_of_rows):
                column_name = get_input("COLUMN NAME: ")
                no_of_rows = int(no_of_rows)
                rows_values = []

                for _ in range(no_of_rows):
                    row_data = input("          ->")
                    rows_values.append(row_data)
                    if check(row_data):
                        continue

                for values in rows_values:
                    command = f"INSERT INTO {table_name} ({column_name}) VALUES({values})"
                    cursor.execute(command)
                    connection.commit()
                    row_count += 1

                print(f"\nQuery OK, inserted the given value(s) in column(s) [{column_name}] in table [{table_name}]")

    finally:
        print(f"\nAffected row(s): {row_count}")


def average():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f"SELECT ROUND(AVG({column_name}), 2) '{title}' FROM {table_name}"
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = 'l'
                print(table, '\n')


def conditional_average():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(AVG({column_name}), 2) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def distinct_average():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(AVG(DISTINCT {column_name}), 2) ' \
                          f'"{title}" from {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def distinct_conditional_average():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(AVG(DISTINCT {column_name}), 2)' \
                              f' "{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def count():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT COUNT({column_name}) "{title}" ' \
                          f'FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def conditional_count():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT COUNT({column_name}) "{title}" ' \
                              f'FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def distinct_count():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT COUNT(DISTINCT {column_name}) "{title}"' \
                          f' FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table)


def distinct_conditional_count():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT COUNT(DISTINCT {column_name}) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def mysql_max():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(MAX({column_name}), 2) "{title}"' \
                          f' FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def conditional_mysql_max():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(MAX({column_name}), 2)' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def distinct_mysql_max():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(MAX(DISTINCT {column_name}), 2) ' \
                          f'"{title}" FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def distinct_conditional_mysql_max():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(MAX(DISTINCT {column_name}), 2) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def mysql_min():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(MIN({column_name}), 2) ' \
                          f'"{title}" FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def conditional_mysql_min():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(MIN({column_name}), 2) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def distinct_mysql_min():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(MIN(DISTINCT {column_name}), 2) ' \
                          f'"{title}" FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def distinct_conditional_mysql_min():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(MIN(DISTINCT {column_name}), 2) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def mysql_sum():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(SUM({column_name}), 2) "{title}" ' \
                          f'FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def conditional_mysql_sum():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(SUM({column_name}), 2) ' \
                              f'"{title}" FROM {table_name}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def distinct_mysql_sum():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            title = get_input("TITLE: ")
            if check(title):
                command = f'SELECT ROUND(SUM(DISTINCT {column_name}), 2) ' \
                          f'"{title}" FROM {table_name}'
                cursor.execute(command)
                cursor.execute(command)
                table = from_db_cursor(cursor)
                table.align = "l"
                print(table, "\n")


def distinct_conditional_mysql_sum():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        column_name = get_input("COLUMN NAME: ")
        if check(column_name):
            condition = get_input("CONDITION: ")
            if check(condition):
                title = get_input("TITLE: ")
                if check(title):
                    command = f'SELECT ROUND(SUM(DISTINCT {column_name}), 2) ' \
                              f'"{title}" FROM {table_name} WHERE {condition}'
                    cursor.execute(command)
                    cursor.execute(command)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table, "\n")


def create_user():
    new_usr_name = get_input("NEW USER NAME: ")
    if check(new_usr_name):
        new_usr_pwd = get_input("NEW USER's PASSWORD: ")
        if check(new_usr_pwd):
            host_name = get_input("HOSTNAME: ")
            create_command = f"CREATE USER '{new_usr_name}'@'{host_name}' " \
                             f"IDENTIFIED BY '{new_usr_pwd}'"

            cursor.execute(create_command)
            connection.commit()

            grant_command = f"GRANT ALL ON * . * TO " \
                            f"'{new_usr_name}'@'{host_name}'"
            cursor.execute(grant_command)
            connection.commit()

            print(f"\nQuery OK, created and granted all privileges to the "
                  f"user [{new_usr_name}].\n")


def reveal_users():
    command = "SELECT * FROM INFORMATION_SCHEMA.USER_ATTRIBUTES"
    cursor.execute(command)
    table = from_db_cursor(cursor)
    table.align = "l"
    print(table, "\n")


def delete_user():
    user_name = get_input("USER-NAME: ")
    if check(user_name):
        host_name = get_input("HOST: ")
        if check(host_name):
            print()
            opt = input(f"IRREVERSIBLE CHANGE! Do you really want "
                        f"to remove the user [{user_name}]? (y/[n]) ")
            if opt.lower() == 'y':
                command = f"DROP USER '{user_name}'@'{host_name}'"
                cursor.execute(command)
                connection.commit()
                print(f"\nQuery OK, removed the user [{user_name}].\n")
            else:
                print(f"\nQuery cancelled, for removal of user "
                      f"[{user_name}].\n")


def show_default_engine():
    command = "SHOW ENGINES"
    cursor.execute(command)
    table = from_db_cursor(cursor)
    table.align = "l"
    print(table, "\n")


def change_default_engine():
    engine_name = get_input("NEW ENGINE NAME: ")
    if check(engine_name):
        command = f"SET default_storage_engine={engine_name}"
        cursor.execute(command)
        connection.commit()
        print(f"\nQuery OK, now using [{engine_name}] storage engine "
              f"as default.\n")


def show_table_engine():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        command = f"SELECT TABLE_NAME,  ENGINE FROM " \
                  f"INFORMATION_SCHEMA.TABLES WHERE table_name = '{table_name}'"
        cursor.execute(command)
        table = from_db_cursor(cursor)
        table.align = "l"
        print(table, "\n")


def change_table_engine():
    table_name = get_input("TABLE NAME: ")
    if check(table_name):
        engine_name = get_input("ENGINE NAME: ")
        if check(engine_name):
            command = f"ALTER TABLE {table_name} ENGINE = {engine_name}"
            cursor.execute(command)
            connection.commit()
            print(f"\nQuery OK, now using [{engine_name}] storage engine "
                  f"for table [{table_name}].\n")


def advance_mode():
    global db

    print()
    if db is not None:
        print(f"NOTE! Current Database: [{db}]\n")
    while True:
        try:
            print("mysql> ", end="")
            statement = input()

            if r"\c" in statement or statement == "":
                print()
                continue

            while ";" not in statement:
                print("    -> ", end="")
                continued_statement = input()
                if continued_statement != "":
                    statement += " " + continued_statement
                    if r"\c" in continued_statement:
                        print()
                        break
                else:
                    continue

            if r"\c" in statement:
                continue
            if statement.lower() in ("exit;", "quit;"):
                print("\nTo exit advance mode, type 'exit advance mode;'\n")
            elif statement == "exit advance mode;":
                print()
                break
            else:
                cursor.execute(statement)

                if "use" in statement:
                    database_name = statement.strip(';').split()[1]
                    if database_name != db:
                        db = database_name
                    print("\nDatabase changed\n")
                elif cursor.with_rows:
                    row_count = len(cursor.fetchall())
                    cursor.execute(statement)
                    table = from_db_cursor(cursor)
                    table.align = "l"
                    print(table)
                    print(f"{row_count} rows in set")
                    print()
                else:
                    connection.commit()
                    affected_rows = cursor.rowcount
                    print(f"\nQuery OK, affected rows: {affected_rows}\n")
        except mysql.connector.Error as err:
            print(f"\n{err}\n")


def close():
    print("Exiting...")
    time.sleep(2)
    print("Bye.\n")


def program_help():
    print("""
+------------------------------------------------------------------------------+
|                               INSTRUCTIONS                                   |
|                               ------------                                   |
|                                                                              |
| COMMANDS FOR DATABASE MANIPULATION:                                          |
|                                                                              |
| use database;    > To use a database.                                        |
| show databases;  > To show all of the databases.                             |
| create database; > To create a new database.                                 |
| delete database; > To delete an existing database.                           |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR TABLE MANIPULATION:                                             |
|                                                                              |
| show tables;    > To show tables present in a database.                      |
| create table;   > To create a new table.                                     |
| describe table; > To see the schema(structure) of a table.                   |
| delete table;   > To delete a table completely.                              |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR COLUMN MANIPULATION:                                            |
|                                                                              |
| add column;    > To add a new column to an existing table.                   |
| modify column; > To change data-type of a column in a table.                 |
| delete column; > To delete an exiting column inside a table.                 |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR IN-TABLE QUERIES:                                               |
|                                                                              |
| reveal; > To show all of the data stored inside columns in a table.          |
| search; > To search for specific data-items in a table.                      |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR  IN-TABLE MANIPULATION:                                         |
|                                                                              |
| insert; > To insert data in a table.                                         |
| update; > To modify or change value of a data-item present in a column/field.|
| delete; > To delete row(s)/record(s).                                        |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR SPECIAL OPERATIONS:                                             |
|                                                                              |
| I. Group Data Insertion:                                                     |
|     1. group insert; > To insert data in a grouped manner in a table.        |
|                                                                              |
| II. Average Of Data-Items In A Column:                                       |
|     1. average;                      > To get the average of data-items.     |
|     2. conditional average;          > To get the average data-items based   |
|                                        on condition.                         |
|     3. distinct average;             > To get the average of distinct        |
|                                        data-items.                           |
|     4. distinct conditional average; > To get the average of distinct        |
|                                        data-items based on condition.        |
|                                                                              |
| III. Count Of Data-Items In A Column:                                        |
|     1. count;                      > To count the number of NOT NULL         |
|                                      data-items.                             |
|     2. conditional count;          > To count the number of NOT NULL         |
|                                      data-items based on a condition.        |
|     3. distinct count;             > To count the number of distinct         |
|                                      NOT NULL data-items.                    |
|     4. distinct conditional count; > To count the number of distinct         |
|                                      NOT NULL data-items based on a          |
|                                      condition.                              |
|                                                                              |
| IV. Maximum Value:                                                           |
|     1. max;                      > To get the value of biggest data-item.    |
|     2. conditional max;          > To get the value of biggest data-item     |
|                                    based on a condition.                     |
|     3. distinct max;             > To get the value of biggest distinct      |
|                                    data-item.                                |
|     4. distinct conditional max; > To get the value of biggest distinct      |
|                                    data-item based on a condition.           |
|                                                                              |
| V. Minimum Value:                                                            |
|     1. min;                      > To get the value of smallest data-item.   |
|     2. conditional min;          > To get the value of smallest data-item    |
|                                    based on a condition.                     |
|     3. distinct min;             > To get the value of smallest distinct     |
|                                    data-item.                                |
|     4. distinct conditional min; > To get the value of smallest distinct     |
|                                    data-item based on a condition.           |
|                                                                              |
| VI. Summation Of Data-Items In A Column:                                     |
|     1. sum;                      > To get the sum of all data-items.         |
|     2. conditional sum;          > To get the sum of all data-items based    |
|                                    on a condition.                           |
|     3. distinct sum;             > To get the sum of all distinct            |
|                                    data-items.                               |
|     4. distinct conditional sum; > To get the sum of all distinct            |
|                                    data-items based on a condition.          |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| ADVANCE MODE FOR PURE MYSQL QUERY                                            |
|                                                                              |
| advance mode;      > To enter advance mode to execute pure MySQL query.      |
| exit advance mode; > To exit advance mode and enter LessSQL mode.            |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR USER MANAGEMENT:                                                |
|                                                                              |
| create user;  > To create a new user and grant all permissions.              |
| reveal users; > To show information about all of the users.                  |
| delete user;  > To delete an user.                                           |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMANDS FOR ENGINE MANAGEMENT:                                              |
|                                                                              |
| show default engine;   > To show the default engine.                         |
| change default engine; > To change the default engine.                       |
| show table engine;     > To show the default engine for a table.             |
| change table engine;   > To change the default engine for a table.           |
|                                                                              |
|______________________________________________________________________________|
|______________________________________________________________________________|
|                                                                              |
| COMMAND TO EXIT PySQL:                                                       |
|                                                                              |
| exit; > To exit PySQL.                                                       |
|                                                                              |
|------------------------------------------------------------------------------|
| For more help visit: https://github.com/Kunal-Kumar-Sahoo                    |
+------------------------------------------------------------------------------+
""")


def user_info():
    print(r"""
+------------------------------------------------------------+
| Welcome to PySQL Database Management Client                |
| Version: 5.3.5                                             |
|                                                            |
| Copyright (c) 2021 Kunal Kumar Sahoo                       |
|                                                            |
| This program comes with ABSOLUTELY NO WARRANTY.            |
|                                                            |
| For more info and updates visit:                           |
| https://github.com/Kunal-Kumar-Sahoo                       |
|                                                            |
| Commands end with ;                                        |
|                                                            |
| To cancel any input statement type '\c'                    |
|                                                            |
|                                                            |
| Type 'help;' or '\h;' for help. To exit type 'exit;'       |
+------------------------------------------------------------+
""")


if __name__ == '__main__':
    main()
