import database_functions as df

def main():
    df.setup()
    connection = df.connect_database()
    if connection:
        # Create necessary tables and triggers etc.
        df.create_tables(connection)
        df.create_triggers_and_procedure(connection)
        # Populate the tables
        df.populate_tables(connection)
        # run queries for the tables
        df.run_queries(connection)
        # fetch advanced queries
        df.fetch_advanced_queries(connection)

        # USER INTERFACE:
        while True:
            print(f"Welcome to user interface\nThe following commands are at your disposal:\n"
              "1. Wipes the database\n"
              "2. Test the tables if they have been wiped\n"
              "3. Deletes the database\n"
              "4. Use for manual testing for specific tables\n"
              "5. Resets the tables to the orginal function with the orginal table population\n"
              "6. Ends the program")
            word = input("Select an option: \n")
            if word == '1':
                df.wipe_database(connection)
            elif word == "2":
                df.test_tables(connection)
            elif word == '3':
                df.drop_database(connection, 'testDatabase')
            elif word == '4':
                # jump to manual testing
                df.manual(connection)
            elif word == '5':
                #reset to factory new
                df.reset(connection)
            elif word == '6':
                break
        # close connection to database
        connection.close()

if __name__ == "__main__":
    main()
