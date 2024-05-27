import mysql.connector

def create_database(connection):
    """Create a new database."""
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS testyBoy3")
        print("Database 'testyBoy3' created successfully")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def connect_database():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='asus456kebab14',
            database='testyBoy3',  # database
        )
        print("Connection to DataBase successful")
    except mysql.connector.Error as e:
        print(f"Error '{e}' occurred")
    return connection

def create_connection():
    """Create a database connection."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',       # or another hostname if not local
            user='root',            # your MySQL username
            password='asus456kebab14'  # your MySQL password
        )
        print("Connection to MySQL DB successful")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    """Execute a SQL query."""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def fetch_query(connection, query):
    """Fetch results from a SQL query."""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = cursor.column_names
        return columns, result
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def create_tables(connection):
    """Create tables in the database."""
    create_customer_table = """
    CREATE TABLE IF NOT EXISTS customer(
        customerID INT AUTO_INCREMENT PRIMARY KEY,
        fname VARCHAR(255),
        lname VARCHAR(255),
        email VARCHAR(255),
        address VARCHAR(255)
    );
    """
    create_orders_table = """
    CREATE TABLE IF NOT EXISTS orders(
        orderID INT AUTO_INCREMENT PRIMARY KEY,
        customerID INT NOT NULL,
        startDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        orderStatus ENUM('pending', 'transport', 'delivered') DEFAULT 'pending',
        FOREIGN KEY (customerID) REFERENCES customer(customerID)
    );
    """
    create_product_type_table = """
    CREATE TABLE IF NOT EXISTS productType(
        typeID CHAR(1) PRIMARY KEY,
        description VARCHAR(255) NOT NULL,
        price DECIMAL(10, 2)
    );
    """
    create_product_table = """
    CREATE TABLE IF NOT EXISTS Product (
        productID INT AUTO_INCREMENT PRIMARY KEY,
        typeID CHAR(1),
        FOREIGN KEY (typeID) REFERENCES productType(typeID)
    );
    """
    create_transport_table = """
    CREATE TABLE IF NOT EXISTS transport(
        transportID INT AUTO_INCREMENT PRIMARY KEY,
        orderID INT NOT NULL,
        customerID INT NOT NULL,
        FOREIGN KEY (orderID) REFERENCES orders(orderID),
        FOREIGN KEY (customerID) REFERENCES customer(customerID)
    );
    """
    create_order_status_changes_table = """
    CREATE TABLE IF NOT EXISTS orderStatusChanges(
        statusChangeID INT AUTO_INCREMENT PRIMARY KEY,
        orderID INT,
        orderStatus ENUM('pending', 'transport', 'delivered'),
        statusChangedOn TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (orderID) REFERENCES orders(orderID)
    );
    """
    create_sales_table = """
    CREATE TABLE IF NOT EXISTS sales(
        salesID INT AUTO_INCREMENT PRIMARY KEY,
        customerID INT NOT NULL,
        orderID INT NOT NULL,
        typeID CHAR(1),
        quantity INT DEFAULT 1,
        totalCost DECIMAL(10, 2),
        FOREIGN KEY (orderID) REFERENCES orders(orderID),
        FOREIGN KEY (customerID) REFERENCES customer(customerID),
        FOREIGN KEY (typeID) REFERENCES productType(typeID)
    );
    """
    execute_query(connection, create_customer_table)
    execute_query(connection, create_orders_table)
    execute_query(connection, create_product_type_table)
    execute_query(connection, create_product_table)
    execute_query(connection, create_transport_table)
    execute_query(connection, create_order_status_changes_table)
    execute_query(connection, create_sales_table)

def create_triggers_and_procedure(connection):
    """Create triggers and stored procedure in the database."""
    create_initial_order_trigger = """
    CREATE TRIGGER CreateInitialOrder AFTER INSERT ON customer
    FOR EACH ROW
    BEGIN
        INSERT INTO orders (customerID, startDate, orderStatus)
        VALUES (NEW.customerID, NOW(), 'pending');
    END;
    """
    compute_sale_total_trigger = """
    CREATE TRIGGER ComputeSaleTotal BEFORE INSERT ON sales
    FOR EACH ROW
    BEGIN
        DECLARE unitPrice DECIMAL(10,2);
        SELECT price INTO unitPrice FROM productType WHERE typeID = NEW.typeID;
        SET NEW.totalCost = unitPrice * NEW.quantity;
    END;
    """
    update_sale_total_trigger = """
    CREATE TRIGGER UpdateSaleTotal BEFORE UPDATE ON sales
    FOR EACH ROW
    BEGIN
        DECLARE unitPrice DECIMAL(10,2);
        SELECT price INTO unitPrice FROM productType WHERE typeID = NEW.typeID;
        SET NEW.totalCost = unitPrice * NEW.quantity;
    END;
    """
    create_products_after_sale_insert_trigger = """
    CREATE TRIGGER CreateProductsAfterSaleInsert
    AFTER INSERT ON sales
    FOR EACH ROW
    BEGIN
        DECLARE i INT DEFAULT 0;
        WHILE i < NEW.quantity DO
            INSERT INTO Product (typeID) VALUES (NEW.typeID);
            SET i = i + 1;
        END WHILE;
    END;
    """
    update_order_status_after_change_trigger = """
    CREATE TRIGGER UpdateOrderStatusAfterChange
    AFTER INSERT ON orderStatusChanges
    FOR EACH ROW
    BEGIN
        UPDATE orders
        SET orderStatus = NEW.orderStatus
        WHERE orderID = NEW.orderID;
    END;
    """
    create_transport_trigger = """
    CREATE TRIGGER after_transport_insert
    AFTER INSERT ON transport
    FOR EACH ROW
    BEGIN
        INSERT INTO orderStatusChanges (orderID, orderStatus)
        VALUES (NEW.orderID, 'transport');
    END;
    """
    add_sale_procedure = """
    CREATE PROCEDURE AddSale(IN customer_id INT, IN type_id CHAR(1), IN quantity INT)
    BEGIN
        DECLARE order_id INT;

        -- Attempt to find an existing order for the customer
        SELECT orderID INTO order_id FROM orders
        WHERE customerID = customer_id
        ORDER BY startDate DESC
        LIMIT 1;

        -- Insert the new sale
        INSERT INTO sales (customerID, orderID, typeID, quantity)
        VALUES (customer_id, order_id, type_id, quantity);
    END;
    """
    add_wipe_procedure = """
    CREATE PROCEDURE wipe_database()
    BEGIN
        SET FOREIGN_KEY_CHECKS = 0;
        TRUNCATE TABLE transport;
        TRUNCATE TABLE sales;
        TRUNCATE TABLE orderStatusChanges;
        TRUNCATE TABLE Product;
        TRUNCATE TABLE productType;
        TRUNCATE TABLE orders;
        TRUNCATE TABLE customer;
        SET FOREIGN_KEY_CHECKS = 1;
    END;
    """
    
    execute_query(connection, create_initial_order_trigger)
    execute_query(connection, compute_sale_total_trigger)
    execute_query(connection, update_sale_total_trigger)
    execute_query(connection, create_products_after_sale_insert_trigger)
    execute_query(connection, update_order_status_after_change_trigger)
    execute_query(connection, create_transport_trigger)
    execute_query(connection, add_sale_procedure)
    execute_query(connection, add_wipe_procedure)

def populate_tables(connection):
    """Populate tables with initial data."""
    insert_customers = """
    INSERT INTO customer (fname, lname, email, address) VALUES
    ('Bob', 'theBuilder', 'bob.thebuilder@gmail.com', 'Cape Town'),
    ('Alice', 'Wonderland', 'alice.wonderland@gmail.com', 'Fairyland'),
    ('John', 'Doe', 'john.doe@gmail.com', 'New York'),
    ('Jane', 'Smith', 'jane.smith@gmail.com', 'Los Angeles'),
    ('Michael', 'Johnson', 'michael.johnson@gmail.com', 'Chicago'),
    ('Emily', 'Davis', 'emily.davis@gmail.com', 'Houston'),
    ('David', 'Wilson', 'david.wilson@gmail.com', 'Phoenix'),
    ('Sarah', 'Martinez', 'sarah.martinez@gmail.com', 'San Antonio'),
    ('James', 'Garcia', 'james.garcia@gmail.com', 'San Diego'),
    ('Linda', 'Rodriguez', 'linda.rodriguez@gmail.com', 'Dallas'),
    ('Robert', 'Lopez', 'robert.lopez@gmail.com', 'San Jose'),
    ('Patricia', 'Gonzales', 'patricia.gonzales@gmail.com', 'Austin'),
    ('Charles', 'Hernandez', 'charles.hernandez@gmail.com', 'Jacksonville'),
    ('Barbara', 'Moore', 'barbara.moore@gmail.com', 'Fort Worth'),
    ('Joseph', 'Taylor', 'joseph.taylor@gmail.com', 'Columbus'),
    ('Elizabeth', 'Anderson', 'elizabeth.anderson@gmail.com', 'Charlotte'),
    ('Thomas', 'Thomas', 'thomas.thomas@gmail.com', 'San Francisco'),
    ('Jessica', 'Jackson', 'jessica.jackson@gmail.com', 'Indianapolis'),
    ('Christopher', 'White', 'christopher.white@gmail.com', 'Seattle'),
    ('Karen', 'Harris', 'karen.harris@gmail.com', 'Denver');
    """
    insert_product_types = """
    INSERT INTO productType (typeID, description, price) VALUES
    ('A', 'Electronics', 299.99),
    ('B', 'Books', 19.99),
    ('C', 'Clothing', 49.99);
    """
    insert_products = """
    INSERT INTO Product (typeID) VALUES
    ('A'),
    ('B'),
    ('C');
    """
    insert_status_changes = """
    INSERT INTO orderStatusChanges (orderID, orderStatus) values
    (4, 'transport'),
    (5, 'delivered'),
    (4, 'delivered'),
    (7, 'transport');
    """
    execute_query(connection, insert_customers)
    execute_query(connection, insert_product_types)
    execute_query(connection, insert_products)
    execute_query(connection, insert_status_changes)
    add_sale(connection, 1, 'A', 3)
    add_sale(connection, 1, 'C', 2)
    add_sale(connection, 2, 'A', 2)

def sales_additions(connection):
    add_sale(connection, 1, 'A', 3)
    add_sale(connection, 1, 'C', 2)
    add_sale(connection, 2, 'A', 2)

def add_sale(connection, customer_id, type_id, quantity):
    """Call the AddSale procedure to add a sale."""
    cursor = connection.cursor()
    try:
        cursor.callproc('AddSale', [customer_id, type_id, quantity])
        connection.commit()
        print(f"Sale added successfully for customer_id={customer_id}, type_id={type_id}, quantity={quantity}")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def wipe_database(connection):
    """Truncate all tables in the database without dropping them."""
    cursor = connection.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        tables = ['transport', 'sales', 'orderStatusChanges', 'Product', 'productType', 'orders', 'customer']
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table};")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        connection.commit()
        print("Database content wiped successfully")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def drop_database(connection, database_name):
    """Drop the specified database."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP DATABASE {database_name}")
        print(f"Database '{database_name}' dropped successfully")
    except mysql.connector.Error as e:
        print(f"The error '{e}' occurred: {e}")
    finally:
        cursor.close()

def test_tables(connection):
    """Test if tables are wiped and print their contents."""
    tables = ['customer', 'orders', 'productType', 'Product', 'transport', 'orderStatusChanges', 'sales']
    for table in tables:
        select_query = f"SELECT * FROM {table};"
        columns, results = fetch_query(connection, select_query)
        if not results:
            print(f"Table '{table}' is wiped (no rows).")
        else:
            print(f"Table '{table}' contents:")
            print(f"Columns: {columns}")
            for row in results:
                print(row)
        print("\n")

def run_queries(connection):
    """Run sample queries to verify the data."""
    # SELECT query for customer table
    print("Fetch all customers from the database")
    select_query = "SELECT * FROM customer;"
    columns, customers = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for customer in customers:
        print(customer)
    print("\n")

    # SELECT query for orders table
    print("Fetch all orders from the database")
    select_query = "SELECT * FROM orders;"
    columns, orders = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for order in orders:
        orderID, customerID, startDate, orderStatus = order
        formatted_date = startDate.strftime("%Y-%m-%d %H:%M:%S")
        print(orderID, customerID, formatted_date, orderStatus)
    print("\n")

    # SELECT query for productType table
    print("Fetch all product types from the database")
    select_query = "SELECT * FROM productType;"
    columns, product_types = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for product_type in product_types:
        print(product_type)
    print("\n")

    # SELECT query for product table
    print("Fetch all products from the database")
    select_query = "SELECT * FROM Product;"
    columns, products = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for product in products:
        print(product)
    print("\n")

    # SELECT query for transport table
    print("Fetch all transports from the database")
    select_query = "SELECT * FROM transport;"
    columns, transports = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for transport in transports:
        print(transport)
    print("\n")

    # SELECT query for orderStatusChanges table
    print("Fetch all order status changes from the database")
    select_query = "SELECT * FROM orderStatusChanges;"
    columns, order_status_changes = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for order_status_change in order_status_changes:
        print(order_status_change)
    print("\n")

    # SELECT query for sales table
    print("Fetch all sales from the database")
    select_query = "SELECT * FROM sales;"
    columns, sales = fetch_query(connection, select_query)
    print(f"Columns: {columns}")
    for sale in sales:
        print(sale)
    print("\n")

def fetch_advanced_queries(connection):
    """Fetch information using advanced SQL queries."""
    # SQL JOIN query
    print("Join customers with their orders:")
    join_query = """
    SELECT c.fname, c.lname, o.orderID, o.startDate, o.orderStatus
    FROM customer c
    JOIN orders o ON c.customerID = o.customerID;
    """
    columns, join_results = fetch_query(connection, join_query)
    print(f"Columns: {columns}")
    for result in join_results:
        fname, lname, orderID, startDate, orderStatus = result
        formatted_date = startDate.strftime("%Y-%m-%d %H:%M:%S")
        print(fname, lname, orderID, formatted_date, orderStatus)
    print("\n")

    # Aggregation or grouping query
    print("Count of orders per customer:")
    group_query = """
    SELECT c.fname, c.lname, COUNT(o.orderID) as order_count
    FROM customer c
    JOIN orders o ON c.customerID = o.customerID
    GROUP BY c.customerID;
    """
    columns, group_results = fetch_query(connection, group_query)
    print(f"Columns: {columns}")
    for result in group_results:
        print(result)
    print("\n")

    # Multi-relation query 1
    print("Fetch products and their types with orders:")
    multi_relation_query1 = """
    SELECT p.productID, p.typeID, pt.description, s.orderID, s.quantity
    FROM Product p
    JOIN productType pt ON p.typeID = pt.typeID
    JOIN sales s ON p.typeID = s.typeID;
    """
    columns, multi_relation_results1 = fetch_query(connection, multi_relation_query1)
    print(f"Columns: {columns}")
    for result in multi_relation_results1:
        print(result)
    print("\n")

    # Multi-relation query 2
    print("Fetch customer information with sales details:")
    multi_relation_query2 = """
    SELECT c.fname, c.lname, s.salesID, s.typeID, s.quantity, s.totalCost
    FROM customer c
    JOIN sales s ON c.customerID = s.customerID;
    """
    columns, multi_relation_results2 = fetch_query(connection, multi_relation_query2)
    print(f"Columns: {columns}")
    for result in multi_relation_results2:
        print(result)
    print("\n")

def reset(connection):
    """Reset the database to its original state and populate the tables."""
    wipe_database(connection)
    create_tables(connection)
    create_triggers_and_procedure(connection)
    populate_tables(connection)
    sales_additions(connection)

def setup():
    connection = create_connection()
    create_database(connection)
    connection.close()

def print_menu():
        print("\nSelect an option:")
        print("1. View table contents")
        print("2. Perform JOIN queries")
        print("3. Specific join queries")
        print("4. Exit")

def print_tables_menu():
        print("\nSelect a table to view:")
        print("1. customer")
        print("2. orders")
        print("3. productType")
        print("4. Product")
        print("5. transport")
        print("6. orderStatusChanges")
        print("7. sales")
        print("8. Back to main menu")

def print_joins_menu():
        print("\nSelect a JOIN query to perform:")
        print("1. Join customers with their orders")
        print("2. Join products with their types and orders")
        print("3. Join customers with sales details")
        print("4. Back to main menu")

def print_specific_joins_menu():
        print("\nSelect a specific JOIN query to perform:")
        print("1. Join customers with their orders for all 'pending' orders")
        print("2. Join customers with their total amount of orders")
        print("3. Join the latest change to an order for orderID = 5")
        print("4. Back to main menu")

def manual(connection):
    """Interactive test environment for the database."""

    def fetch_table_data(table_name):
        select_query = f"SELECT * FROM {table_name};"
        columns, results = fetch_query(connection, select_query)
        if columns is None or results is None:
            print(f"Table '{table_name}' does not exist.")
        elif not results:
            print(f"Table '{table_name}' is empty.")
        else:
            print(f"Table '{table_name}' contents:")
            print(f"Columns: {columns}")
            for row in results:
                print(row)
        print("\n")

    def perform_join_query(option):
        if option == 1:
            join_query = """
            SELECT c.fname, c.lname, o.orderID, o.startDate, o.orderStatus
            FROM customer c
            JOIN orders o ON c.customerID = o.customerID;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join customers with their orders:")
            print(f"Columns: {columns}")
        elif option == 2:
            join_query = """
            SELECT p.productID, p.typeID, pt.description, s.orderID, s.quantity
            FROM Product p
            JOIN productType pt ON p.typeID = pt.typeID
            JOIN sales s ON p.typeID = s.typeID;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join products with their types and orders:")
            print(f"Columns: {columns}")
        elif option == 3:
            join_query = """
            SELECT c.fname, c.lname, s.salesID, s.typeID, s.quantity, s.totalCost
            FROM customer c
            JOIN sales s ON c.customerID = s.customerID;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join customers with sales details:")
            print(f"Columns: {columns}")
        else:
            return

        if columns is None or results is None:
            print("The join query failed.")
        elif not results:
            print("No results found for the join query.")
        else:
            for row in results:
                print(row)
        
        print("\n")
    
    def perform_specific_join_query(option):
        if option == 1:
            join_query = """
            SELECT c.fname, c.lname, o.orderID, o.startDate, o.orderStatus
            FROM customer c
            JOIN orders o ON c.customerID = o.customerID
            WHERE o.orderStatus = "pending"
            ORDER BY orderID ASC;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join customers with their orders for all 'pending' orders:")
            print(f"Columns: {columns}")
        elif option == 2:
            join_query = """
            SELECT c.fname, c.lname, COUNT(o.orderID) as order_count
            FROM customer c
            JOIN orders o ON c.customerID = o.customerID
            GROUP BY c.customerID;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join the customers with their total amount of orders.")
            print(f"Columns: {columns}")
        elif option == 3:
            join_query = """
            SELECT 
            osc.orderID, 
            osc.orderStatus, 
            osc.statusChangedOn AS lastStatusChangedOn, 
            (SELECT COUNT(*) 
             FROM orderStatusChanges 
             WHERE orderID = osc.orderID) AS AmountOfUpdates
            FROM orderStatusChanges osc
            WHERE 
                osc.orderID = 5
            ORDER BY 
                osc.statusChangedOn DESC 
            LIMIT 1;
            """
            columns, results = fetch_query(connection, join_query)
            print("Join the latest change to an order for orderID = 5.")
            print(f"Columns: {columns}")
        else:
            return

        if columns is None or results is None:
            print("The join query failed.")
        elif not results:
            print("No results found for the join query.")
        else:
            for row in results:
                print(row)

    while True:
        print_menu()
        main_option = input("Enter your choice: ")
        if main_option == "1":
            while True:
                print_tables_menu()
                table_option = input("Enter your choice: ")
                if table_option == "1":
                    fetch_table_data("customer")
                elif table_option == "2":
                    fetch_table_data("orders")
                elif table_option == "3":
                    fetch_table_data("productType")
                elif table_option == "4":
                    fetch_table_data("Product")
                elif table_option == "5":
                    fetch_table_data("transport")
                elif table_option == "6":
                    fetch_table_data("orderStatusChanges")
                elif table_option == "7":
                    fetch_table_data("sales")
                elif table_option == "8":
                    break
                else:
                    print("Invalid choice, please try again.")
        elif main_option == "2":
            while True:
                print_joins_menu()
                join_option = input("Enter your choice: ")
                if join_option in ["1", "2", "3"]:
                    perform_join_query(int(join_option))
                elif join_option == "4":
                    break
                else:
                    print("Invalid choice, please try again.")
        elif main_option == "3":
            while True:
                print_specific_joins_menu()
                join_option = input("Enter your choice: ")
                if join_option in ["1", "2", "3"]:
                    perform_specific_join_query(int(join_option))
                elif join_option == "4":
                    break
                else:
                    print("Invalid choice, please try again.")
        elif main_option == "4":
            print("Exiting the interactive test environment.")
            break
        else:
            print("Invalid choice, please try again.")
