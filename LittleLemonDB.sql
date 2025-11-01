import pandas as pd
import mysql.connector
from mysql.connector import Error

# -------------------------------
# ⚙️ Paramètres MySQL
# -------------------------------
config = {
    'host': 'localhost',
    'user': 'bracoul',
    'password': 'kungfu'
}

excel_path = "LittleLemon_data.xlsx"

try:
    # -------------------------------
    # 1 Connexion au serveur MySQL
    # -------------------------------
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    connection.autocommit = False
    print("✅ Connexion MySQL réussie")

    # -------------------------------
    # 2 Création des bases
    # -------------------------------
    cursor.execute("CREATE DATABASE IF NOT EXISTS LittleLemonDataWH CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;") ;
    cursor.execute("CREATE DATABASE IF NOT EXISTS LittleLemon CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;") ;

    # -------------------------------
    # 3 Lecture du fichier Excel
    # -------------------------------
    df = pd.read_excel(excel_path).fillna('')
    print(f"✅ {len(df)} lignes lues depuis {excel_path}")

    # -------------------------------
    # 4 Création du Data Warehouse
    # -------------------------------
    connection.database = 'LittleLemonDataWH'
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS LittleLemon_datas (
        RowNumber VARCHAR(255),
        OrderID VARCHAR(255),
        OrderDate DATE,
        DeliveryDate DATE,
        CustomerID VARCHAR(255),
        CustomerName VARCHAR(255),
        City VARCHAR(255),
        Country VARCHAR(255),
        PostalCode VARCHAR(255),
        CountryCode VARCHAR(255),
        Cost DECIMAL(10,2),
        Sales DECIMAL(10,2),
        Quantity INT,
        Discount DECIMAL(5,2),
        DeliveryCost DECIMAL(10,2),
        CourseName VARCHAR(255),
        CuisineName VARCHAR(255),
        StarterName VARCHAR(255),
        DessertName VARCHAR(255),
        Drink VARCHAR(255),
        Sides VARCHAR(255)
    );
    """)

    cursor.execute("TRUNCATE TABLE LittleLemon_datas;")

    insert_query = """
    INSERT INTO LittleLemon_datas VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    connection.commit()
    print("✅ Données importées dans LittleLemonDataWH.LittleLemon_datas")

    # -------------------------------
    # 5 Création du schéma LittleLemon
    # -------------------------------
    connection.database = 'LittleLemon'

    tables = [
        "OrderDetails", "Deliveries", "Orders", "Customers", "PostalCodes",
        "Countries", "Dates", "Courses", "Cuisine", "Drinks", "Starters", "Desserts", "Sides"
    ]
    for t in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {t};")

    schema_sql = """
    CREATE TABLE Cuisine (
      CuisineID VARCHAR(255) PRIMARY KEY,
      CuisineName VARCHAR(255) NOT NULL
    );

    CREATE TABLE Courses (
      CourseID VARCHAR(255) PRIMARY KEY,
      CourseName VARCHAR(255) NOT NULL,
      CuisineID VARCHAR(255),
      FOREIGN KEY (CuisineID) REFERENCES Cuisine (CuisineID)
    );

    CREATE TABLE Countries (
      CountryID VARCHAR(255) PRIMARY KEY,
      CountryCode VARCHAR(10),
      CountryName VARCHAR(100)
    );

    CREATE TABLE PostalCodes (
      PostalCodeID VARCHAR(255) PRIMARY KEY,
      PostalCode VARCHAR(20),
      City VARCHAR(100),
      CountryID VARCHAR(255),
      FOREIGN KEY (CountryID) REFERENCES Countries (CountryID)
    );

    CREATE TABLE Customers (
      CustomerID VARCHAR(255) PRIMARY KEY,
      CustomerName VARCHAR(255),
      PostalCodeID VARCHAR(255),
      FOREIGN KEY (PostalCodeID) REFERENCES PostalCodes (PostalCodeID)
    );

    CREATE TABLE Dates (
      DateID DATE PRIMARY KEY,
      Year INT GENERATED ALWAYS AS (YEAR(DateID)) STORED,
      Month INT GENERATED ALWAYS AS (MONTH(DateID)) STORED,
      Day INT GENERATED ALWAYS AS (DAY(DateID)) STORED
    );

    CREATE TABLE Orders (
      OrderID VARCHAR(255),
      OrdersDateID DATE,
      CustomerID VARCHAR(255),
      Discount DECIMAL(5,2),
      Cost DECIMAL(10,2),
      Sales DECIMAL(10,2),
      PRIMARY KEY (OrderID, OrdersDateID, CustomerID),
      FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID),
      FOREIGN KEY (OrdersDateID) REFERENCES Dates (DateID)
    );

    CREATE TABLE Starters (
      StarterID VARCHAR(255) PRIMARY KEY,
      StarterName VARCHAR(255),
      CuisineID VARCHAR(255),
      FOREIGN KEY (CuisineID) REFERENCES Cuisine (CuisineID)
    );

    CREATE TABLE Drinks (
      DrinkID VARCHAR(255) PRIMARY KEY,
      DrinkName VARCHAR(255)
    );

    CREATE TABLE Sides (
      SideID VARCHAR(255) PRIMARY KEY,
      SideName VARCHAR(255)
    );

    CREATE TABLE Desserts (
      DessertID VARCHAR(255) PRIMARY KEY,
      DessertName VARCHAR(255),
      CuisineID VARCHAR(255),
      FOREIGN KEY (CuisineID) REFERENCES Cuisine (CuisineID)
    );

    CREATE TABLE OrderDetails (
      OrderDetailID VARCHAR(255) PRIMARY KEY,
      OrderID VARCHAR(255),
      Quantity INT,
      CourseID VARCHAR(255),
      StarterID VARCHAR(255),
      DessertID VARCHAR(255),
      DrinkID VARCHAR(255),
      SideID VARCHAR(255),
      FOREIGN KEY (OrderID) REFERENCES Orders (OrderID),
      FOREIGN KEY (CourseID) REFERENCES Courses (CourseID),
      FOREIGN KEY (StarterID) REFERENCES Starters (StarterID),
      FOREIGN KEY (DessertID) REFERENCES Desserts (DessertID),
      FOREIGN KEY (DrinkID) REFERENCES Drinks (DrinkID),
      FOREIGN KEY (SideID) REFERENCES Sides (SideID)
    );

    CREATE TABLE Deliveries (
      DeliveryID VARCHAR(255) PRIMARY KEY,
      OrderID VARCHAR(255),
      DateID DATE,
      DeliveryCost DECIMAL(10,2),
      FOREIGN KEY (OrderID) REFERENCES Orders (OrderID),
      FOREIGN KEY (DateID) REFERENCES Dates (DateID)
    );
    """

    for stmt in schema_sql.split(';'):
        if stmt.strip():
            cursor.execute(stmt)
    connection.commit()
    print("✅ Schéma LittleLemon créé avec succès")

    # -------------------------------
    # 6 Création des triggers
    # -------------------------------
    triggers = [
        ("before_insert_cuisine", """
        CREATE TRIGGER before_insert_cuisine
        BEFORE INSERT ON Cuisine
        FOR EACH ROW
        BEGIN
            IF NEW.CuisineName IS NOT NULL AND NEW.CuisineName <> '' THEN
                SET NEW.CuisineID = REPLACE(NEW.CuisineName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_courses", """
        CREATE TRIGGER before_insert_courses
        BEFORE INSERT ON Courses
        FOR EACH ROW
        BEGIN
            IF NEW.CourseName IS NOT NULL AND NEW.CourseName <> '' THEN
                SET NEW.CourseID = CONCAT(REPLACE(NEW.CourseName, ' ', ''), NEW.CuisineID);
            END IF;
        END;
        """),
        ("before_insert_drinks", """
        CREATE TRIGGER before_insert_drinks
        BEFORE INSERT ON Drinks
        FOR EACH ROW
        BEGIN
            IF NEW.DrinkName IS NOT NULL AND NEW.DrinkName <> '' THEN
                SET NEW.DrinkID = REPLACE(NEW.DrinkName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_desserts", """
        CREATE TRIGGER before_insert_desserts
        BEFORE INSERT ON Desserts
        FOR EACH ROW
        BEGIN
            IF NEW.DessertName IS NOT NULL AND NEW.DessertName <> '' THEN
                SET NEW.DessertID = REPLACE(NEW.DessertName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_starters", """
        CREATE TRIGGER before_insert_starters
        BEFORE INSERT ON Starters
        FOR EACH ROW
        BEGIN
            IF NEW.StarterName IS NOT NULL AND NEW.StarterName <> '' THEN
                SET NEW.StarterID = REPLACE(NEW.StarterName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_sides", """
        CREATE TRIGGER before_insert_sides
        BEFORE INSERT ON Sides
        FOR EACH ROW
        BEGIN
            IF NEW.SideName IS NOT NULL AND NEW.SideName <> '' THEN
                SET NEW.SideID = REPLACE(NEW.SideName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_countries", """
        CREATE TRIGGER before_insert_countries
        BEFORE INSERT ON Countries
        FOR EACH ROW
        BEGIN
            IF NEW.CountryName IS NOT NULL AND NEW.CountryName <> '' THEN
                SET NEW.CountryID = REPLACE(NEW.CountryName, ' ', '');
            END IF;
        END;
        """),
        ("before_insert_postalcodes", """
        CREATE TRIGGER before_insert_postalcodes
        BEFORE INSERT ON PostalCodes
        FOR EACH ROW
        BEGIN
            IF NEW.PostalCode IS NOT NULL AND NEW.City <> '' THEN
                SET NEW.PostalCodeID = CONCAT(REPLACE(NEW.PostalCode, ' ', ''), REPLACE(NEW.City, ' ', ''), NEW.CountryID);
            END IF;
        END;
        """),
        ("before_insert_customers", """
        CREATE TRIGGER before_insert_customers
        BEFORE INSERT ON Customers
        FOR EACH ROW
        BEGIN
            IF NEW.CustomerName IS NOT NULL AND NEW.CustomerName <> '' THEN
                SET NEW.CustomerID = CONCAT(REPLACE(NEW.CustomerName, ' ', ''), NEW.PostalCodeID);
            END IF;
        END;
        """),
        ("before_insert_deliveries", """
        CREATE TRIGGER before_insert_deliveries
        BEFORE INSERT ON Deliveries
        FOR EACH ROW
        BEGIN
            IF NEW.DeliveryID IS NULL AND NEW.DeliveryCost IS NOT NULL THEN
                SET NEW.DeliveryID = CONCAT(
                    REPLACE(NEW.OrderID, ' ', ''),
                    NEW.DateID,
                    NEW.DeliveryCost
                );
            END IF;
        END;
        """)
    ]

    for name, sql in triggers:
        cursor.execute(f"DROP TRIGGER IF EXISTS {name};")
        cursor.execute(sql)
    connection.commit()
    print("✅ Triggers créés avec succès")

    # -------------------------------
    # 7 Remplissage automatique des tables
    # -------------------------------
    cursor.execute("""
        INSERT IGNORE INTO Cuisine (CuisineName)
        SELECT DISTINCT CuisineName FROM LittleLemonDataWH.LittleLemon_datas WHERE CuisineName <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Courses (CourseName, CuisineID)
        SELECT DISTINCT d.CourseName, c.CuisineID
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.Cuisine c ON d.CuisineName = c.CuisineName
        WHERE d.CourseName <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Drinks (DrinkName)
        SELECT DISTINCT Drink FROM LittleLemonDataWH.LittleLemon_datas WHERE Drink <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Desserts (DessertName, CuisineID)
        SELECT DISTINCT d.DessertName, c.CuisineID
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.Cuisine c ON d.CuisineName = c.CuisineName
        WHERE d.DessertName <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Starters (StarterName, CuisineID)
        SELECT DISTINCT d.StarterName, c.CuisineID
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.Cuisine c ON d.CuisineName = c.CuisineName
        WHERE d.StarterName <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Sides (SideName)
        SELECT DISTINCT Sides FROM LittleLemonDataWH.LittleLemon_datas WHERE Sides <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Countries (CountryCode, CountryName)
        SELECT DISTINCT CountryCode, Country FROM LittleLemonDataWH.LittleLemon_datas WHERE Country <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO PostalCodes (PostalCode, City, CountryID)
        SELECT DISTINCT d.PostalCode, d.City, c.CountryID
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.Countries c ON d.Country = c.CountryName
        WHERE d.PostalCode <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Customers (CustomerName, PostalCodeID)
        SELECT DISTINCT d.CustomerName, p.PostalCodeID
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.PostalCodes p ON d.PostalCode = p.PostalCode AND d.City = p.City
        WHERE d.CustomerName <> '';
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Dates (DateID)
        SELECT DISTINCT OrderDate FROM LittleLemonDataWH.LittleLemon_datas WHERE OrderDate IS NOT NULL;
    """)
    cursor.execute("""
        INSERT IGNORE INTO Dates (DateID)
        SELECT DISTINCT DeliveryDate FROM LittleLemonDataWH.LittleLemon_datas WHERE DeliveryDate IS NOT NULL;
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO Orders (OrderID, OrdersDateID, CustomerID, Discount, Cost, Sales)
        SELECT DISTINCT d.OrderID, d.OrderDate, c.CustomerID, d.Discount, d.Cost, d.Sales
        FROM LittleLemonDataWH.LittleLemon_datas d
        JOIN LittleLemon.Customers c ON d.CustomerName = c.CustomerName
        WHERE d.OrderID <> '';
    """)
    connection.commit()

    cursor.execute("""
       INSERT IGNORE INTO Deliveries (DeliveryID, OrderID, DateID, DeliveryCost)
       SELECT DISTINCT d.OrderID, d.OrderID, d.DeliveryDate, d.DeliveryCost
       FROM LittleLemonDataWH.LittleLemon_datas d
       WHERE d.DeliveryDate IS NOT NULL;
    """)
    connection.commit()

    cursor.execute("""
        INSERT IGNORE INTO OrderDetails (OrderDetailID, OrderID, Quantity, CourseID, StarterID, DessertID, DrinkID, SideID)
        SELECT DISTINCT 
            d.OrderID,
            d.OrderID,
            d.Quantity,
            co.CourseID,
            st.StarterID,
            de.DessertID,
            dr.DrinkID,
            si.SideID
        FROM LittleLemonDataWH.LittleLemon_datas d
        LEFT JOIN LittleLemon.Courses co ON d.CourseName = co.CourseName
        LEFT JOIN LittleLemon.Starters st ON d.StarterName = st.StarterName
        LEFT JOIN LittleLemon.Desserts de ON d.DessertName = de.DessertName
        LEFT JOIN LittleLemon.Drinks dr ON d.Drink = dr.DrinkName
        LEFT JOIN LittleLemon.Sides si ON d.Sides = si.SideName;
    """)
    connection.commit()

    # -------------------------------
    # 8 Résumé des insertions
    # -------------------------------
    tables_to_check = [
        "Cuisine", "Courses", "Drinks", "Desserts", "Starters", "Sides",
        "Countries", "PostalCodes", "Customers", "Dates", "Orders", "Deliveries", "OrderDetails"
    ]
    print("\n📊 Résumé des insertions :")
    for t in tables_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f" - {t:<15}: {count} lignes")

    print("\n🎉 Import complet terminé avec succès !")

except Error as e:
    print("❌ Erreur MySQL :", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("🔒 Connexion MySQL fermée.")