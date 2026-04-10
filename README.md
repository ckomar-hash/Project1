This program aims to read processed airline data from Part A of the project and performs normalization using functional dependencies.
It does so by checking 1NF, 2NF, 3NF, and BCNF, generates SQL statements, creates tables in MySQL, and provides a query interface to perform simple tasks.

Installation and Usage Instructions
1. Ensure MySQL is running and the database airline_project exists
2. Place partA_data.csv in the same folder as part_b.py
3. Run "python part_b.py"

The primary key should be entered using +
firstname+lastname+source_airport+dest_airport+requested_class+assigned_class+npass

In addition, functional dependencies should be comma-separated
firstname+lastname+source_airport+dest_airport+requested_class+assigned_class+npass

The program provides a menu to
1. View tables
2. Run custom SELECT queries
3. Insert rows
4. Update rows
5. Delete rows

NOTE: Table display is currently limited to 10 rows at a time for readability since the data is very large
Updates are also limited to non-key attributes, for example npass
