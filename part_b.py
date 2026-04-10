import pandas as pd
import mysql.connector

# load CSV into pandas DataFrame
df = pd.read_csv("partA_data.csv")

print("Number of rows:", df.shape[0])
print("Number of columns:", df.shape[1])

print("\nColumns:")
print(df.columns)

print("\nData types:")
print(df.dtypes)

print("\nSample data:")
print(df.head())

# parse functional dependencies entered by user
def parse_fd_input(fd_input):
    fds = []
    for fd in fd_input.split(","):
        fd = fd.strip()
        if "->" in fd:
            left, right = fd.split("->")
            lhs = [x.strip() for x in left.strip().split("+") if x.strip() != ""]
            rhs = [x.strip() for x in right.strip().split("+") if x.strip() != ""]
            fds.append((lhs, rhs))
    return fds

# compute closure of a set of attributes
def closure(attrs, fds):
    result = set(attrs)

    changed = True
    while changed:
        changed = False
        for lhs, rhs in fds:
            if set(lhs).issubset(result):
                old_size = len(result)
                result.update(rhs)
                if len(result) > old_size:
                    changed = True

    return result

# check if a set of attributes is a superkey
def is_superkey(attrs, relation_attrs, fds):
    return set(relation_attrs).issubset(closure(attrs, fds))


def unique_list(seq):
    seen = set()
    out = []
    for item in seq:
        t = tuple(sorted(item))
        if t not in seen:
            seen.add(t)
            out.append(item)
    return out


all_attrs = list(df.columns)

# get primary key and functional dependencies from user
print("\nEnter Primary Key:")
pk = input().strip()
pk_list = [x.strip() for x in pk.split("+") if x.strip() != ""]

print("\nEnter Functional Dependencies (format: A->B, separate with commas):")
fd_input = input().strip()
fds = parse_fd_input(fd_input)

print("\nPrimary Key:", pk)
print("Functional Dependencies:", fds)

pk_closure = closure(pk_list, fds)

print("\nClosure of primary key:")
print(pk_closure)

if set(all_attrs).issubset(pk_closure):
    print("\nThe entered primary key can determine all attributes.")
else:
    print("\nThe entered primary key does NOT determine all attributes.")

print("\nAll attributes in dataset:")
print(set(all_attrs))

# 1NF check (each field contains only one value)
print("\n--- 1NF Check ---")
print("Assuming data is already 1NF because each CSV field holds one value.")


prime_attrs = set(pk_list)
non_prime_attrs = set(all_attrs) - prime_attrs

# detect and resolve partial dependencies (2NF)
print("\n--- 2NF Check and Resolution ---")
partial_dependencies = []

for lhs, rhs in fds:
    lhs_set = set(lhs)
    rhs_set = set(rhs)

    if lhs_set.issubset(prime_attrs) and lhs_set != prime_attrs:
        moved = list(rhs_set.intersection(non_prime_attrs))
        if len(moved) > 0:
            partial_dependencies.append((lhs, moved))

if len(partial_dependencies) == 0:
    print("No partial dependencies found.")
else:
    print("Partial dependencies found:")
    for lhs, rhs in partial_dependencies:
        print(lhs, "->", rhs)


tables_2nf = []
main_table_attrs = set(all_attrs)

for lhs, rhs in partial_dependencies:
    new_table = list(dict.fromkeys(lhs + rhs))
    tables_2nf.append(new_table)

    for attr in rhs:
        if attr in main_table_attrs:
            main_table_attrs.remove(attr)

tables_2nf.append(list(main_table_attrs))
tables_2nf = unique_list(tables_2nf)

print("\n2NF Tables:")
for i, table in enumerate(tables_2nf, start=1):
    print("Table", i, ":", table)

# detect and resolve transitive dependencies (3NF)
print("\n--- 3NF Check and Resolution ---")
tables_3nf = []

for table in tables_2nf:
    table_set = set(table)
    current_attrs = set(table)
    created_tables = []

    for lhs, rhs in fds:
        lhs_set = set(lhs)
        rhs_set = set(rhs)

        if lhs_set.issubset(table_set) and rhs_set.issubset(table_set):
            if not is_superkey(lhs, table, fds):
                if not rhs_set.issubset(set(lhs)):
                    new_table = list(dict.fromkeys(lhs + rhs))
                    created_tables.append(new_table)

                    for attr in rhs:
                        if attr in current_attrs and attr not in lhs:
                            current_attrs.remove(attr)

    for t in created_tables:
        tables_3nf.append(t)

    tables_3nf.append(list(current_attrs))

tables_3nf = unique_list(tables_3nf)

print("\n3NF Tables:")
for i, table in enumerate(tables_3nf, start=1):
    print("Table", i, ":", table)

# perform BCNF decomposition
print("\n--- BCNF Check and Decomposition ---")
tables_bcnf = []

for table in tables_3nf:
    table_set = set(table)
    changed = False

    for lhs, rhs in fds:
        lhs_set = set(lhs)
        rhs_set = set(rhs)

        if lhs_set.issubset(table_set) and rhs_set.issubset(table_set):
            if not is_superkey(lhs, table, fds):
                table1 = list(dict.fromkeys(lhs + rhs))
                table2 = [x for x in table if x not in rhs_set or x in lhs_set]

                tables_bcnf.append(table1)
                tables_bcnf.append(table2)
                changed = True
                break

    if not changed:
        tables_bcnf.append(table)

tables_bcnf = unique_list(tables_bcnf)

print("\nBCNF Tables:")
for i, table in enumerate(tables_bcnf, start=1):
    print("Table", i, ":", table)

# generate SQL CREATE TABLE statements for normalized tables
print("\n--- SQL Script Generation ---")

def sql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "INT"
    return "VARCHAR(255)"


for i, table in enumerate(tables_bcnf, start=1):
    table_name = f"normalized_table_{i}"
    print(f"\nCREATE TABLE {table_name} (")

    lines = []
    for col in table:
        lines.append(f"    {col} {sql_type(df[col])}")

    print(",\n".join(lines))
    print(");")


# connect to MySQL and create/populate normalized tables
print("\n--- Database Creation ---")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="airline_project"
)

cursor = conn.cursor()

for i, table in enumerate(tables_bcnf, start=1):
    table_name = f"normalized_table_{i}"

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    column_defs = []
    for col in table:
        if pd.api.types.is_integer_dtype(df[col]):
            column_defs.append(f"{col} INT")
        else:
            column_defs.append(f"{col} VARCHAR(255)")

    create_sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
    cursor.execute(create_sql)

    cols = ", ".join(table)
    placeholders = ", ".join(["%s"] * len(table))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    unique_rows = df[table].drop_duplicates()

    for _, row in unique_rows.iterrows():
        cursor.execute(insert_sql, tuple(row[col] for col in table))

conn.commit()

print("Normalized tables created and populated.")

# simple menu to show basic operations
print("\nQuery Interface")

while True:
    print("\n1. Show normalized_table_1")
    print("2. Show normalized_table_2")
    print("3. Show normalized_table_3")
    print("4. Run custom SELECT query")
    print("5. Insert row")
    print("6. Update row")
    print("7. Delete row")
    print("8. Exit")

    choice = input("\nEnter choice: ")

    if choice == "1":
        cursor.execute("SELECT * FROM normalized_table_1 LIMIT 10")
        for row in cursor.fetchall():
            print(row)

    elif choice == "2":
        cursor.execute("SELECT * FROM normalized_table_2 LIMIT 10")
        for row in cursor.fetchall():
            print(row)

    elif choice == "3":
        cursor.execute("SELECT * FROM normalized_table_3 LIMIT 10")
        for row in cursor.fetchall():
            print(row)

    elif choice == "4":
        query = input("Enter SELECT query: ")
        if query.strip().lower().startswith("select"):
            cursor.execute(query)
            for row in cursor.fetchall():
                print(row)
        else:
            print("Only SELECT queries allowed.")

    elif choice == "5":
        table_num = input("Enter table number (1, 2, or 3): ")
        table_name = f"normalized_table_{table_num}"

        if table_num == "1":
            firstname = input("firstname: ")
            lastname = input("lastname: ")
            npass = input("npass: ")
            cursor.execute(
                f"INSERT INTO {table_name} (firstname, lastname, npass) VALUES (%s, %s, %s)",
                (firstname, lastname, int(npass))
            )
            conn.commit()
            print("Row inserted.")

        elif table_num == "2":
            requested_class = input("requested_class: ")
            assigned_class = input("assigned_class: ")
            cursor.execute(
                f"INSERT INTO {table_name} (requested_class, assigned_class) VALUES (%s, %s)",
                (requested_class, assigned_class)
            )
            conn.commit()
            print("Row inserted.")

        elif table_num == "3":
            dest_airport = input("dest_airport: ")
            source_airport = input("source_airport: ")
            requested_class = input("requested_class: ")
            firstname = input("firstname: ")
            lastname = input("lastname: ")
            cursor.execute(
                f"""INSERT INTO {table_name}
                (dest_airport, source_airport, requested_class, firstname, lastname)
                VALUES (%s, %s, %s, %s, %s)""",
                (dest_airport, source_airport, requested_class, firstname, lastname)
            )
            conn.commit()
            print("Row inserted.")

        else:
            print("Invalid table number.")

    elif choice == "6":
        table_num = input("Enter table number (1, 2, or 3): ")
        table_name = f"normalized_table_{table_num}"

        if table_num == "1":
            old_firstname = input("Enter firstname to find row: ")
            old_lastname = input("Enter lastname to find row: ")
            new_npass = input("Enter new npass: ")
            cursor.execute(
                f"""UPDATE {table_name}
                SET npass = %s
                WHERE firstname = %s AND lastname = %s""",
                (int(new_npass), old_firstname, old_lastname)
            )
            conn.commit()
            print("Row updated.")

        elif table_num == "2":
            old_requested = input("Enter requested_class to find row: ")
            new_assigned = input("Enter new assigned_class: ")
            cursor.execute(
                f"""UPDATE {table_name}
                SET assigned_class = %s
                WHERE requested_class = %s""",
                (new_assigned, old_requested)
            )
            conn.commit()
            print("Row updated.")

        elif table_num == "3":
            old_firstname = input("Enter firstname to find row: ")
            old_lastname = input("Enter lastname to find row: ")
            new_requested = input("Enter new requested_class: ")
            cursor.execute(
                f"""UPDATE {table_name}
                SET requested_class = %s
                WHERE firstname = %s AND lastname = %s""",
                (new_requested, old_firstname, old_lastname)
            )
            conn.commit()
            print("Row updated.")

        else:
            print("Invalid table number.")

    elif choice == "7":
        table_num = input("Enter table number (1, 2, or 3): ")
        table_name = f"normalized_table_{table_num}"

        if table_num == "1":
            firstname = input("Enter firstname to delete: ")
            lastname = input("Enter lastname to delete: ")
            cursor.execute(
                f"DELETE FROM {table_name} WHERE firstname = %s AND lastname = %s",
                (firstname, lastname)
            )
            conn.commit()
            print("Row deleted.")

        elif table_num == "2":
            requested_class = input("Enter requested_class to delete: ")
            cursor.execute(
                f"DELETE FROM {table_name} WHERE requested_class = %s",
                (requested_class,)
            )
            conn.commit()
            print("Row deleted.")

        elif table_num == "3":
            firstname = input("Enter firstname to delete: ")
            lastname = input("Enter lastname to delete: ")
            cursor.execute(
                f"DELETE FROM {table_name} WHERE firstname = %s AND lastname = %s",
                (firstname, lastname)
            )
            conn.commit()
            print("Row deleted.")

        else:
            print("Invalid table number.")

    elif choice == "8":
        break

    else:
        print("Invalid choice.")

cursor.close()
conn.close()
