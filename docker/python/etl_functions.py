def read_config_file(path_to_file):
    with open(path_to_file, "r") as f:
        config_file = f.read()

    postgres_config = config_file.split("\n")[0]
    print("postgres_config =", postgres_config)
    
    db_name = postgres_config.split("'")[1]
    print("db_name = ", db_name)

    input_data_dir = config_file.split("\n")[1].split("=")[1]
    print("input_data_dir =", input_data_dir)

    raw_data_table_name = config_file.split("\n")[2].split("=")[1]  #"raw_data_table"    
    print("raw_data_table_name =", raw_data_table_name)
    
    fact_table_name = config_file.split("\n")[3].split("=")[1]  #"fact_table"
    print("fact_table_name =", fact_table_name)
    
    dimension_table_name = config_file.split("\n")[4].split("=")[1]  #"dimension_table"
    print("dimension_table_name =", dimension_table_name)
    print()
    
    return postgres_config, db_name, input_data_dir, raw_data_table_name, fact_table_name, dimension_table_name


def create_db_if_none_exists(cur, db_name):
    cur.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = '%s'" % db_name)
    not_exists_row = cur.fetchone()
    not_exists = not_exists_row[0]
    if not_exists:
        print("..creating database..")
        cur.execute("CREATE DATABASE %s  ;" % db_name)
    else:
        print("database '%s' already exists" % db_name)
    print()    
    
    
def get_column_names(input_data_dir):
    with open(input_data_dir, "r") as f:
        names = f.readline()
    names = names.split("\t")
    names[-1] = names[-1][:-1]  # remove the newline character from last column name

    names_formatted = [col.replace(" ", "_") for col in names]  # replace spaces with underscores
    print("Column names being used = %s" % names_formatted)
    return names_formatted


def get_facts_and_dimensions(names_formatted):
    names_types_list = []
    fact_names_formatted = []
    fact_names_types_list = []
    dimension_names_formatted = []
    dimension_names_types_list = []
    for col in names_formatted:
        if col == "sample":
            names_types_list.append(col + " varchar(20) PRIMARY KEY")
        elif "dim" == col[:3]:
            names_types_list.append(col + " float8")
            fact_names_formatted.append(col)
            fact_names_types_list.append(col + " float8")
        else:
            names_types_list.append(col + " varchar(20)")
            dimension_names_formatted.append(col)
            dimension_names_types_list.append(col + " varchar(20)")

    names_types_str = ", ".join(names_types_list)  # a string of the column names and their types to be read in by SQL

    fact_names_types_str = ", ".join(fact_names_types_list)  # a string of the fact column names and their types to be read in by SQL
    fact_names_formatted_str = ", ".join(fact_names_formatted)  # a string of the fact column names to be read in by SQL

    dimension_names_types_str = ", ".join(dimension_names_types_list)  # a string of the fact column names and their types to be read in by SQL
    dimension_names_formatted_str = ", ".join(dimension_names_formatted)  # a string of the fact column names to be read in by SQL

    print("Columns used in fact table = %s" % fact_names_formatted)
    print("Columns used in dimension table = %s\n" % dimension_names_formatted)
    return names_types_str, \
            fact_names_types_str, fact_names_formatted, fact_names_formatted_str, \
            dimension_names_types_str, dimension_names_formatted, dimension_names_formatted_str


def drop_if_exists_and_create_table(cur, table_name, names_and_types):
    # create raw data table
    print("..creating %s..\n" % table_name)
    cur.execute("""DROP TABLE IF EXISTS %s;""" % table_name)
    cur.execute("""CREATE TABLE %s(%s);""" % (table_name, names_and_types))

    