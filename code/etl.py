import psycopg2
from etl_functions import *

# todo - remove second config var
postgres_config_file_dir = "code/python/config_file.txt"
postgres_config_file_dir = "config_file.txt"  # remove!

postgres_config, db_name, input_data_dir, raw_data_table_name, \
        fact_table_name, dimension_table_name =read_config_file(postgres_config_file_dir)

# create connection to postgres
con = psycopg2.connect(postgres_config)
cur = con.cursor()
con.autocommit = True

# todo - store a log in a text file
# todo - write tests to check that inputs exist and work, 
    # if they don't store it in the log file, 
    # which should be accessible after running docker even if the code fails

# check if a database exists, if not, create one
create_db_if_none_exists(cur, db_name)
# cur.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = '%s'" % db_name)
# not_exists_row = cur.fetchone()
# not_exists = not_exists_row[0]
# if not_exists:
#     print("..creating database..")
#     cur.execute("CREATE DATABASE %s  ;" % db_name)
# else:
#     print("database '%s' already exists" % db_name)
# print()

# read in the first row of the data file for the names of the columns
names_formatted = get_column_names(input_data_dir)
# with open(input_data_dir, "r") as f:
#     names = f.readline()
# names = names.split("\t")
# names[-1] = names[-1][:-1]  # remove the newline character from last column name

# names_formatted = [col.replace(" ", "_") for col in names]  # replace spaces with underscores
# print("Column names being used = %s" % names_formatted)

# Create Star Schema: divide columns into facts and dimensions
names_types_str, \
        fact_names_types_str, fact_names_formatted, fact_names_formatted_str, \
        dimension_names_types_str, dimension_names_formatted, dimension_names_formatted_str = \
        get_facts_and_dimensions(names_formatted)
# names_types_list = []
# fact_names_formatted = []
# fact_names_types_list = []
# dimension_names_formatted = []
# dimension_names_types_list = []
# for col in names_formatted:
#     if col == "sample":
#         names_types_list.append(col + " varchar(20) PRIMARY KEY")
#     elif "dim" == col[:3]:
#         names_types_list.append(col + " float8")
#         fact_names_formatted.append(col)
#         fact_names_types_list.append(col + " float8")
#     else:
#         names_types_list.append(col + " varchar(20)")
#         dimension_names_formatted.append(col)
#         dimension_names_types_list.append(col + " varchar(20)")

# names_types_str = ", ".join(names_types_list)  # a string of the column names and their types to be read in by SQL

# fact_names_types_str = ", ".join(fact_names_types_list)  # a string of the fact column names and their types to be read in by SQL
# fact_names_formatted_str = ", ".join(fact_names_formatted)  # a string of the fact column names to be read in by SQL

# dimension_names_types_str = ", ".join(dimension_names_types_list)  # a string of the fact column names and their types to be read in by SQL
# dimension_names_formatted_str = ", ".join(dimension_names_formatted)  # a string of the fact column names to be read in by SQL

# print("Columns used in fact table = %s" % fact_names_formatted)
# print("Columns used in dimension table = %s\n" % dimension_names_formatted)

# create raw data table
drop_if_exists_and_create_table(cur, raw_data_table_name, names_types_str)
# print("..creating raw data table..\n")
# cur.execute("""DROP TABLE IF EXISTS %s;""" % raw_data_table_name)
# cur.execute("""CREATE TABLE %s(%s);""" % (raw_data_table_name, names_types_str))

# insert data into raw data table
print("..populating %s..\n" % raw_data_table_name)
with open(input_data_dir, 'r') as f:
    next(f)  # Skip the header row.
    cur.copy_from(f, raw_data_table_name, sep='\t')

# todo - chop the prefix from sample values and store the integer in the fact table as sample_id

# Fact table
# Needs to be dropped first if it exists, because of relationship to dimension table
drop_if_exists_and_create_table(cur, fact_table_name, fact_names_types_str + ", " + dimension_names_types_str)

# dimensions table
drop_if_exists_and_create_table(cur, dimension_table_name, dimension_names_types_str)
# cur.execute("""DROP TABLE IF EXISTS %s;""" % dimension_table_name)
# print("..creating dimensions table..\n")
# # create dimensions table
# cur.execute("""create table %s (%s);""" % (dimension_table_name, dimension_names_types_str))

print("..populating dimensions table..\n")
cur.execute("""insert into %s 
            select distinct %s 
            from %s ;""" % (dimension_table_name, dimension_names_formatted_str, raw_data_table_name))
cur.execute("""ALTER TABLE %s ADD COLUMN ID SERIAL PRIMARY KEY;""" % dimension_table_name)



# print("..creating fact table..\n")
# cur.execute("""create table %s (%s);""" % (fact_table_name, 
#                                            fact_names_types_str + ", " + dimension_names_types_str))
print("..populating fact table..\n")
cur.execute("""insert into %s
            select %s 
            from %s;""" % (fact_table_name, 
                           fact_names_formatted_str + ", " + dimension_names_formatted_str, 
                           raw_data_table_name))
cur.execute("""ALTER TABLE %s ADD COLUMN dimension_ID_FK int;""" % (fact_table_name)) 

print("..creating foreign key from fact table to dimensions table..\n")
fact_table_where_query_list = []
for col in dimension_names_formatted:
    fact_table_where_query_list.append("%s.%s=%s.%s" % (fact_table_name, col, dimension_table_name, col))
fact_table_where_query_str = " AND ".join(fact_table_where_query_list)
cur.execute("""
                UPDATE %s SET dimension_ID_FK=%s.id 
                FROM %s
                WHERE %s;""" % 
                    (fact_table_name,
                    dimension_table_name,
                    dimension_table_name,
                    fact_table_where_query_str
                    )
           )
cur.execute("""ALTER TABLE %s ALTER dimension_ID_FK SET NOT NULL;""" % fact_table_name)
cur.execute("""ALTER TABLE %s ADD CONSTRAINT "dimension_ID_FK_constraint"
     FOREIGN KEY (dimension_ID_FK) REFERENCES %s(id);""" % (fact_table_name,
                                                                        dimension_table_name))

# remove dimension columns from fact table
for col in dimension_names_formatted:
    cur.execute(""" ALTER TABLE %s DROP COLUMN %s;""" % (fact_table_name, col))
    
# count the number of records in the new database when the fact and dimension tables are joined
cur.execute("""select count(*) from fact_table left join dimension_table
on fact_table.dimension_ID_FK = dimension_table.id
""")
print("number of rows = ", cur.fetchone())

# inspect the first row of the fact and dimension tables when joined
cur.execute("""select * from fact_table left join dimension_table
on fact_table.dimension_ID_FK = dimension_table.id
""")
print("first row = ", cur.fetchone())

con.close()

print("..done..")