import psycopg2
from etl_functions import *

postgres_config_file_dir = "code/python/config_file.txt"
# postgres_config_file_dir = "config_file.txt"

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

# read in the first row of the data file for the names of the columns
names_formatted = get_column_names(input_data_dir)

# Create Star Schema: divide columns into facts and dimensions
names_types_str, \
        fact_names_types_str, fact_names_formatted, fact_names_formatted_str, \
        dimension_names_types_str, dimension_names_formatted, dimension_names_formatted_str = \
        get_facts_and_dimensions(names_formatted)

# create raw data table
drop_if_exists_and_create_table(cur, raw_data_table_name, names_types_str)

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

print("..populating dimensions table..\n")
cur.execute("""insert into %s 
            select distinct %s 
            from %s ;""" % (dimension_table_name, dimension_names_formatted_str, raw_data_table_name))
cur.execute("""ALTER TABLE %s ADD COLUMN ID SERIAL PRIMARY KEY;""" % dimension_table_name)

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
    
# create indexes
for col in fact_names_formatted:
    cur.execute("""CREATE INDEX idx_%s ON %s(%s)""" % (col, fact_table_name, col))
    
for col in dimension_names_formatted:
    cur.execute("""CREATE INDEX idx_%s ON %s(%s)""" % (col, dimension_table_name, col))
    
# count the number of records in the new database when the fact and dimension tables are joined
cur.execute("""select count(*) from %s left join %s
            on %s.dimension_ID_FK = %s.id
            """ % (fact_table_name, dimension_table_name, fact_table_name, dimension_table_name))
fact_dim_rows = cur.fetchone()
print("number of rows in fact/dimension join = ", fact_dim_rows)

cur.execute("""select count(*) from %s""" % raw_data_table_name)
raw_rows = cur.fetchone()
print("Number of rows in fact/dimension join equal to number of rows in raw data table: ", fact_dim_rows == raw_rows)

# inspect the first row of the fact and dimension tables when joined
cur.execute("""select * from %s left join %s
            on %s.dimension_ID_FK = %s.id
            """ % (fact_table_name, dimension_table_name, fact_table_name, dimension_table_name))
print("\nfirst row of fact/dim join = ", cur.fetchone())

con.close()

print("..done..")
