# Databricks notebook source
# Fifa19 data

# File location and type
file_location = "/FileStore/tables/fifa19.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

import unicodedata
import sys

from pyspark.sql.functions import translate

def get_accent_lookup():
    matching_string = ""
    replace_string = ""

    for i in range(ord(" "), sys.maxunicode):
        name = unicodedata.name(chr(i), "")
        if "WITH" in name:
            try:
                base = unicodedata.lookup(name.split(" WITH")[0])
                matching_string += chr(i)
                replace_string += base
            except KeyError:
                pass
    return matching_string, replace_string

def clean_text(c):
    matching_string, replace_string = get_accent_lookup()
    return translate(c, matching_string, replace_string)

@udf
def replace_discrepancies(c):
    return c.replace(" Jr","").replace("-","").replace("ð","d")

@udf
def lastName(c):
    if c is not None and " " in c:
        c = c[(c.index(" ")+1):]
    return c  

@udf
def formatTeam(c):
    if c is not None:
        c = c.replace("Utd","United").replace("FC ","").replace("Bayern München","Bayern Munich") 
    return c
  
df = df.withColumn("Name", lower(clean_text(replace_discrepancies("Name"))))
df = df.withColumn("Last_Name", lower(clean_text(lastName(replace_discrepancies("Name")))))
df = df.withColumn("club", formatTeam("club"))


# Create a view or table

temp_table_name = "fifa19"

df.createOrReplaceTempView(temp_table_name)

display(df)

# COMMAND ----------

# Football events data

# File location and type
file_location = "/FileStore/tables/football_events.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

@udf
def formatName(c):
    if c is not None and " " in c:
        c = c[0] + "." + c[c.index(" "):]
    return c
  
@udf
def lastName(c):
    if c is not None and " " in c:
        c = c[(c.index(" ")+1):]
    return c
  
@udf
def formatTeam(c):
    return c.replace("Utd","United").replace("FC ","").replace("Tottenham","Tottenham Hotspur")
  
  
df = df.withColumn("name", formatName("player"))
df = df.withColumn("last_name", lastName("player"))
df = df.withColumn("event_team", formatTeam("event_team"))

# Create a view or table

temp_table_name = "football_events"

df.createOrReplaceTempView(temp_table_name)

display(df)

# COMMAND ----------

# Football games data

# File location and type
file_location = "/FileStore/tables/football_games.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

temp_table_name = "football_games"

df.createOrReplaceTempView(temp_table_name)

display(df)

# Code continued in Soccer_Queries.hql
