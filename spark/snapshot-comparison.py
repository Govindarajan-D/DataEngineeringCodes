## PARAMS

#Pass the table name of the table that contains the snapshots and the timestamp of snapshot to compare
TableName = 'schema_name.table_name'
SnapshotTime = 'snapshot_timestamp' #Can be passed as parameter or calculated value based on current date

#Add the Key columns and the columns to compare in the below respective arrays
key_columns = ["Key1","Key2"]
other_columns = ["OtherCol1","OtherCol2"]


#Pass the schema datatype of the key columns as dictionary
schema_string = "{'Key1':'StringType','Key2':'StringType'}"
# Snapshot comparison program that can compare two data snapshots and return a set of rows that contains the changes made on each column
# This program can run in Spark and Databricks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, lit, when, sha2, col, concat_ws, when
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, IntegerType
import ast
import sys


## CODE

spark_session = SparkSession.builder.appName('Snapshot_Comparison').getOrCreate()

#Create two dataframe which contain the previous and current timestamp snapshots for comparison
timestamps = spark.table(TableName).select("snapshot_timestamp").distinct().orderBy(col("snapshot_timestamp").desc()).collect()

snapshot_current_ts = timestamps[0]
snapshot_previous_ts = timestamps[1]

data_previous = spark.table(TableName).filter(col("snapshot_timestamp") == snapshot_previous_ts['snapshot_timestamp']).select(*key_columns,*other_columns).distinct()
data_current = spark.table(TableName).filter(col("snapshot_timestamp") == snapshot_current_ts['snapshot_timestamp']).select(*key_columns,*other_columns,"snapshot_timestamp").distinct()


#Create hash columns based on key and non-key columns so that its easy to make a single join condition instead of multiple complex join conditions

data_current = data_current.withColumn("HashKey",sha2(concat_ws("|",*key_columns),256)).withColumn("HashOther",sha2(concat_ws("|",*other_columns),256))
data_previous = data_previous.withColumn("HashKey",sha2(concat_ws("|",*key_columns),256)).withColumn("HashOther",sha2(concat_ws("|",*other_columns),256))

joined_data = data_current.join(data_previous,on=[(data_current['HashKey'] == data_previous['HashKey']) & (data_current['HashOther'] != data_previous['HashOther']) ],how='inner')
joined_data.cache().count()


#Creating an empty DataFrame with predefined schema so that we can append the comparison data in the for loop
#We convert string representation of dictionary to actual dictionary using literal_eval
#Then we loop through the dictionary to create the schema

predefined_columns_schema = "{'Attribute':'StringType','PrevValue':'StringType','CurrValue':'StringType','ChangedOn':'TimestampType'}"

schema_dict = ast.literal_eval(schema_string)
predefined_columns_schema_dict = ast.literal_eval(predefined_columns_schema)

schema_dict.update(predefined_columns_schema_dict)

comparison_dataframe_schema = StructType()
get_class = lambda x: globals()[x]

for key,value in schema_dict.items():
  type_value = get_class(value)
  comparison_dataframe_schema.add(StructField(key,type_value(),False))
  
emptyRDD = spark_session.sparkContext.emptyRDD()
comparison_dataframe = spark_session.createDataFrame(data=emptyRDD,schema=comparison_dataframe_schema)

#Loop through the non-key columns and create rows for every column that we are comparing
#We check if the two columns are equal. We use Null safe operator to test for null and value conditions.

snapshot_comparison_new_columns = ["Attribute","PrevValue","CurrValue","ChangedOn"]

for looping_column in other_columns:
  temp_df = joined_data.filter(~(data_current[looping_column].eqNullSafe(data_previous[looping_column])))
  temp_df = temp_df.select(
                           lit(looping_column).alias("Attribute"), 
                           data_previous[looping_column].alias("PrevValue"),
                           data_current[looping_column].alias("CurrValue"),
                           data_current['snapshot_timestamp'].alias("ChangedOn"),
                           data_current["*"],
                           *snapshot_comparison_new_columns
                           ).select(*key_columns, *snapshot_comparison_new_columns)
                               
  comparison_dataframe = comparison_dataframe.union(temp_df)

comparison_dataframe = comparison_dataframe.select(lit(TableName).alias("TableName"),
                                                   "*", 
                                                   lit(snapshot_previous_ts['snapshot_timestamp']).alias("old_snapshot_timestamp"),
                                                   lit(snapshot_current_ts['snapshot_timestamp']).alias("new_snapshot_timestamp"))

#Write the data frame as a delta table. 
comparison_dataframe.filter("Attribute is not null").write.format('delta').option("mergeSchema", "true").mode("append").saveAsTable("order_comparison")
