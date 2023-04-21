import findspark

findspark.init()
import pyspark  # only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd

# creating spark session
spark = SparkSession.builder.master("local[*]").appName("Master_DataSet").getOrCreate()

Access_key_ID = '***********'
Secret_access_key = '************'
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                                  "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

# ______________________________________________________________________________________________
# bajaj car_color
bajaj_car_color_data = "s3a://rds-dms-s3/bajaj/public/car_color/LOAD00000001.csv"
bajaj_car_color_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    bajaj_car_color_data)

bajaj_car_color_df = bajaj_car_color_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "color")
# bajaj_car_color_df.show()

# bajaj car_drive_mode
bajaj_car_drive_mode_data = "s3a://rds-dms-s3/bajaj/public/car_drive_mode/LOAD00000001.csv"
bajaj_car_drive_mode_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    bajaj_car_drive_mode_data)

bajaj_car_drive_mode_df = bajaj_car_drive_mode_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "model_varient")
# bajaj_car_drive_mode_df.show()

# bajaj_car_model
bajaj_car_model_data = "s3a://rds-dms-s3/bajaj/public/car_model/LOAD00000001.csv"
bajaj_car_model_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    bajaj_car_model_data)

bajaj_car_model_df = bajaj_car_model_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "company").withColumnRenamed("_c2", "car_model")
# bajaj_car_model_df.show()

# bajaj_car_price
bajaj_car_price_data = "s3a://rds-dms-s3/bajaj/public/car_price/LOAD00000001.csv"
bajaj_car_price_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    bajaj_car_price_data)

bajaj_car_price_df = bajaj_car_price_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "car_price").withColumnRenamed("_c3", "fuel")
# bajaj_car_price_df.show()


# bajaj_company
bajaj_company_data = "s3a://rds-dms-s3/bajaj/public/company_country/LOAD00000001.csv"
bajaj_company_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    bajaj_company_data)
bajaj_company_df = bajaj_company_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "company").withColumnRenamed("_c2", "country")

# bajaj_company_df.show()
bajaj_joined_df = bajaj_car_color_df.join(bajaj_car_drive_mode_df, on='id', how='inner') \
    .join(bajaj_car_model_df, on='id', how='inner') \
    .join(bajaj_car_price_df, on='id', how='inner').drop(bajaj_car_color_df.car_model) \
    .drop(bajaj_car_model_df.car_model) \
    .drop(bajaj_car_price_df.car_model)

# kia company ------------------------------------------------------------------------------------------------

# kia car_color
kia_car_color_data = "s3a://rds-dms-s3/kia/public/car_color/car_color.csv"
kia_car_color_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    kia_car_color_data)
kia_car_color_df = kia_car_color_df.withColumnRenamed("company", "car_model")
# kia_car_color_df.show()

# kia car_drive_mode
kia_car_drive_mode_data = "s3a://rds-dms-s3/kia/public/car_drive_mode/car_drive_mode.csv"
kia_car_drive_mode_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    kia_car_drive_mode_data)

# kia_car_drive_mode_df.show()

# kia_car_model
kia_car_model_data = "s3a://rds-dms-s3/kia/public/car_model/car_model.csv"
kia_car_model_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    kia_car_model_data)

# kia_car_model_df.show()

# kia_car_price
kia_car_price_data = "s3a://rds-dms-s3/kia/public/car_price/car_price.csv"
kia_car_price_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    kia_car_price_data)
kia_car_price_df = kia_car_price_df.withColumnRenamed("price", "car_price")

# kia_car_price_df.show()


# kia_company
kia_company_data = "s3a://rds-dms-s3/kia/public/company_country/company_country.csv"
kia_company_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(kia_company_data)

# kia_company_df.show()


kia_joined_df = kia_car_color_df.join(kia_car_drive_mode_df, on='id', how='inner') \
    .join(kia_car_model_df, on='id', how='inner') \
    .join(kia_car_price_df, on='id', how='inner') \
    .drop(kia_car_color_df.car_model) \
    .drop(kia_car_model_df.car_model) \
    .drop(kia_car_price_df.car_model)

# tata company---------------------------------------------------------------------------------------


# tata car_color
tata_car_color_data = "s3a://rds-dms-s3/tata/public/car_color/LOAD00000001.csv"
tata_car_color_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    tata_car_color_data)

tata_car_color_df = tata_car_color_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "color")
# tata_car_color_df.show()

# tata car_drive_mode
tata_car_drive_mode_data = "s3a://rds-dms-s3/tata/public/car_drive_mode/LOAD00000001.csv"
tata_car_drive_mode_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    tata_car_drive_mode_data)

tata_car_drive_mode_df = tata_car_drive_mode_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "model_varient")
# tata_car_drive_mode_df.show()

# tata_car_model
tata_car_model_data = "s3a://rds-dms-s3/tata/public/car_model/LOAD00000001.csv"
tata_car_model_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    tata_car_model_data)

tata_car_model_df = tata_car_model_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "company").withColumnRenamed("_c2", "car_model")
# tata_car_model_df.show()

# tata_car_price
tata_car_price_data = "s3a://rds-dms-s3/tata/public/car_price/LOAD00000001.csv"
tata_car_price_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    tata_car_price_data)

tata_car_price_df = tata_car_price_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "car_price").withColumnRenamed("_c3", "fuel")
# tata_car_price_df.show()


# tata_company
tata_company_data = "s3a://rds-dms-s3/tata/public/company_country/LOAD00000001.csv"
tata_company_df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load(
    tata_company_data)

tata_company_df = tata_company_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "company").withColumnRenamed("_c2", "country")
# tata_company_df.show()


tata_joined_df = tata_car_color_df.join(tata_car_drive_mode_df, on='id', how='inner') \
    .join(tata_car_model_df, on='id', how='inner') \
    .join(tata_car_price_df, on='id', how='inner').drop(tata_car_color_df.car_model) \
    .drop(tata_car_model_df.car_model) \
    .drop(tata_car_price_df.car_model)

# maruti company ----------------------------------------------------------------------------

# maruti car_color
maruti_car_color_data = "s3a://rds-dms-s3/maruti/public/maruti/car_color/car_color.csv"
maruti_car_color_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    maruti_car_color_data)

maruti_car_color_df = maruti_car_color_df.withColumnRenamed("_c0", "id") \
    .withColumnRenamed("_c1", "car_model").withColumnRenamed("_c2", "color")
# maruti_car_color_df.show()

# maruti car_drive_mode
maruti_car_drive_mode_data = "s3a://rds-dms-s3/maruti/public/maruti/car_drive_mode/car_drive_mode.csv"
maruti_car_drive_mode_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    maruti_car_drive_mode_data)

maruti_car_drive_mode_df = maruti_car_drive_mode_df \
    .withColumnRenamed("drive_mode", "model_varient")
# maruti_car_drive_mode_df.show()

# maruti_car_model
maruti_car_model_data = "s3a://rds-dms-s3/maruti/public/maruti/car_model/car_model.csv"
maruti_car_model_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    maruti_car_model_data)

'''maruti_car_model_df = tata_car_model_df.withColumnRenamed("_c0","id") \
    .withColumnRenamed("_c1","company").withColumnRenamed("_c2","car_model")'''
# maruti_car_model_df.show()

# maruti_car_price
maruti_car_price_data = "s3a://rds-dms-s3/maruti/public/maruti/car_price/car_price.csv"
maruti_car_price_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    maruti_car_price_data)

maruti_car_price_df = maruti_car_price_df.withColumnRenamed("price", "car_price")
# maruti_car_price_df.show()


# maruti_company
maruti_company_data = "s3a://rds-dms-s3/maruti/public/maruti/company_country/company_country.csv"
maruti_company_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    maruti_company_data)

'''maruti_company_df = maruti_company_df.withColumnRenamed("_c0","id") \
    .withColumnRenamed("_c1","company").withColumnRenamed("_c2","country")'''
# maruti_company_df.show()


maruti_joined_df = maruti_car_color_df.join(maruti_car_drive_mode_df, on='id', how='inner') \
    .join(maruti_car_model_df, on='id', how='inner') \
    .join(maruti_car_price_df, on='id', how='inner').drop(maruti_car_color_df.car_model) \
    .drop(maruti_car_model_df.car_model) \
    .drop(maruti_car_price_df.car_model)

bajaj = bajaj_joined_df.toPandas()
kia = kia_joined_df.toPandas()
tata = tata_joined_df.toPandas()
maruti = maruti_joined_df.toPandas()

df_cd = pd.concat([bajaj, kia, tata, maruti], ignore_index=True)
df_cd = df_cd.drop("id", axis='columns')


# saving the dataframe
#df_csv = df_cd.to_csv('master-dataset.csv')

import pandas as pd
import boto3
from io import StringIO



def upload_s3(df,i):
    s3 = boto3.client("s3",aws_access_key_id=Access_key_ID,aws_secret_access_key=Secret_access_key)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket="rds-dms-s3", Body=csv_buf.getvalue(), Key='master database/'+i)


#upload_s3(df_cd,"master-dataset.csv")





