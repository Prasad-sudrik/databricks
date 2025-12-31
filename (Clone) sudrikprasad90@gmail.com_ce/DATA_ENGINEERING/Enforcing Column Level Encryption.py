# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark) \
    .tableName("dimEmployee") \
    .addColumn("empId","INT") \
    .addColumn("empName","STRING") \
    .addColumn("SSN","STRING") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dimEmployee values (100,"Mike",'2345671');
# MAGIC insert into dimEmployee values (200,"David",'5632178');
# MAGIC insert into dimEmployee values (300,"Peter",'1456782');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dimemployee;

# COMMAND ----------

pip install cryptography

# COMMAND ----------

from cryptography.fernet import Fernet

key = Fernet.generate_key()
f = Fernet(key)

# COMMAND ----------

PIIData = b"testmail@gmail.com"
TestData = f.encrypt(PIIData)
print(TestData)

# COMMAND ----------

# DBTITLE 1,Decrypt Sample Data
print(f.decrypt(TestData))

# COMMAND ----------

def encrypt_data(data,KEY):
    from cryptography.fernet import Fernet
    f = Fernet(KEY)
    dataB = bytes(data, 'utf-8')
    encrypted_data = f.encrypt(dataB)
    encrypted_data = str(encrypted_data.decode('ascii'))
    return encrypted_data

# COMMAND ----------

def decrypt_data(encrypted_data,KEY):
    from cryptography.fernet import Fernet
    f = Fernet(KEY)
    decrypted_data = f.decrypt(encrypted_data.encode()).decode()
    return decrypted_data;

# COMMAND ----------

from pyspark.sql.functions import udf,lit,md5
from pyspark.sql.types import StringType

encryption = udf(encrypt_data,StringType())
decryption = udf(decrypt_data,StringType())

# COMMAND ----------

df = spark.table("dimEmployee")
encryptedDF = df.withColumn("ssn_encrypted",encryption("SSN",lit(key)))
display(encryptedDF)

# COMMAND ----------

decryptedDF = encryptedDF.withColumn("ssn_decrypted",decryption("ssn_encrypted",lit(key)))
display(decryptedDF)

# COMMAND ----------

import time
time.sleep(30*60)

print("30 minutes have passsed!")

# COMMAND ----------


