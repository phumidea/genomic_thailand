from pyspark.sql import SparkSession
import os
from time import time
from pyspark.sql.functions import array_contains



## Create spark environment
os.environ['PYSPARK_SUBMIT_ARGS'] = "--conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions --name 'PySparkShell' --packages 'io.projectglow:glow-spark3_2.12:0.6.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' pyspark-shell"

## Create spark builder
spark = SparkSession.builder.appName("connector") \
                            .master("local[*]") \
                            .getOrCreate()

spark.sparkContext.setLogLevel("OFF")


## Load dataframe to cassandra
read_main_options = {"table": "main", "keyspace": "genome", "spark.cassandra.connection.host": "10.168.206.98:9042, 10.168.206.98:9043, 10.168.206.98:9044, 10.168.206.98:9045",}
read_info_options = {"table": "info", "keyspace": "genome", "spark.cassandra.connection.host": "10.168.206.98:9042, 10.168.206.98:9043, 10.168.206.98:9044, 10.168.206.98:9045",}
read_csq_options = {"table": "csq", "keyspace": "genome", "spark.cassandra.connection.host": "10.168.206.98:9042, 10.168.206.98:9043, 10.168.206.98:9044, 10.168.206.98:9045",}

df_main = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(**read_main_options)\
    .load()

df_info = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(**read_info_options)\
    .load()

df_csq = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(**read_csq_options)\
    .load()

print("Welcome to Query VCF")
while True:
    print("Type A to F for select query type")
    print("A: Select * where chrom = 'xxx'")
    print("B: Select * where chrom = 'xxx' and pos = 'xxx'")
    print("C: Select * where chrom = 'xx' and pos >= 'xx' and pos <= 'xx'")
    print("D: Select * where Existing_variation contains 'xxx'")
    print("E: Select * where gnomADe_AF < xx and gnomADg_AF < xx")
    print("F: Select * where symbol ='xxx' and consequence contains ‘xxx’")
    print("Please type 'quit' to exit")
    command = input("Type your command here => ")
    if command == "quit": break
    elif command == "A":
        chrom = input("chrom input => ")
        df_main_tmp = df_main.filter(df_main.chrom == chrom)
        df_info_tmp = df_info.filter(df_info.chrom == chrom)
        df_csq_tmp = df_csq.filter(df_csq.chrom == chrom)

    elif command == "B":
        chrom = input("chrom input => ")
        pos = int(input("pos input => "))
        df_main_tmp = df_main.filter((df_main.chrom == chrom) & (df_main.pos == pos))
        df_info_tmp = df_info.filter((df_info.chrom == chrom) & (df_info.pos == pos))
        df_csq_tmp = df_csq.filter((df_csq.chrom == chrom) & (df_csq.pos == pos))
    elif command == "C":
        chrom = input("chrom input => ")
        pos_start = int(input("pos_start input => "))
        pos_end = int(input("pos_start input => "))
        df_main_tmp = df_main.filter((df_main.chrom == chrom) & (df_main.pos >= pos_start) & (df_main.pos <= pos_end))
        df_info_tmp = df_info.filter((df_info.chrom == chrom) & (df_info.pos >= pos_start) & (df_info.pos <= pos_end))
        df_csq_tmp = df_csq.filter((df_csq.chrom == chrom) & (df_csq.pos >= pos_start) & (df_csq.pos <= pos_end))

    elif command == "D":
        existing_variation = input("Existing_variation input => ")
        df_main_tmp = df_main
        df_info_tmp = df_info
        df_csq_tmp = df_csq.where(array_contains("existing_variation", existing_variation))

    elif command == "E":
        gnomADe_AF = input("gonmADe_AF input => ")
        gnomADg_AF = input("gnomADg_AF input => ")
        df_main_tmp = df_main
        df_info_tmp = df_info
        df_csq_tmp = df_csq.filter((df_csq.gnomad_exomes_af < gnomADe_AF) & (df_csq.gnomad_genomes_af < gnomADg_AF))
        
    elif command == "F":
        symbol = input("symbol input => ")
        consequence = input("consequence = > ")
        df_main_tmp = df_main
        df_info_tmp = df_info
        df_csq_tmp = df_csq.filter(df_csq.symbol == symbol)
        df_csq_tmp = df_csq_tmp.where(array_contains("existing_variation", existing_variation))
        
    df_main_tmp = df_main_tmp.withColumnRenamed("chrom", "chrom_main")\
                .withColumnRenamed("pos", "pos_main")

    df_info_tmp = df_info_tmp.withColumnRenamed("chrom", "chrom_info")\
                .withColumnRenamed("pos", "pos_info")

    df_csq_tmp = df_csq_tmp.withColumnRenamed("chrom", "chrom_csq")\
                .withColumnRenamed("pos", "pos_csq")
    
    main_info_df = df_main_tmp.join(df_info_tmp, (df_main_tmp.chrom_main == df_info_tmp.chrom_info)&(df_main_tmp.pos_main == df_info_tmp.pos_info), "inner")
    main_info_df.drop(*["chrom_info", "pos_info"])
    main_info_df = main_info_df.withColumnRenamed("chrom_main", "chrom")\
            .withColumnRenamed("pos_main", "pos")
    
    df = df_csq_tmp.join(main_info_df, (df_csq_tmp.chrom_csq == main_info_df.chrom)&(df_csq_tmp.pos_csq == main_info_df.pos), "left")
    df = df.drop(*["chrom_csq", "info_csq"])
    df.show()
    
print("Thank you for use program")

