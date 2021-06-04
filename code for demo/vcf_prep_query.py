import os
import glow
from pyspark.sql import SparkSession
from time import time
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, BooleanType, MapType
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import flatten, explode
from pyspark.sql.functions import col
from time import time
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
import sys
from cassandra.cluster import Cluster


#### Initial Stage ####

## Config spark ##
os.environ['PYSPARK_SUBMIT_ARGS'] = "--conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec \
                                     --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
                                     --name 'PySparkShell' \
                                     --packages 'io.projectglow:glow-spark3_2.12:0.6.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' pyspark-shell"

## Craete Spark ##
spark = SparkSession.builder.appName("Senior_project") \
                            .master("local[*]") \
                            .getOrCreate()

## Turn off Spark warning
spark.sparkContext.setLogLevel("OFF")

## Read file name
filename = sys.argv[1]

## Read vcf file as input ##
df = spark.read.format('vcf') \
    .option("includeSampleIds", True) \
    .option("flattenInfo", True) \
    .load(filename)

## start in glow start from 0 so we add 1
df = df.withColumn("start",col("start") + lit(1))

#### Main table ####
columns_main = {"contigName": "chrom", "start": "pos", "names":"id", \
                "referenceAllele": "ref", "alternateAlleles": "alt", \
                    "qual": "qual","filters": "filter"}

## Select main columns ##
glow_main_names = [*columns_main]
original_main_names = list(columns_main.values())
main_df = df.select(glow_main_names)
main_df = main_df.toDF(*original_main_names)

## List all genotype name ##
genotype_full_list = df.select("genotypes.sampleId").collect()[0][0]

## Rename some genotype that cassandra can't accept format ##
for i in range(len(genotype_full_list)):
    genotype_full_list[i] = genotype_full_list[i].replace("-","_").lower()

start = 0
end = 5
while start < len(genotype_full_list):
    # print(start)
    genotype_list = genotype_full_list[start:end]

    cluster = Cluster([('10.168.206.98','9042'),('10.168.206.98','9043'),('10.168.206.98','9044'),('10.168.206.98','9045')])
    session = cluster.connect()
    session.set_keyspace('genome')

    ## Create schema for support genotype data
    for genotype in genotype_list:
        try:
            rows = session.execute(f"alter table main_query add (s{genotype}_int map<text,int>,s{genotype}_text map<text,text>,s{genotype}_list_int map<text,frozen<list<int>>>);")
        except:
            pass

    cluster.shutdown()

    schema_data = []
    for genotype in genotype_list:
        genotype_struct = [StructField(f"s{genotype}_int", MapType(StringType(), IntegerType()), True), \
                            StructField(f"s{genotype}_text", MapType(StringType(), StringType()), True), \
                            StructField(f"s{genotype}_list_int", MapType(StringType(),ArrayType(IntegerType())), True)]
        schema_data += genotype_struct

    ## Get genotype data as each attribute
    allele_depth_data = df.select("genotypes.alleleDepths").collect()
    pid_data = df.select("genotypes.PID").collect()
    calls_data = df.select("genotypes.calls").collect()
    min_dp_data = df.select("genotypes.MIN_DP").collect()
    pgt_data = df.select("genotypes.PGT").collect()
    plh_data = df.select("genotypes.phredLikelihoods").collect()
    depth_data = df.select("genotypes.depth").collect()

    n_row = df.count()
    n_geno = len(genotype_list)
    output = []
    ## Create dict for each genotype
    for n_row in range(n_row):
        result = []
        for geno_index in range(n_geno): ## each genotypes
        
            #### Int ####
            min_dp = min_dp_data[n_row][0][geno_index]
            depth = depth_data[n_row][0][geno_index]
        
            #### Text ####
            pid = pid_data[n_row][0][geno_index]
            pgt = pgt_data[n_row][0][geno_index]

            #### Array ####
            allele_depth = allele_depth_data[n_row][0][geno_index]
            calls = calls_data[n_row][0][geno_index]
            plh = plh_data[n_row][0][geno_index]

            int_value = dict()
            text_value = dict()
            list_int_value = dict()

            if min_dp: int_value["min_dp"] = min_dp 
            if depth: int_value["depth"] = depth

            if pid: text_value["pid"] = pid
            if pgt: text_value["pgt"] = pgt
            
            if allele_depth: list_int_value["allele_depth"] = allele_depth
            if calls: list_int_value["calls"] = calls
            if plh: list_int_value["plh"] = plh

            result += [int_value, text_value,list_int_value]

        result = tuple(result)
        output.append(result)
    
    format_df = spark.createDataFrame(data=output, schema = StructType(schema_data))

    ## Join main_df and format_df
    w = Window().orderBy(lit('A'))
    format_df = format_df.withColumn("row_num_format", row_number().over(w))
    w = Window().orderBy(lit('A'))
    main_df = main_df.withColumn("row_num_main", row_number().over(w))
    main_table_df = main_df.join(format_df, main_df.row_num_main == format_df.row_num_format, "inner")
    columns_to_drop = ["row_num_main", "row_num_format"]
    main_table_df = main_table_df.drop(*columns_to_drop)

    ## Write main dataframe to specific keyspace and table
    write_options = {"table": "main_query", "keyspace": "genome",
                     "spark.cassandra.connection.host":"10.168.206.98:9042, 10.168.206.98:9043,10.168.206.98:9044, 10.168.206.98:9045"}

    main_table_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode("append")\
        .options(**write_options)\
        .save()
    break
    start += 10
    end += 10

#### Info table ####
columns_info = {"contigName": "chrom", "start": "pos", "INFO_AC": "ac",
    "INFO_AN":"an", "INFO_BaseQRankSum":"baseqranksum", "INFO_DB":"db",
    "INFO_RAW_MQ":"raw_mq"}

glow_info_names = [*columns_info]
original_info_names = list(columns_info.values())
info_df = df.select(glow_info_names)
info_df = info_df.toDF(*original_info_names)

## Wirte to Cassandra ##
write_options = {"table": "info_query", 
                 "keyspace": "genome",
                 "spark.cassandra.connection.host":
                     "10.168.206.98:9042, 10.168.206.98:9043, \
                      10.168.206.98:9044, 10.168.206.98:9045"}
info_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(**write_options)\
    .save()

#### CSQ table ####

extract_csq_df = df.withColumn("INFO_CSQ", explode("INFO_CSQ")).select(
    "*", col("INFO_CSQ").alias("INFO_CSQ_tmp"))

columns_csq = {"contigName": "chrom", "start": "pos", "INFO_CSQ_tmp.Allele": "allele", "INFO_CSQ_tmp.Consequence": "consequence",
                "INFO_CSQ_tmp.Existing_variation":"existing_variation", "INFO_CSQ_tmp.gnomAD_exomes_AF":"gnomad_exomes_af",
                "INFO_CSQ_tmp.gnomAD_genomes_AF":"gnomad_genomes_af", "INFO_CSQ_tmp.SYMBOL":"symbol"
            }

glow_csq_names = [*columns_csq]
original_csq_names = list(columns_csq.values())
csq_df = extract_csq_df.select(glow_csq_names)
csq_df=csq_df.toDF(*original_csq_names)

write_options = {"table": "csq_query", 
                 "keyspace": "genome", 
                 "spark.cassandra.connection.host": 
                        "10.168.206.98:9042, 10.168.206.98:9043, \
                           10.168.206.98:9044, 10.168.206.98:9045"}
csq_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(**write_options)\
    .save()
