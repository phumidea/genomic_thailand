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
end = 10
while start < len(genotype_full_list):
    # print(start)
    genotype_list = genotype_full_list[start:end]

    cluster = Cluster([('10.168.206.98','9042'),('10.168.206.98','9043'),('10.168.206.98','9044'),('10.168.206.98','9045')])
    session = cluster.connect()
    session.set_keyspace('genome')

    ## Create schema for support genotype data
    for genotype in genotype_list:
        try:
            rows = session.execute(f"alter table main add (s{genotype}_int map<text,int>,s{genotype}_text map<text,text>,s{genotype}_list_int map<text,frozen<list<int>>>);")
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
    write_options = {"table": "main", 
                     "keyspace": "genome",
                     "spark.cassandra.connection.host":"10.168.206.98:9042, 10.168.206.98:9043,\
                                                        10.168.206.98:9044, 10.168.206.98:9045"}

    main_table_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode("append")\
        .options(**write_options)\
        .save()

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
write_options = {"table": "info", 
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

columns_csq = {"contigName": "chrom", "start": "pos", "INFO_CSQ_tmp.Allele": "allele", "INFO_CSQ_tmp.Consequence": "consequence",\
            "INFO_CSQ_tmp.IMPACT":"impact", "INFO_CSQ_tmp.SYMBOL":"symbol", "INFO_CSQ_tmp.Gene":"gene","INFO_CSQ_tmp.Feature":"feature",\
            "INFO_CSQ_tmp.BIOTYPE":"biotype","INFO_CSQ_tmp.EXON":"exon","INFO_CSQ_tmp.INTRON":"intron","INFO_CSQ_tmp.HGVSc":"hgvsc",\
            "INFO_CSQ_tmp.HGVSp":"hgvsp","INFO_CSQ_tmp.cDNA_position":"cdna_position","INFO_CSQ_tmp.CDS_position":"cds_position",\
            "INFO_CSQ_tmp.Protein_position":"protein_position", "INFO_CSQ_tmp.Amino_acids":"amino_acids","INFO_CSQ_tmp.Codons":"codons",\
            "INFO_CSQ_tmp.Existing_variation":"existing_variation","INFO_CSQ_tmp.DISTANCE":"distance","INFO_CSQ_tmp.STRAND":"strand",\
            "INFO_CSQ_tmp.VARIANT_CLASS":"variant_class","INFO_CSQ_tmp.SYMBOL_SOURCE":"symbol_source","INFO_CSQ_tmp.HGNC_ID":"hgnc_id",\
            "INFO_CSQ_tmp.CANONICAL":"canonical","INFO_CSQ_tmp.ENSP":"ensp","INFO_CSQ_tmp.UNIPROT_ISOFORM":"uniprot_isoform",\
            "INFO_CSQ_tmp.GENE_PHENO":"gene_pheno","INFO_CSQ_tmp.HGVSg":"hgvsg","INFO_CSQ_tmp.CLIN_SIG":"clin_sig",\
            "INFO_CSQ_tmp.SOMATIC":"somatic", "INFO_CSQ_tmp.PHENO":"pheno","INFO_CSQ_tmp.PUBMED":"pubmed","INFO_CSQ_tmp.1000Gp3_AC":"csq_1000gp3_ac",\
            "INFO_CSQ_tmp.1000Gp3_AF":"csq_1000gp3_af","INFO_CSQ_tmp.1000Gp3_AFR_AC":"csq_1000gp3_afr_ac",\
            "INFO_CSQ_tmp.1000Gp3_AFR_AF":"csq_1000gp3_afr_af","INFO_CSQ_tmp.1000Gp3_AMR_AC":"csq_1000gp3_amr_ac","INFO_CSQ_tmp.1000Gp3_AMR_AF":"csq_1000gp3_amr_af",\
            "INFO_CSQ_tmp.1000Gp3_EAS_AC":"csq_1000gp3_eas_ac","INFO_CSQ_tmp.1000Gp3_EAS_AF":"csq_1000gp3_eas_af","INFO_CSQ_tmp.1000Gp3_EUR_AC":"csq_1000gp3_eur_ac",\
            "INFO_CSQ_tmp.1000Gp3_EUR_AF":"csq_1000gp3_eur_af","INFO_CSQ_tmp.1000Gp3_SAS_AC":"csq_1000gp3_sas_ac","INFO_CSQ_tmp.1000Gp3_SAS_AF":"csq_1000gp3_sas_af", \
            "INFO_CSQ_tmp.ALSPAC_AC":"alspac_ac","INFO_CSQ_tmp.ALSPAC_AF":"alspac_af","INFO_CSQ_tmp.APPRIS":"appris","INFO_CSQ_tmp.Aloft_Confidence":"aloft_confidence",\
            "INFO_CSQ_tmp.Aloft_Fraction_transcripts_affected":"aloft_fraction_transcripts_affected","INFO_CSQ_tmp.Aloft_pred":"aloft_pred",\
            "INFO_CSQ_tmp.Aloft_prob_Dominant":"aloft_prob_dominant","INFO_CSQ_tmp.Aloft_prob_Recessive":"aloft_prob_recessive",\
            "INFO_CSQ_tmp.Aloft_prob_Tolerant":"aloft_prob_tolerant","INFO_CSQ_tmp.AltaiNeandertal":"altaineandertal","INFO_CSQ_tmp.Ancestral_allele":"ancestral_allele",\
            "INFO_CSQ_tmp.BayesDel_addAF_pred":"bayesdel_addaf_pred","INFO_CSQ_tmp.BayesDel_addAF_rankscore":"bayesdel_addaf_rankscore",\
            "INFO_CSQ_tmp.BayesDel_addAF_score":"bayesdel_addaf_score","INFO_CSQ_tmp.BayesDel_noAF_pred":"bayesdel_noaf_pred", \
            "INFO_CSQ_tmp.BayesDel_noAF_rankscore":"bayesdel_noaf_rankscore","INFO_CSQ_tmp.BayesDel_noAF_score":"bayesdel_noaf_score",\
            "INFO_CSQ_tmp.CADD_phred":"cadd_phred","INFO_CSQ_tmp.CADD_phred_hg19":"cadd_phred_hg19","INFO_CSQ_tmp.CADD_raw":"cadd_raw",\
            "INFO_CSQ_tmp.CADD_raw_hg19":"cadd_raw_hg19","INFO_CSQ_tmp.CADD_raw_rankscore":"cadd_raw_rankscore",\
            "INFO_CSQ_tmp.CADD_raw_rankscore_hg19":"cadd_raw_rankscore_hg19","INFO_CSQ_tmp.ClinPred_pred":"clinpred_pred",\
            "INFO_CSQ_tmp.ClinPred_rankscore":"clinpred_rankscore","INFO_CSQ_tmp.ClinPred_score":"clinpred_score",\
            "INFO_CSQ_tmp.DANN_rankscore":"dann_rankscore","INFO_CSQ_tmp.DANN_score":"dann_score","INFO_CSQ_tmp.DEOGEN2_pred":"deogen2_pred",\
            "INFO_CSQ_tmp.DEOGEN2_rankscore":"deogen2_rankscore","INFO_CSQ_tmp.DEOGEN2_score":"deogen2_score",\
            "INFO_CSQ_tmp.Denisova":"denisova","INFO_CSQ_tmp.ESP6500_AA_AC":"esp6500_aa_ac",\
            "INFO_CSQ_tmp.ESP6500_AA_AF":"esp6500_aa_af","INFO_CSQ_tmp.ESP6500_EA_AC":"esp6500_ea_ac","INFO_CSQ_tmp.ESP6500_EA_AF":"esp6500_ea_af",\
            "INFO_CSQ_tmp.Eigen-PC-phred_coding":"eigen_pc_phred_coding","INFO_CSQ_tmp.Eigen-PC-raw_coding":"eigen_pc_raw_coding",\
            "INFO_CSQ_tmp.Eigen-PC-raw_coding_rankscore":"eigen_pc_raw_coding_rankscore","INFO_CSQ_tmp.Eigen-phred_coding":"eigen_phred_coding",\
            "INFO_CSQ_tmp.Eigen-raw_coding":"eigen_raw_coding","INFO_CSQ_tmp.Eigen-raw_coding_rankscore":"eigen_raw_coding_rankscore",\
            "INFO_CSQ_tmp.Ensembl_geneid":"ensembl_geneid","INFO_CSQ_tmp.Ensembl_proteinid":"ensembl_proteinid","INFO_CSQ_tmp.Ensembl_transcriptid":"ensembl_transcriptid",\
            "INFO_CSQ_tmp.ExAC_AC":"exac_ac","INFO_CSQ_tmp.ExAC_AF":"exac_af","INFO_CSQ_tmp.ExAC_AFR_AC":"exac_afr_ac","INFO_CSQ_tmp.ExAC_AFR_AF":"exac_afr_af",\
            "INFO_CSQ_tmp.ExAC_AMR_AC":"exac_amr_ac","INFO_CSQ_tmp.ExAC_AMR_AF":"exac_amr_af","INFO_CSQ_tmp.ExAC_Adj_AC":"exac_adj_ac","INFO_CSQ_tmp.ExAC_Adj_AF":"exac_adj_af",\
            "INFO_CSQ_tmp.ExAC_EAS_AC":"exac_eas_ac","INFO_CSQ_tmp.ExAC_EAS_AF":"exac_eas_af", "INFO_CSQ_tmp.ExAC_FIN_AC":"exac_fin_ac","INFO_CSQ_tmp.ExAC_FIN_AF":"exac_fin_af",\
            "INFO_CSQ_tmp.ExAC_NFE_AC":"exac_nfe_ac","INFO_CSQ_tmp.ExAC_NFE_AF":"exac_nfe_af", "INFO_CSQ_tmp.ExAC_SAS_AC":"exac_sas_ac","INFO_CSQ_tmp.ExAC_SAS_AF":"exac_sas_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_AC":"exac_nontcga_ac","INFO_CSQ_tmp.ExAC_nonTCGA_AF":"exac_nontcga_af", "INFO_CSQ_tmp.ExAC_nonTCGA_AFR_AC":"exac_nontcga_afr_ac",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_AFR_AF":"exac_nontcga_afr_af", "INFO_CSQ_tmp.ExAC_nonTCGA_AMR_AC":"exac_nontcga_amr_ac","INFO_CSQ_tmp.ExAC_nonTCGA_AMR_AF":"exac_nontcga_amr_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_Adj_AC":"exac_nontcga_adj_ac","INFO_CSQ_tmp.ExAC_nonTCGA_Adj_AF":"exac_nontcga_adj_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_EAS_AC":"exac_nontcga_eas_ac","INFO_CSQ_tmp.ExAC_nonTCGA_EAS_AF":"exac_nontcga_eas_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_FIN_AC":"exac_nontcga_fin_ac","INFO_CSQ_tmp.ExAC_nonTCGA_FIN_AF":"exac_nontcga_fin_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_NFE_AC":"exac_nontcga_nfe_ac","INFO_CSQ_tmp.ExAC_nonTCGA_NFE_AF":"exac_nontcga_nfe_af",\
            "INFO_CSQ_tmp.ExAC_nonTCGA_SAS_AC":"exac_nontcga_sas_ac","INFO_CSQ_tmp.ExAC_nonTCGA_SAS_AF":"exac_nontcga_sas_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_AC":"exac_nonpsych_ac","INFO_CSQ_tmp.ExAC_nonpsych_AF":"exac_nonpsych_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_AFR_AC":"exac_nonpsych_afr_ac","INFO_CSQ_tmp.ExAC_nonpsych_AFR_AF":"exac_nonpsych_afr_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_AMR_AC":"exac_nonpsych_amr_ac","INFO_CSQ_tmp.ExAC_nonpsych_AMR_AF":"exac_nonpsych_amr_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_Adj_AC":"exac_nonpsych_adj_ac","INFO_CSQ_tmp.ExAC_nonpsych_Adj_AF":"exac_nonpsych_adj_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_EAS_AC":"exac_nonpsych_eas_ac","INFO_CSQ_tmp.ExAC_nonpsych_EAS_AF":"exac_nonpsych_eas_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_FIN_AC":"exac_nonpsych_fin_ac","INFO_CSQ_tmp.ExAC_nonpsych_FIN_AF":"exac_nonpsych_fin_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_NFE_AC":"exac_nonpsych_nfe_ac","INFO_CSQ_tmp.ExAC_nonpsych_NFE_AF":"exac_nonpsych_nfe_af",\
            "INFO_CSQ_tmp.ExAC_nonpsych_SAS_AC":"exac_nonpsych_sas_ac","INFO_CSQ_tmp.ExAC_nonpsych_SAS_AF":"exac_nonpsych_sas_af",\
            "INFO_CSQ_tmp.FATHMM_converted_rankscore":"fathmm_converted_rankscore","INFO_CSQ_tmp.FATHMM_pred":"fathmm_pred", \
            "INFO_CSQ_tmp.FATHMM_score":"fathmm_score","INFO_CSQ_tmp.GENCODE_basic":"gencode_basic",\
            "INFO_CSQ_tmp.GERP++_NR":"gerpplusplus_nr","INFO_CSQ_tmp.GERP++_RS":"gerpplusplus_rs","INFO_CSQ_tmp.GERP++_RS_rankscore":"gerpplusplus_rs_rankscore",\
            "INFO_CSQ_tmp.GM12878_confidence_value":"gm12878_confidence_value","INFO_CSQ_tmp.GM12878_fitCons_rankscore":"gm12878_fitcons_rankscore",\
            "INFO_CSQ_tmp.GM12878_fitCons_score":"gm12878_fitcons_score","INFO_CSQ_tmp.GTEx_V8_gene":"gtex_v8_gene","INFO_CSQ_tmp.GTEx_V8_tissue":"gtex_v8_tissue",\
            "INFO_CSQ_tmp.GenoCanyon_rankscore":"genocanyon_rankscore","INFO_CSQ_tmp.GenoCanyon_score":"genocanyon_score",\
            "INFO_CSQ_tmp.Geuvadis_eQTL_target_gene":"geuvadis_eqtl_target_gene","INFO_CSQ_tmp.H1-hESC_confidence_value":"h1_hesc_confidence_value",\
            "INFO_CSQ_tmp.H1-hESC_fitCons_rankscore":"h1_hesc_fitcons_rankscore","INFO_CSQ_tmp.H1-hESC_fitCons_score":"h1_hesc_fitcons_score",\
            "INFO_CSQ_tmp.HGVSc_ANNOVAR":"hgvsc_annovar","INFO_CSQ_tmp.HGVSc_VEP":"hgvsc_vep","INFO_CSQ_tmp.HGVSc_snpEff":"hgvsc_snpeff",\
            "INFO_CSQ_tmp.HGVSp_ANNOVAR":"hgvsp_annovar","INFO_CSQ_tmp.HGVSp_VEP":"hgvsp_vep","INFO_CSQ_tmp.HGVSp_snpEff":"hgvsp_snpeff",\
            "INFO_CSQ_tmp.HUVEC_confidence_value":"huvec_confidence_value","INFO_CSQ_tmp.HUVEC_fitCons_rankscore":"huvec_fitcons_rankscore",\
            "INFO_CSQ_tmp.HUVEC_fitCons_score":"huvec_fitcons_score","INFO_CSQ_tmp.Interpro_domain":"interpro_domain", "INFO_CSQ_tmp.LINSIGHT":"linsight",\
            "INFO_CSQ_tmp.LINSIGHT_rankscore":"linsight_rankscore","INFO_CSQ_tmp.LIST-S2_pred":"list_s2_pred", "INFO_CSQ_tmp.LIST-S2_rankscore":"list_s2_rankscore",\
            "INFO_CSQ_tmp.LIST-S2_score":"list_s2_score","INFO_CSQ_tmp.LRT_Omega":"lrt_omega","INFO_CSQ_tmp.LRT_converted_rankscore":"lrt_converted_rankscore",\
            "INFO_CSQ_tmp.LRT_pred":"lrt_pred","INFO_CSQ_tmp.LRT_score":"lrt_score", "INFO_CSQ_tmp.M-CAP_pred":"m_cap_pred", "INFO_CSQ_tmp.M-CAP_rankscore":"m_cap_rankscore",\
            "INFO_CSQ_tmp.M-CAP_score":"m_cap_score", "INFO_CSQ_tmp.MPC_rankscore":"mpc_rankscore", "INFO_CSQ_tmp.MPC_score":"mpc_score",\
            "INFO_CSQ_tmp.MVP_rankscore":"mvp_rankscore", "INFO_CSQ_tmp.MVP_score":"mvp_score","INFO_CSQ_tmp.MetaLR_pred":"metalr_pred",\
            "INFO_CSQ_tmp.MetaLR_rankscore":"metalr_rankscore","INFO_CSQ_tmp.MetaLR_score":"metalr_score",\
            "INFO_CSQ_tmp.MetaSVM_pred":"metasvm_pred","INFO_CSQ_tmp.MetaSVM_rankscore":"metasvm_rankscore","INFO_CSQ_tmp.MetaSVM_score":"metasvm_score",\
            "INFO_CSQ_tmp.MutPred_AAchange":"mutpred_aachange", "INFO_CSQ_tmp.MutPred_Top5features":"mutpred_top5features", "INFO_CSQ_tmp.MutPred_protID":"mutpred_protid",\
            "INFO_CSQ_tmp.MutPred_rankscore":"mutpred_rankscore","INFO_CSQ_tmp.MutPred_score":"mutpred_score",\
            "INFO_CSQ_tmp.MutationAssessor_pred":"mutationassessor_pred","INFO_CSQ_tmp.MutationAssessor_rankscore":"mutationassessor_rankscore",\
            "INFO_CSQ_tmp.MutationAssessor_score":"mutationassessor_score", "INFO_CSQ_tmp.MutationTaster_AAE":"mutationtaster_aae",\
            "INFO_CSQ_tmp.MutationTaster_converted_rankscore":"mutationtaster_converted_rankscore", "INFO_CSQ_tmp.MutationTaster_model":"mutationtaster_model",\
            "INFO_CSQ_tmp.MutationTaster_pred":"mutationtaster_pred", "INFO_CSQ_tmp.MutationTaster_score":"mutationtaster_score",\
            "INFO_CSQ_tmp.PROVEAN_converted_rankscore":"provean_converted_rankscore", "INFO_CSQ_tmp.PROVEAN_pred":"provean_pred", "INFO_CSQ_tmp.PROVEAN_score":"provean_score",\
            "INFO_CSQ_tmp.Polyphen2_HDIV_score":"polyphen2_hdiv_score", "INFO_CSQ_tmp.Polyphen2_HDIV_pred":"polyphen2_hdiv_pred", \
            "INFO_CSQ_tmp.Polyphen2_HDIV_rankscore":"polyphen2_hdiv_rankscore", "INFO_CSQ_tmp.Polyphen2_HVAR_score":"polyphen2_hvar_score",\
            "INFO_CSQ_tmp.Polyphen2_HVAR_pred":"polyphen2_hvar_pred", "INFO_CSQ_tmp.Polyphen2_HVAR_rankscore":"polyphen2_hvar_rankscore",\
            "INFO_CSQ_tmp.PrimateAI_pred":"primateai_pred", "INFO_CSQ_tmp.PrimateAI_rankscore":"primateai_rankscore", "INFO_CSQ_tmp.PrimateAI_score":"primateai_score", \
            "INFO_CSQ_tmp.REVEL_rankscore":"revel_rankscore", "INFO_CSQ_tmp.REVEL_score":"revel_score", "INFO_CSQ_tmp.Reliability_index":"reliability_index",\
            "INFO_CSQ_tmp.SIFT4G_converted_rankscore":"sift4g_converted_rankscore", "INFO_CSQ_tmp.SIFT4G_pred":"sift4g_pred", "INFO_CSQ_tmp.SIFT4G_score":"sift4g_score",\
            "INFO_CSQ_tmp.SIFT_converted_rankscore":"sift_converted_rankscore", "INFO_CSQ_tmp.SIFT_pred":"sift_pred", "INFO_CSQ_tmp.SIFT_score":"sift_score",\
            "INFO_CSQ_tmp.SiPhy_29way_logOdds":"siphy_29way_logodds", "INFO_CSQ_tmp.SiPhy_29way_logOdds_rankscore":"siphy_29way_logodds_rankscore",\
            "INFO_CSQ_tmp.SiPhy_29way_pi":"siphy_29way_pi", "INFO_CSQ_tmp.TWINSUK_AC":"twinsuk_ac", "INFO_CSQ_tmp.TWINSUK_AF":"twinsuk_af", "INFO_CSQ_tmp.UK10K_AC":"uk10k_ac",\
            "INFO_CSQ_tmp.UK10K_AF":"uk10k_af","INFO_CSQ_tmp.Uniprot_acc":"uniprot_acc", "INFO_CSQ_tmp.Uniprot_entry":"uniprot_entry",\
            "INFO_CSQ_tmp.VEP_canonical":"vep_canonical", "INFO_CSQ_tmp.VEST4_rankscore":"vest4_rankscore", "INFO_CSQ_tmp.VEST4_score":"vest4_score", \
            "INFO_CSQ_tmp.VindijiaNeandertal":"vindijianeandertal", "INFO_CSQ_tmp.aaalt":"aaalt", "INFO_CSQ_tmp.aapos":"aapos", "INFO_CSQ_tmp.aaref":"aaref", \
            "INFO_CSQ_tmp.alt":"alt", "INFO_CSQ_tmp.bStatistic":"bstatistic", "INFO_CSQ_tmp.bStatistic_converted_rankscore":"bstatistic_converted_rankscore", \
            "INFO_CSQ_tmp.cds_strand":"cds_strand","INFO_CSQ_tmp.chr":"chr","INFO_CSQ_tmp.clinvar_MedGen_id":"clinvar_medgen_id", "INFO_CSQ_tmp.clinvar_OMIM_id":"clinvar_omim_id",\
            "INFO_CSQ_tmp.clinvar_Orphanet_id":"clinvar_orphanet_id","INFO_CSQ_tmp.clinvar_clnsig":"clinvar_clnsig","INFO_CSQ_tmp.clinvar_hgvs":"clinvar_hgvs",\
            "INFO_CSQ_tmp.clinvar_id":"clinvar_id","INFO_CSQ_tmp.clinvar_review":"clinvar_review", "INFO_CSQ_tmp.clinvar_trait":"clinvar_trait",\
            "INFO_CSQ_tmp.clinvar_var_source":"clinvar_var_source","INFO_CSQ_tmp.codon_degeneracy":"codon_degeneracy", "INFO_CSQ_tmp.codonpos":"codonpos",\
            "INFO_CSQ_tmp.fathmm-MKL_coding_group":"fathmm_mkl_coding_group","INFO_CSQ_tmp.fathmm-MKL_coding_pred":"fathmm_mkl_coding_pred",\
            "INFO_CSQ_tmp.fathmm-MKL_coding_rankscore":"fathmm_mkl_coding_rankscore","INFO_CSQ_tmp.fathmm-MKL_coding_score":"fathmm_mkl_coding_score",\
            "INFO_CSQ_tmp.fathmm-XF_coding_pred":"fathmm_xf_coding_pred","INFO_CSQ_tmp.fathmm-XF_coding_rankscore":"fathmm_xf_coding_rankscore",\
            "INFO_CSQ_tmp.fathmm-XF_coding_score":"fathmm_xf_coding_score","INFO_CSQ_tmp.genename":"genename", "INFO_CSQ_tmp.gnomAD_exomes_AC":"gnomad_exomes_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_AF":"gnomad_exomes_af","INFO_CSQ_tmp.gnomAD_exomes_AFR_AC":"gnomad_exomes_afr_ac", "INFO_CSQ_tmp.gnomAD_exomes_AFR_AF":"gnomad_exomes_afr_af", \
            "INFO_CSQ_tmp.gnomAD_exomes_AFR_AN":"gnomad_exomes_afr_an", "INFO_CSQ_tmp.gnomAD_exomes_AFR_nhomalt":"gnomad_exomes_afr_nhomalt", \
            "INFO_CSQ_tmp.gnomAD_exomes_AMR_AC":"gnomad_exomes_amr_ac", "INFO_CSQ_tmp.gnomAD_exomes_AMR_AF":"gnomad_exomes_amr_af", \
            "INFO_CSQ_tmp.gnomAD_exomes_AMR_AN":"gnomad_exomes_amr_an", "INFO_CSQ_tmp.gnomAD_exomes_AMR_nhomalt":"gnomad_exomes_amr_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_AN":"gnomad_exomes_an", "INFO_CSQ_tmp.gnomAD_exomes_ASJ_AC":"gnomad_exomes_asj_ac", \
            "INFO_CSQ_tmp.gnomAD_exomes_ASJ_AF":"gnomad_exomes_asj_af", "INFO_CSQ_tmp.gnomAD_exomes_ASJ_AN":"gnomad_exomes_asj_an", \
            "INFO_CSQ_tmp.gnomAD_exomes_ASJ_nhomalt":"gnomad_exomes_asj_nhomalt", "INFO_CSQ_tmp.gnomAD_exomes_EAS_AC":"gnomad_exomes_eas_ac", \
            "INFO_CSQ_tmp.gnomAD_exomes_EAS_AF":"gnomad_exomes_eas_af", "INFO_CSQ_tmp.gnomAD_exomes_EAS_AN":"gnomad_exomes_eas_an", \
            "INFO_CSQ_tmp.gnomAD_exomes_EAS_nhomalt":"gnomad_exomes_eas_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_FIN_AC":"gnomad_exomes_fin_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_FIN_AF":"gnomad_exomes_fin_af","INFO_CSQ_tmp.gnomAD_exomes_FIN_AN":"gnomad_exomes_fin_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_FIN_nhomalt":"gnomad_exomes_fin_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_NFE_AC":"gnomad_exomes_nfe_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_NFE_AF":"gnomad_exomes_nfe_af","INFO_CSQ_tmp.gnomAD_exomes_NFE_AN":"gnomad_exomes_nfe_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_NFE_nhomalt":"gnomad_exomes_nfe_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_POPMAX_AC":"gnomad_exomes_popmax_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_POPMAX_AF":"gnomad_exomes_popmax_af","INFO_CSQ_tmp.gnomAD_exomes_POPMAX_AN":"gnomad_exomes_popmax_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_POPMAX_nhomalt":"gnomad_exomes_popmax_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_SAS_AC":"gnomad_exomes_sas_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_SAS_AF":"gnomad_exomes_sas_af","INFO_CSQ_tmp.gnomAD_exomes_SAS_AN":"gnomad_exomes_sas_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_SAS_nhomalt":"gnomad_exomes_sas_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_controls_AC":"gnomad_exomes_controls_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AF":"gnomad_exomes_controls_af","INFO_CSQ_tmp.gnomAD_exomes_controls_AFR_AC":"gnomad_exomes_controls_afr_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AFR_AF":"gnomad_exomes_controls_afr_af","INFO_CSQ_tmp.gnomAD_exomes_controls_AFR_AN":"gnomad_exomes_controls_afr_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AFR_nhomalt":"gnomad_exomes_controls_afr_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AMR_AC":"gnomad_exomes_controls_amr_ac", "INFO_CSQ_tmp.gnomAD_exomes_controls_AMR_AF":"gnomad_exomes_controls_amr_af",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AMR_AN":"gnomad_exomes_controls_amr_an", "INFO_CSQ_tmp.gnomAD_exomes_controls_AMR_nhomalt":"gnomad_exomes_controls_amr_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_AN":"gnomad_exomes_controls_an","INFO_CSQ_tmp.gnomAD_exomes_controls_ASJ_AC":"gnomad_exomes_controls_asj_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_ASJ_AF":"gnomad_exomes_controls_asj_af","INFO_CSQ_tmp.gnomAD_exomes_controls_ASJ_AN":"gnomad_exomes_controls_asj_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_ASJ_nhomalt":"gnomad_exomes_controls_asj_nhomalt","INFO_CSQ_tmp.gnomAD_exomes_controls_EAS_AC":"gnomad_exomes_controls_eas_ac",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_EAS_AF":"gnomad_exomes_controls_eas_af","INFO_CSQ_tmp.gnomAD_exomes_controls_EAS_AN":"gnomad_exomes_controls_eas_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_EAS_nhomalt":"gnomad_exomes_controls_eas_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_FIN_AC":"gnomad_exomes_controls_fin_ac", "INFO_CSQ_tmp.gnomAD_exomes_controls_FIN_AF":"gnomad_exomes_controls_fin_af",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_FIN_AN":"gnomad_exomes_controls_fin_an", "INFO_CSQ_tmp.gnomAD_exomes_controls_FIN_nhomalt":"gnomad_exomes_controls_fin_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_NFE_AC":"gnomad_exomes_controls_nfe_ac", "INFO_CSQ_tmp.gnomAD_exomes_controls_NFE_AF":"gnomad_exomes_controls_nfe_af",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_NFE_AN":"gnomad_exomes_controls_nfe_an", "INFO_CSQ_tmp.gnomAD_exomes_controls_NFE_nhomalt":"gnomad_exomes_controls_nfe_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_POPMAX_AC":"gnomad_exomes_controls_popmax_ac", "INFO_CSQ_tmp.gnomAD_exomes_controls_POPMAX_AF":"gnomad_exomes_controls_popmax_af",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_POPMAX_AN":"gnomad_exomes_controls_popmax_an",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_POPMAX_nhomalt":"gnomad_exomes_controls_popmax_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_SAS_AC":"gnomad_exomes_controls_sas_ac", "INFO_CSQ_tmp.gnomAD_exomes_controls_SAS_AF":"gnomad_exomes_controls_sas_af",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_SAS_AN":"gnomad_exomes_controls_sas_an", "INFO_CSQ_tmp.gnomAD_exomes_controls_SAS_nhomalt":"gnomad_exomes_controls_sas_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_exomes_controls_nhomalt":"gnomad_exomes_controls_nhomalt", "INFO_CSQ_tmp.gnomAD_exomes_flag":"gnomad_exomes_flag",\
            "INFO_CSQ_tmp.gnomAD_exomes_nhomalt":"gnomad_exomes_nhomalt", "INFO_CSQ_tmp.gnomAD_genomes_AC":"gnomad_genomes_ac", "INFO_CSQ_tmp.gnomAD_genomes_AF":"gnomad_genomes_af",\
            "INFO_CSQ_tmp.gnomAD_genomes_AFR_AC":"gnomad_genomes_afr_ac", "INFO_CSQ_tmp.gnomAD_genomes_AFR_AF":"gnomad_genomes_afr_af", \
            "INFO_CSQ_tmp.gnomAD_genomes_AFR_AN":"gnomad_genomes_afr_an", "INFO_CSQ_tmp.gnomAD_genomes_AFR_nhomalt":"gnomad_genomes_afr_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_AMI_AC":"gnomad_genomes_ami_ac", "INFO_CSQ_tmp.gnomAD_genomes_AMI_AF":"gnomad_genomes_ami_af", \
            "INFO_CSQ_tmp.gnomAD_genomes_AMI_AN":"gnomad_genomes_ami_an", "INFO_CSQ_tmp.gnomAD_genomes_AMI_nhomalt":"gnomad_genomes_ami_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_AMR_AC":"gnomad_genomes_amr_ac", "INFO_CSQ_tmp.gnomAD_genomes_AMR_AF":"gnomad_genomes_amr_af", \
            "INFO_CSQ_tmp.gnomAD_genomes_AMR_AN":"gnomad_genomes_amr_an", "INFO_CSQ_tmp.gnomAD_genomes_AMR_nhomalt":"gnomad_genomes_amr_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_AN":"gnomad_genomes_an", "INFO_CSQ_tmp.gnomAD_genomes_ASJ_AC":"gnomad_genomes_asj_ac",\
            "INFO_CSQ_tmp.gnomAD_genomes_ASJ_AF":"gnomad_genomes_asj_af","INFO_CSQ_tmp.gnomAD_genomes_ASJ_AN":"gnomad_genomes_asj_an",\
            "INFO_CSQ_tmp.gnomAD_genomes_ASJ_nhomalt":"gnomad_genomes_asj_nhomalt", "INFO_CSQ_tmp.gnomAD_genomes_EAS_AC":"gnomad_genomes_eas_ac",\
            "INFO_CSQ_tmp.gnomAD_genomes_EAS_AF":"gnomad_genomes_eas_af", "INFO_CSQ_tmp.gnomAD_genomes_EAS_AN":"gnomad_genomes_eas_an",\
            "INFO_CSQ_tmp.gnomAD_genomes_EAS_nhomalt":"gnomad_genomes_eas_nhomalt", "INFO_CSQ_tmp.gnomAD_genomes_FIN_AC":"gnomad_genomes_fin_ac",\
            "INFO_CSQ_tmp.gnomAD_genomes_FIN_AF":"gnomad_genomes_fin_af", "INFO_CSQ_tmp.gnomAD_genomes_FIN_AN":"gnomad_genomes_fin_an",\
            "INFO_CSQ_tmp.gnomAD_genomes_FIN_nhomalt":"gnomad_genomes_fin_nhomalt", "INFO_CSQ_tmp.gnomAD_genomes_NFE_AC":"gnomad_genomes_nfe_ac",\
            "INFO_CSQ_tmp.gnomAD_genomes_NFE_AF":"gnomad_genomes_nfe_af", "INFO_CSQ_tmp.gnomAD_genomes_NFE_AN":"gnomad_genomes_nfe_an",\
            "INFO_CSQ_tmp.gnomAD_genomes_NFE_nhomalt":"gnomad_genomes_nfe_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_POPMAX_AC":"gnomad_genomes_popmax_ac", "INFO_CSQ_tmp.gnomAD_genomes_POPMAX_AF":"gnomad_genomes_popmax_af",\
            "INFO_CSQ_tmp.gnomAD_genomes_POPMAX_AN":"gnomad_genomes_popmax_an", "INFO_CSQ_tmp.gnomAD_genomes_POPMAX_nhomalt":"gnomad_genomes_popmax_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_SAS_AC":"gnomad_genomes_sas_ac", "INFO_CSQ_tmp.gnomAD_genomes_SAS_AF":"gnomad_genomes_sas_af",\
            "INFO_CSQ_tmp.gnomAD_genomes_SAS_AN":"gnomad_genomes_sas_an", "INFO_CSQ_tmp.gnomAD_genomes_SAS_nhomalt":"gnomad_genomes_sas_nhomalt",\
            "INFO_CSQ_tmp.gnomAD_genomes_flag":"gnomad_genomes_flag", "INFO_CSQ_tmp.gnomAD_genomes_nhomalt":"gnomad_genomes_nhomalt",\
            "INFO_CSQ_tmp.hg18_chr":"hg18_chr", "INFO_CSQ_tmp.hg18_pos(1-based)":"hg18_pos_1_based",\
            "INFO_CSQ_tmp.hg19_chr":"hg19_chr", "INFO_CSQ_tmp.hg19_pos(1-based)":"hg19_pos_1_based",\
            "INFO_CSQ_tmp.integrated_confidence_value":"integrated_confidence_value", "INFO_CSQ_tmp.integrated_fitCons_rankscore":"integrated_fitcons_rankscore",\
            "INFO_CSQ_tmp.integrated_fitCons_score":"integrated_fitcons_score", "INFO_CSQ_tmp.phastCons100way_vertebrate":"phastcons100way_vertebrate",\
            "INFO_CSQ_tmp.phastCons100way_vertebrate_rankscore":"phastcons100way_vertebrate_rankscore",\
            "INFO_CSQ_tmp.phastCons17way_primate":"phastcons17way_primate", "INFO_CSQ_tmp.phastCons17way_primate_rankscore":"phastcons17way_primate_rankscore",\
            "INFO_CSQ_tmp.phastCons30way_mammalian":"phastcons30way_mammalian", "INFO_CSQ_tmp.phastCons30way_mammalian_rankscore":"phastcons30way_mammalian_rankscore",\
            "INFO_CSQ_tmp.phyloP100way_vertebrate":"phylop100way_vertebrate", "INFO_CSQ_tmp.phyloP100way_vertebrate_rankscore":"phylop100way_vertebrate_rankscore",\
            "INFO_CSQ_tmp.phyloP17way_primate":"phylop17way_primate", "INFO_CSQ_tmp.phyloP17way_primate_rankscore":"phylop17way_primate_rankscore",\
            "INFO_CSQ_tmp.phyloP30way_mammalian":"phylop30way_mammalian", "INFO_CSQ_tmp.phyloP30way_mammalian_rankscore":"phylop30way_mammalian_rankscore",\
            "INFO_CSQ_tmp.pos(1-based)":"pos_1_based", "INFO_CSQ_tmp.ref":"ref", "INFO_CSQ_tmp.refcodon":"refcodon", "INFO_CSQ_tmp.rs_dbSNP151":"rs_dbsnp151",\
            "INFO_CSQ_tmp.T-RExDB_AUT":"t_rexdb_aut", "INFO_CSQ_tmp.T-RExDB_AUT_AN_TH":"t_rexdb_aut_an_th",\
            "INFO_CSQ_tmp.T-RExDB_AUT_MAF_TH":"t_rexdb_aut_maf_th", "INFO_CSQ_tmp.T-RExDB_AUT_GTF_TH":"t_rexdb_aut_gtf_th",\
            "INFO_CSQ_tmp.T-RExDB_X":"t_rexdb_x", "INFO_CSQ_tmp.T-RExDB_X_AN_TH":"t_rexdb_x_an_th", "INFO_CSQ_tmp.T-RExDB_X_MAF_TH":"t_rexdb_x_maf_th",\
            "INFO_CSQ_tmp.T-RExDB_X_GTF_TH_M":"t_rexdb_x_gtf_th_m", "INFO_CSQ_tmp.T-RExDB_X_GTF_TH_F":"t_rexdb_x_gtf_th_f"}

glow_csq_names = [*columns_csq]
original_csq_names = list(columns_csq.values())
csq_df = extract_csq_df.select(glow_csq_names)
csq_df=csq_df.toDF(*original_csq_names)

write_options = {"table": "csq", 
                 "keyspace": "genome", 
                 "spark.cassandra.connection.host": 
                        "10.168.206.98:9042, 10.168.206.98:9043, \
                           10.168.206.98:9044, 10.168.206.98:9045"}
csq_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(**write_options)\
    .save()
