
import os
import logging
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GoldJob")

# ─── Config ───────────────────────────────────────────────────────────────────
SILVER_BASE  = os.environ.get("SILVER_BASE", "hdfs://namenode:9000/warehouse/silver")
GOLD_BASE    = os.environ.get("GOLD_BASE",   "hdfs://namenode:9000/warehouse/gold")
ACTIVE_ETATC = "11"
TARGET_PRODUCTS = {
    "label_maRetraite":       "MaRetraite",
    "label_avenirMesEnfants": "AVENIR MESENFANTS",
    "label_epargneEvolution": "EPARGNE EVOLUTION",
}

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("GoldJob")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

job_start = time.time()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — BASE: all clients from perimetre
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 1 — Loading client base from silver_perimetre")

perimetre = spark.read.parquet(f"{SILVER_BASE}/silver_perimetre")

base = perimetre.select(
    "RADICAL",
    "BANQUE",
    "AGENCE",
    "DATE_OF_BIRTH",
    "CODE_VILLE",
    "LIBELLE_VILLE",
    "BPR",
    "GENDER",
    "MARITAL_STATUS",
    "NOMBRE_ENFANT",
    "CUSTOMER_RATING",
    "TAILLE_ENTREPRI",
).withColumn(
    "age",
    F.floor(F.datediff(F.current_date(), F.col("DATE_OF_BIRTH")) / 365)
).drop("DATE_OF_BIRTH")

logger.info(f"  Base clients: {base.count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — LABELS from silver_assurance
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 2 — Building labels from silver_assurance")

assurance = spark.read.parquet(f"{SILVER_BASE}/silver_assurance")

active_assi = assurance.filter(F.col("ETATC") == ACTIVE_ETATC)

labels = active_assi.groupBy("RADICAL").agg(
    F.max(F.when(
        F.col("LIBELLE_PRODUIT") == TARGET_PRODUCTS["label_maRetraite"], 1
    ).otherwise(0)).alias("label_maRetraite"),

    F.max(F.when(
        F.col("LIBELLE_PRODUIT") == TARGET_PRODUCTS["label_avenirMesEnfants"], 1
    ).otherwise(0)).alias("label_avenirMesEnfants"),

    F.max(F.when(
        F.col("LIBELLE_PRODUIT") == TARGET_PRODUCTS["label_epargneEvolution"], 1
    ).otherwise(0)).alias("label_epargneEvolution"),

    F.max(F.when(
        F.col("LIBELLE_PRODUIT") == "ATTAMINE CHAABI HISSAB", 1
    ).otherwise(0)).alias("has_attamine"),

    F.max(F.when(
        F.col("LIBELLE_PRODUIT").contains("INJAD"), 1
    ).otherwise(0)).alias("has_injad"),

    F.countDistinct("CODE_PRODUIT").alias("nb_insurance_products"),
)

logger.info(f"  Label distribution:")
labels.agg(
    F.sum("label_maRetraite").alias("maRetraite_positives"),
    F.sum("label_avenirMesEnfants").alias("avenir_positives"),
    F.sum("label_epargneEvolution").alias("epargne_positives"),
).show()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — ACCOUNT FEATURES from silver_comptes
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 3 — Account features from silver_comptes")

comptes = spark.read.parquet(f"{SILVER_BASE}/silver_comptes")

comptes_features = comptes.groupBy("RADICAL").agg(
    F.countDistinct("CCLE").alias("nb_accounts"),
    F.min("DTOUVR").alias("first_account_date"),
    F.datediff(
        F.current_date(), F.min("DTOUVR")
    ).alias("anciennete_days"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — WEALTH FEATURES from silver_soldes
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 4 — Wealth features from silver_soldes")

soldes = spark.read.parquet(f"{SILVER_BASE}/silver_soldes")

_last_w = Window.partitionBy("RADICAL").orderBy(F.col("DATE_CHARG").desc())
_last_balance = (
    soldes
    .withColumn("_rn", F.row_number().over(_last_w))
    .filter(F.col("_rn") == 1)
    .select("RADICAL", F.col("SOLDEVERIF").alias("last_balance"))
)

soldes_features = soldes.groupBy("RADICAL").agg(
    F.avg("SOLDEVERIF").alias("avg_balance"),
    F.max("SOLDEVERIF").alias("max_balance"),
    F.min("SOLDEVERIF").alias("min_balance"),
    F.stddev("SOLDEVERIF").alias("std_balance"),
    F.count("DATE_CHARG").alias("nb_balance_snapshots"),
).join(_last_balance, on="RADICAL", how="left")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — INCOME FEATURES from silver_flux
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 5 — Income features from silver_flux")

flux = spark.read.parquet(f"{SILVER_BASE}/silver_flux")

flux_features = flux.groupBy("RADICAL").agg(
    F.sum("FLUX_CRED").alias("total_flux_cred"),
    F.avg("FLUX_CRED").alias("avg_flux_cred"),
    F.max("FLUX_CRED").alias("max_flux_cred"),
    F.stddev("FLUX_CRED").alias("std_flux_cred"),
    F.countDistinct("DATE_CHARG").alias("nb_flux_months"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — GAB FEATURES from silver_gab
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 6 — GAB features from silver_gab")

gab = spark.read.parquet(f"{SILVER_BASE}/silver_gab")

gab_features = gab.groupBy("RADICAL").agg(
    F.count("*").alias("nb_gab_transactions"),
    F.sum("MONTANT").alias("total_gab_amount"),
    F.avg("MONTANT").alias("avg_gab_amount"),
    F.max("MONTANT").alias("max_gab_amount"),
    F.countDistinct("DATE_OP").alias("nb_gab_active_days"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — TPE FEATURES from silver_tpe
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 7 — TPE features from silver_tpe")

tpe = spark.read.parquet(f"{SILVER_BASE}/silver_tpe")

tpe_features = tpe.groupBy("RADICAL").agg(
    F.count("*").alias("nb_tpe_transactions"),
    F.sum("MONTANT_TRANSACTION").alias("total_tpe_amount"),
    F.avg("MONTANT_TRANSACTION").alias("avg_tpe_amount"),
    F.max("MONTANT_TRANSACTION").alias("max_tpe_amount"),
    F.countDistinct("DATE_ACHAT").alias("nb_tpe_active_days"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — RETRAIT FEATURES from silver_retrait
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 8 — Retrait features from silver_retrait")

retrait = spark.read.parquet(f"{SILVER_BASE}/silver_retrait")

retrait_features = retrait.groupBy("RADICAL").agg(
    F.count("*").alias("nb_retraits"),
    F.sum("MONTANT").alias("total_retrait_amount"),
    F.avg("MONTANT").alias("avg_retrait_amount"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — ONLINE FEATURES from silver_online_ope
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 9 — Online features from silver_online_ope")

online = spark.read.parquet(f"{SILVER_BASE}/silver_online_ope")

online_features = online.groupBy("RADICAL").agg(
    F.count("*").alias("nb_online_transactions"),
    F.sum("MONTANT").alias("total_online_amount"),
    F.avg("MONTANT").alias("avg_online_amount"),
    F.countDistinct("DATE_ACHAT").alias("nb_online_active_days"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 10 — DIGITAL FEATURES from silver_digital
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 10 — Digital features from silver_digital")

digital = spark.read.parquet(f"{SILVER_BASE}/silver_digital")

digital_features = digital.groupBy("RADICAL").agg(
    F.lit(1).alias("has_digital_product"),
    F.max(F.when(
        F.col("DATE_RES_ABON").isNull(), 1
    ).otherwise(0)).alias("has_active_digital"),
    F.countDistinct("CLE").alias("nb_digital_products"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 11 — PAYFAC FEATURES from silver_payfac
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 11 — Payfac features from silver_payfac")

payfac = spark.read.parquet(f"{SILVER_BASE}/silver_payfac")

payfac_features = payfac.groupBy("RADICAL").agg(
    F.count("*").alias("nb_payfac_transactions"),
    F.sum("MONTANT_TOTAL").alias("total_payfac_amount"),
    F.avg("MONTANT_TOTAL").alias("avg_payfac_amount"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 12 — VIREMENT FEATURES from silver_virement
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 12 — Virement features from silver_virement")

virement = spark.read.parquet(f"{SILVER_BASE}/silver_virement")

virement_features = virement.filter(
    F.col("ETAT").isin(["1111", "1112"])
).groupBy("RADICAL").agg(
    F.count("*").alias("nb_virements"),
    F.sum("MT_OPE").alias("total_virement_amount"),
    F.avg("MT_OPE").alias("avg_virement_amount"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 13 — CARTE FEATURES from silver_carte
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 13 — Carte features from silver_carte")

carte = spark.read.parquet(f"{SILVER_BASE}/silver_carte")

carte_features = carte.groupBy("RADICAL").agg(
    F.lit(1).alias("has_carte"),
    F.countDistinct("NUMERO_CARTE").alias("nb_cartes"),
    F.max(F.when(
        F.col("DT_FIN_VALID") >= F.current_date(), 1
    ).otherwise(0)).alias("has_valid_carte"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 14 — OPK FEATURES from silver_opk
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 14 — OPK features from silver_opk")

opk = spark.read.parquet(f"{SILVER_BASE}/silver_opk")

opk_features = opk.groupBy("RADICAL").agg(
    F.lit(1).alias("has_pack"),
    F.countDistinct("CODE_PACK").alias("nb_packs_ever"),
    F.max(F.when(
        F.col("ETATC") == "V", 1
    ).otherwise(0)).alias("has_active_pack"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 15 — DEPOT FEATURES from silver_depot
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 15 — Depot features from silver_depot")

depot = spark.read.parquet(f"{SILVER_BASE}/silver_depot")

depot_features = depot.groupBy("RADICAL").agg(
    F.count("*").alias("nb_depots"),
    F.sum("MONTANT_DEPOT").alias("total_depot_amount"),
    F.avg("MONTANT_DEPOT").alias("avg_depot_amount"),
    F.max(F.when(
        F.col("TYPE_DEPOT") == "Dépôts à terme", 1
    ).otherwise(0)).alias("has_depot_terme"),
    F.max(F.when(
        F.col("TYPE_DEPOT") == "Dépôts à vue", 1
    ).otherwise(0)).alias("has_depot_vue"),
    F.max(F.when(
        F.col("TYPE_DEPOT") == "Bons de caisse", 1
    ).otherwise(0)).alias("has_bon_caisse"),
    F.max(F.when(
        F.col("TYPE_DEPOT") == "Comptes sur carnets", 1
    ).otherwise(0)).alias("has_compte_carnet"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 16 — MAD FEATURES from silver_mad
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 16 — MAD features from silver_mad")

mad = spark.read.parquet(f"{SILVER_BASE}/silver_mad")

mad_features = mad.groupBy("RADICAL").agg(
    F.count("*").alias("nb_mad_operations"),
    F.sum("MONTANT").alias("total_mad_amount"),
    F.avg("MONTANT").alias("avg_mad_amount"),
    F.sum(F.when(
        F.col("TYPE") == "MAD GAB", 1
    ).otherwise(0)).alias("nb_mad_gab"),
    F.sum(F.when(
        F.col("TYPE") == "MAD AGENCE", 1
    ).otherwise(0)).alias("nb_mad_agence"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 17 — VIGNETTE FEATURES from silver_vignette
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 17 — Vignette features from silver_vignette")

vignette = spark.read.parquet(f"{SILVER_BASE}/silver_vignette")

vignette_features = vignette.groupBy("RADICAL").agg(
    F.lit(1).alias("has_vignette"),
    F.countDistinct("ANNEE_PAIEMENT").alias("nb_vignette_years"),
    F.sum("TOTAL_TTC").alias("total_vignette_amount"),
    F.max("ANNEE_PAIEMENT").alias("last_vignette_year"),
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 18 — MASTER TABLE JOIN
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 18 — Building master table")

master = (
    base
    .join(labels,           on="RADICAL", how="left")
    .join(comptes_features, on="RADICAL", how="left")
    .join(soldes_features,  on="RADICAL", how="left")
    .join(flux_features,    on="RADICAL", how="left")
    .join(gab_features,     on="RADICAL", how="left")
    .join(tpe_features,     on="RADICAL", how="left")
    .join(retrait_features, on="RADICAL", how="left")
    .join(online_features,  on="RADICAL", how="left")
    .join(digital_features, on="RADICAL", how="left")
    .join(payfac_features,  on="RADICAL", how="left")
    .join(virement_features,on="RADICAL", how="left")
    .join(carte_features,   on="RADICAL", how="left")
    .join(opk_features,     on="RADICAL", how="left")
    .join(depot_features,   on="RADICAL", how="left")
    .join(mad_features,     on="RADICAL", how="left")
    .join(vignette_features,on="RADICAL", how="left")
)

# ── Fill nulls ─────────────────────────────────────────────────────────────
numeric_fill = {col: 0 for col in master.columns
                if col not in ["RADICAL", "GENDER", "MARITAL_STATUS",
                               "CODE_VILLE", "LIBELLE_VILLE", "BPR",
                               "TAILLE_ENTREPRI", "first_account_date"]}
master = master.fillna(numeric_fill)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 19 — DERIVED FEATURES
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 19 — Computing derived features")

master = master.withColumn(
    "savings_ratio",
    F.col("avg_balance") / (F.col("avg_flux_cred") + 1)
).withColumn(
    "digital_score",
    F.col("has_digital_product") +
    F.when(F.col("nb_payfac_transactions") > 0, 1).otherwise(0) +
    F.when(F.col("nb_online_transactions") > 0, 1).otherwise(0)
).withColumn(
    "spending_diversity",
    F.when(F.col("nb_gab_transactions") > 0, 1).otherwise(0) +
    F.when(F.col("nb_tpe_transactions") > 0, 1).otherwise(0) +
    F.when(F.col("nb_online_transactions") > 0, 1).otherwise(0) +
    F.when(F.col("nb_virements") > 0, 1).otherwise(0)
).withColumn(
    "product_breadth",
    F.col("has_carte") +
    F.col("has_pack") +
    F.col("has_digital_product") +
    F.col("has_vignette") +
    F.col("has_attamine") +
    F.col("has_injad") +
    F.when(F.col("nb_depots") > 0, 1).otherwise(0)
).withColumn(
    "balance_trend",
    F.col("last_balance") - F.col("min_balance")
).withColumn(
    "avg_monthly_spend",
    (F.col("total_gab_amount") +
     F.col("total_tpe_amount") +
     F.col("total_online_amount")) /
    F.greatest(F.col("nb_flux_months"), F.lit(1))
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 20 — WRITE TO GOLD
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Step 20 — Writing master table to Gold layer")

out_path = f"{GOLD_BASE}/master_table"

master.repartition(8).write.mode("overwrite").parquet(out_path)

total_cols = len(master.columns)
elapsed    = time.time() - job_start

logger.info(f"Master table written to {out_path}")
logger.info(f"Columns : {total_cols}")
logger.info(f"Elapsed : {elapsed:.1f}s")

# ── Single-pass summary: row count + label distribution ───────────────────
logger.info("Label distribution in master table:")
summary_row = master.agg(
    F.count("RADICAL").alias("total_clients"),
    F.sum("label_maRetraite").alias("maRetraite"),
    F.sum("label_avenirMesEnfants").alias("avenirMesEnfants"),
    F.sum("label_epargneEvolution").alias("epargneEvolution"),
).collect()[0]

logger.info(f"Rows    : {summary_row['total_clients']:,}")
logger.info(
    f"Labels  : maRetraite={summary_row['maRetraite']:,}  "
    f"avenirMesEnfants={summary_row['avenirMesEnfants']:,}  "
    f"epargneEvolution={summary_row['epargneEvolution']:,}"
)

spark.stop()
logger.info("GoldJob complete.")