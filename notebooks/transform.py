from dotenv import dotenv_values
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from google.cloud import storage

HOME = "/home/jovyan"
config = dotenv_values(HOME + "/.env")
CREDENTIALS = HOME + "/gcp_credentials.json"
BUCKET_NAME = config["project-name"] + "-" + config["bucket-name"]
GCS_CLIENT = storage.Client.from_service_account_json(CREDENTIALS)
INDUSTRIES_PATH = f"gs://{BUCKET_NAME}/raw/industries.parquet"
COMPANIES_PATH = f"gs://{BUCKET_NAME}/raw/companies.parquet"
SHAREPRICES_PATH = f"gs://{BUCKET_NAME}/raw/shareprices.parquet"

def get_file_names(folder, include_subfolders=False):
    bucket_handle = GCS_CLIENT.bucket(BUCKET_NAME)
    names = [
        b.name[(len(folder) + 1):]
        for b in bucket_handle.list_blobs(prefix=folder)
    ]
    if not include_subfolders:
        output = []
        for name in names:
            if len(name) > 1:
                if name[-1] == "/":
                    name = name[0:-1]
                if "/" not in name:
                    output.append(name)
        names = output
    return names

def get_processed_year_months():
    names = get_file_names("processed")
    return sorted([n[0:6] for n in names])

processed_year_months = get_processed_year_months()
last_processed_year_month = processed_year_months[-1]

spark = (
    SparkSession
    .builder
    .master("local")
    .appName("test")
    .getOrCreate()
)
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", CREDENTIALS)

industries = (
    spark
    .read
    .option("header", "true")
    .parquet(INDUSTRIES_PATH)
)
companies = (
    spark
    .read
    .option("header", "true")
    .parquet(COMPANIES_PATH)
)
shareprices = (
    spark
    .read
    .option("header", "true")
    .parquet(SHAREPRICES_PATH)
)

df_1 = (
    shareprices
    .join(companies, shareprices.SimFinId == companies.SimFinId)
)
df_2 = (
    df_1
    .join(industries, df_1.IndustryId == industries.IndustryId)
    .select("Date", "Low", "High", "Company Name", "Sector", "Open", "Close")
)

df_2 = (
    df_2
    .withColumn("Turmoil", 100 * f.abs(df_2["High"] - df_2["Low"]) / df_2["Low"])
    .withColumn("Net Change", 100 * (df_2["Close"] - df_2["Open"]) / df_2["Open"])
    .withColumn("Avg Price", (df_2["Close"] + df_2["Open"]) / 2)
)
value_cols = ["Turmoil", "Net Change", "Avg Price"]

day_totals = df_2.groupBy("Date").agg(*[f.avg(c).alias(c) for c in value_cols])
day_totals = day_totals.withColumn("Company Name", f.lit("Total"))
day_totals = day_totals.withColumn("Sector", f.lit("Total"))
sector_totals = df_2.groupBy("Date", "Sector").agg(*[f.avg(c).alias(c) for c in value_cols])
sector_totals = sector_totals.withColumn("Company Name", f.concat(f.col("Sector"), f.lit(" Total")))

df = (
    df_2
    .unionByName(day_totals, allowMissingColumns=True)
    .unionByName(sector_totals, allowMissingColumns=True)
)
df = (
    df
    .withColumn("Year", f.year("Date"))
    .withColumn("Month", f.format_string('%02d', f.month("Date")))
)
df = df.withColumn("Year-Month", f.concat("Year", "Month"))
df = df.drop("Year").drop("Month")

year_months = sorted([
    v["Year-Month"]
    for v in df.select("Year-Month").distinct().collect()
])

for year_month in year_months: 
    if (year_month not in processed_year_months) or (year_month == last_processed_year_month):
        print(f"Saving {year_month}...")
        month = df.select("*").where(df["Year-Month"] == year_month)
        month.write.parquet(
            f"gs://{BUCKET_NAME}/processed/{year_month}_shares.parquet",
            mode="overwrite"
        )