from pathlib import Path
from prefect import flow, task
from prefect.blocks.system import JSON
import pandas as pd
import io
from prefect_gcp.cloud_storage import GcsBucket
from config import BUCKET_BLOCK, SIM_FIM_API_KEY_BLOCK
import simfin as sf

RAW_PATH = Path(__file__).parent.parent / "data" / "raw"

@task(log_prints=True)
def fetch() -> pd.DataFrame:
    sf_api_key = JSON.load(SIM_FIM_API_KEY_BLOCK).value
    sf.set_api_key(sf_api_key)
    sf.set_data_dir(RAW_PATH)
    data = {
        "companies": sf.load_companies(market='us'),
        "shareprices": sf.load_shareprices(variant='daily', market='us'),
        "industries": sf.load_industries()
    }
    return data


@task()
def write_gcs(data: dict[str, pd.DataFrame]) -> None:
    gcs_block = GcsBucket.load(BUCKET_BLOCK)
    for key in data:
        bytes = io.BytesIO()
        data[key].to_parquet(bytes, compression="gzip")
        bytes.seek(0)
        gcs_block.upload_from_file_object(bytes, f"raw/{key}.parquet")
    return


@flow()
def extract_flow():
    data = fetch()
    write_gcs(data)


if __name__ == "__main__":
    extract_flow()