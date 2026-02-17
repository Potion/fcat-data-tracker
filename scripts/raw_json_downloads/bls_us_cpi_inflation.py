from _common import download_dataset

GROUP = "BLS"
SOURCE_TYPE = "bls"
DATASET_NAME = "US CPI (Inflation)"
DATASET_ID = "CUSR0000SA0"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
