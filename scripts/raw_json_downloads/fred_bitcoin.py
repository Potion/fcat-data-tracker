from _common import download_dataset

GROUP = "FRED"
SOURCE_TYPE = "fred"
DATASET_NAME = "Bitcoin"
DATASET_ID = "CBBTCUSD"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
