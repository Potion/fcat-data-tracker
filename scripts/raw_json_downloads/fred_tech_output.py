from _common import download_dataset

GROUP = "FRED"
SOURCE_TYPE = "fred"
DATASET_NAME = "Tech Output"
DATASET_ID = "IPB51222S"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
