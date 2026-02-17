from _common import download_dataset

GROUP = "FRED"
SOURCE_TYPE = "fred"
DATASET_NAME = "Cloud Costs"
DATASET_ID = "PCU518210518210"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
