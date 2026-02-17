from _common import download_dataset

GROUP = "35 Years"
SOURCE_TYPE = "fred"
DATASET_NAME = "Median Household Income"
DATASET_ID = "MEHOINUSA646N"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
