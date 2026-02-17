from _common import download_dataset

GROUP = "35 Years"
SOURCE_TYPE = "fred"
DATASET_NAME = "Expenses (Age 35-44)"
DATASET_ID = "CXUTOTALEXPLB0404M"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
