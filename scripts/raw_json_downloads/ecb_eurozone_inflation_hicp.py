from _common import download_dataset

GROUP = "ECB"
SOURCE_TYPE = "ecb"
DATASET_NAME = "Eurozone Inflation (HICP)"
DATASET_ID = "ICP.M.U2.N.000000.4.ANR"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
