from _common import download_dataset

GROUP = "ECB"
SOURCE_TYPE = "ecb"
DATASET_NAME = "USD/EUR Exchange Rate"
DATASET_ID = "EXR.D.USD.EUR.SP00.A"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
