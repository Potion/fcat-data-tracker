from _common import download_dataset

GROUP = "CoinGecko"
SOURCE_TYPE = "coingecko"
DATASET_NAME = "Ethereum History"
DATASET_ID = "ethereum"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
