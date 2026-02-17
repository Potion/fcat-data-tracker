from _common import download_dataset

GROUP = "US Census"
SOURCE_TYPE = "census"
DATASET_NAME = "Population by State (2020)"
DATASET_ID = "https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=state:*"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
