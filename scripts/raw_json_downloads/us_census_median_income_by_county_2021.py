from _common import download_dataset

GROUP = "US Census"
SOURCE_TYPE = "census"
DATASET_NAME = "Median Income by County (2021)"
DATASET_ID = "https://api.census.gov/data/2021/acs/acs1/profile?get=NAME,DP03_0062E&for=county:*"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
