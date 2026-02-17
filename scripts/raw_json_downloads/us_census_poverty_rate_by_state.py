from _common import download_dataset

GROUP = "US Census"
SOURCE_TYPE = "census"
DATASET_NAME = "Poverty Rate by State"
DATASET_ID = "https://api.census.gov/data/timeseries/poverty/saipe?get=NAME,SAEPOVRTALL_PT&for=state:*&time=2021"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
