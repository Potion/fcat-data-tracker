from _common import download_dataset

GROUP = "OECD"
SOURCE_TYPE = "oecd"
DATASET_NAME = "Trust in Government (Map)"
DATASET_ID = "https://sdmx.oecd.org/public/rest/data/OECD.GOV.GG,DSD_GOV_TRUST@DF_TRUST_INST,1.0/.......?startPeriod=2020&dimensionAtObservation=AllDimensions"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
