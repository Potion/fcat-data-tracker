from _common import download_dataset

GROUP = "OECD"
SOURCE_TYPE = "oecd"
DATASET_NAME = "USA GDP (Quarterly)"
DATASET_ID = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA,1.1/Q.USA.B1GQ...?startPeriod=2015-Q1&dimensionAtObservation=AllDimensions"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
