from _common import download_dataset

GROUP = "OECD"
SOURCE_TYPE = "oecd"
DATASET_NAME = "Scientific Collaboration (2021)"
DATASET_ID = "https://sdmx.oecd.org/public/rest/data/OECD.STI.STP,DSD_BIBLIO@DF_BIBLIO_COLLAB,1.1/all?startPeriod=2021&endPeriod=2021&dimensionAtObservation=AllDimensions"

if __name__ == "__main__":
    download_dataset(GROUP, DATASET_NAME, SOURCE_TYPE, DATASET_ID)
