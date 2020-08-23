import datetime
import time
import operate_database
import pymongo
import os
import glob
import zipfile

st = time.time()
MONGO_CLIENT_URL = "mongodb://localhost:27017/"
DB_NAME = "mydatabase"


def recreate_collections():
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELL_NAMES")
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_GNODEB_NAMES")
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELLS_HOURLY",
                                          unique_index=("Time", "Cell Name",),
                                          single_indice=["Cell Name", "Time", "gNodeB Name"])
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELLS_DAILY",
                                          unique_index=("Date", "Cell Name",),
                                          single_indice=["Cell Name", "Date", "gNodeB Name"])


def upgrade_5G_hourly_data(fn, COLLECTIONS_NAME="NR_CELLS_HOURLY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("Cell Name", "NR_CELL_NAMES"),
                                                       ("gNodeB Name", "NR_GNODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Time"],
                                 skiprows=6,
                                 unique_index=("Time", "Cell Name",))


def upgrade_5G_daily_data(fn, COLLECTIONS_NAME="NR_CELLS_DAILY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("Cell Name", "NR_CELL_NAMES"),
                                                       ("gNodeB Name", "NR_GNODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Date"],
                                 skiprows=6,
                                 unique_index=("Date", "Cell Name",))


def update_data():
    if not os.path.exists(os.path.join("data", "extract")):
        os.makedirs(os.path.join("data", "extract"))

    for fn in glob.glob(os.path.join("data", "*.zip")):
        for temp in glob.glob(os.path.join("data", "extract", "*")):
            os.remove(temp)

        with zipfile.ZipFile(fn, "r") as zipref:
            print("extracting {}".format(fn))
            zipref.extractall(os.path.join("data", "extract"))

        for extract_fn in glob.glob(os.path.join("data", "extract", "*")):
            print("importing {}".format(extract_fn))
            if "5G" in fn:
                if "Hour" in extract_fn:
                    upgrade_5G_hourly_data(extract_fn)
                else:
                    upgrade_5G_daily_data(extract_fn)


# recreate_collections()
update_data()

# col = pymongo.MongoClient(MONGO_CLIENT_URL)[DB_NAME][COLLECTIONS_NAME]
# for doc in col.find({"Cell Name": "95021NCn781", "Time": datetime.datetime(2020, 4, 26, 0, 0)}):
#     print(doc)
