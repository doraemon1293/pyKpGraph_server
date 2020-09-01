import datetime
import time
import operate_database
import pymongo
import os
import glob
import zipfile
import time

st = time.time()
MONGO_CLIENT_URL = "mongodb://localhost:27017/"
DB_NAME = "mydatabase"


def recreate_collections():
    # 5G Colelctions
    # operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELL_NAMES")
    # operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_GNODEB_NAMES")
    # operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELLS_HOURLY",
    #                                       compound_indice=[("Cell Name", "Time"), ("gNodeB Name", "Time")]
    #                                       )
    # operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "NR_CELLS_DAILY",
    #                                       compound_indice=[("Cell Name", "Date"), ("gNodeB Name", "Date")]
    #                                       )

    # 4G Collections
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_CELL_NAMES")
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_ENODEB_NAMES")

    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_CELLS_HOURLY",
                                          compound_indice=[("Cell Name", "Time"), ("eNodeB Name", "Time")]
                                          )
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_ETHS_HOURLY",
                                          compound_indice=[("eNodeB Name", "Time")]
                                          )

    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_CELLS_DAILY",
                                          compound_indice=[("Cell Name", "Date"), ("eNodeB Name", "Date")]
                                          )
    operate_database.re_create_collection(MONGO_CLIENT_URL, DB_NAME, "LTE_ETHS_DAILY",
                                          compound_indice=[("eNodeB Name", "Date")]
                                          )


def upgrade_4G_hourly_cell_data(fn, COLLECTIONS_NAME="LTE_CELLS_HOURLY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("Cell Name", "LTE_CELL_NAMES"),
                                                       ("eNodeB Name", "LTE_ENODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Time"],
                                 skiprows=6,
                                 unique_index=("Time", "Cell Name",),
                                 )


def upgrade_4G_hourly_eth_data(fn, COLLECTIONS_NAME="LTE_ETHS_HOURLY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("eNodeB Name", "LTE_ENODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Time"],
                                 skiprows=6,
                                 unique_index=("Time", "eNodeB Name", "Cabinet No", "Port No", "Slot No", "Subrack No"),
                                 )


def upgrade_4G_daily_cell_data(fn, COLLECTIONS_NAME="LTE_CELLS_DAILY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("Cell Name", "LTE_CELL_NAMES"),
                                                       ("eNodeB Name", "LTE_ENODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Date"],
                                 skiprows=6,
                                 unique_index=("Date", "Cell Name",),
                                 )


def upgrade_4G_daily_eth_data(fn, COLLECTIONS_NAME="LTE_ETHS_DAILY"):
    operate_database.upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME,
                                 auto_complete_fields=[("eNodeB Name", "LTE_ENODEB_NAMES")],
                                 filename=fn,
                                 parse_dates=["Date"],
                                 skiprows=6,
                                 unique_index=("Date", "eNodeB Name", "Cabinet No", "Port No", "Slot No", "Subrack No"),
                                 )


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
    st = time.time()
    for fn in glob.glob(os.path.join("data", "*.zip")):
        for temp in glob.glob(os.path.join("data", "extract", "*")):
            os.remove(temp)

        with zipfile.ZipFile(fn, "r") as zipref:
            print("extracting {}".format(fn))
            zipref.extractall(os.path.join("data", "extract"))
        print("extract", time.time() - st)
        for extract_fn in glob.glob(os.path.join("data", "extract", "*")):
            print("importing {}".format(extract_fn))
            if "5G" in extract_fn:
                if "Hour" in extract_fn:
                    upgrade_5G_hourly_data(extract_fn)
                else:
                    upgrade_5G_daily_data(extract_fn)
            if "4G" in extract_fn:
                if "Hour" in extract_fn:
                    if "BBU" in fn or "Eth" in fn:
                        upgrade_4G_hourly_eth_data(extract_fn)
                    else:
                        upgrade_4G_hourly_cell_data(extract_fn)
                else:
                    if "BBU" in fn or "Eth" in fn:
                        upgrade_4G_daily_eth_data(extract_fn)
                    else:
                        upgrade_4G_daily_eth_data(extract_fn)

print("start:",time.time())
recreate_collections()
update_data()
print("finish:",time.time())
# col = pymongo.MongoClient(MONGO_CLIENT_URL)[DB_NAME][COLLECTIONS_NAME]
# for doc in col.find({"Cell Name": "95021NCn781", "Time": datetime.datetime(2020, 4, 26, 0, 0)}):
#     print(doc)
