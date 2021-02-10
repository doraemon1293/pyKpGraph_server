import datetime
import time
import operate_database
import pymongo
import os
import glob
import zipfile
import time
import pandas as pd
import shutil

st = time.time()
MONGO_CLIENT_URL = "mongodb://localhost:27017/"
NR_DB_NAME = "NR_TEST_DB"
LTE_DB_NAME = "LTE_TEST_DB"
GSM_DB_NAME = "GSM_TEST_DB"
EP_DB_NAME = "EP"
LTE_EP_COL_NAME = "LTE_EP"
CLUTSER_COL_NAME = "CLUSTERS"
ISDP_COL_NAME = "ISDP"


def get_bandwidth(row):
    if row["Downlink bandwidth"] == "CELL_BW_N25":
        return 5
    elif row["Downlink bandwidth"] == "CELL_BW_N50":
        return 10
    elif row["Downlink bandwidth"] == "CELL_BW_N75":
        return 15
    elif row["Downlink bandwidth"] == "CELL_BW_N100":
        return 20
    else:
        return float('nan')


def get_layer(row):
    if row["Cell Name"][-2:] == "80":
        return "L8"
    if row["Cell Name"][-2:] == "11":
        return "L18"
    elif row["Cell Name"][-2:] == "18":
        return "L18/2"
    elif row["Cell Name"][-2:] == "21":
        return "L21"
    elif row["Cell Name"][-2:] == "26":
        return "L26"
    elif row["Cell Name"][-2:] == "27":
        return "L26/2"
    else:
        return None


def upgrade_2G_hourly_cell_data(fn, zipref, gsm_db_operator):
    gsm_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="GSM_CELLS_HOURLY_",
        filename=fn,
        parse_dates=["Time"],
        skiprows=6,
        indice=[("Cell Name",), ("Site Name",)],
    )


def upgrade_2G_daily_cell_data(fn, zipref, gsm_db_operator):
    gsm_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="GSM_CELLS_DAILY_",
        filename=fn,
        parse_dates=["Date"],
        skiprows=6,
        indice=[("Cell Name",), ("Site Name",)],
    )


def upgrade_2G_daily_cell_data(fn, zipref, gsm_db_operator):
    gsm_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="GSM_CELLS_DAILY_",
        filename=fn,
        parse_dates=["Date"],
        skiprows=6,
        indice=[("Cell Name",), ("Site Name",)],
    )


def upgrade_4G_hourly_cell_data(fn, zipref, lte_db_operator):
    lte_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="LTE_CELLS_HOURLY_",
        filename=fn,
        parse_dates=["Time"],
        skiprows=6,
        indice=[("eNodeB Name",), ("Cell Name",)],
    )


def upgrade_4G_daily_cell_data(fn, zipref, lte_db_operator):
    lte_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="LTE_CELLS_DAILY_",
        filename=fn,
        parse_dates=["Date"],
        skiprows=6,
        indice=[("eNodeB Name",), ("Cell Name",)],
    )


def upgrade_5G_hourly_data(fn, zipref, nr_db_operator):
    nr_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="NR_CELLS_HOURLY_",
        filename=fn,
        parse_dates=["Time"],
        skiprows=6,
        indice=[("gNodeB Name",), ("Cell Name",)],
    )


def upgrade_5G_daily_data(fn, zipref, nr_db_operator):
    nr_db_operator.upsert_data(
        zipref=zipref,
        collection_name_prefix="NR_CELLS_DAILY_",
        filename=fn,
        parse_dates=["Date"],
        skiprows=6,
        indice=[("gNodeB Name",), ("Cell Name",)],
    )


def update_data():
    for fn in glob.glob(os.path.join("data", "*.zip")):
        print(fn)
        with zipfile.ZipFile(fn, "r") as zipref:
            for fn in zipref.namelist():
                print(fn)
                if "5G" in fn:
                    if "Hour" in fn:
                        upgrade_5G_hourly_data(fn, zipref, nr_db_operator)
                    else:
                        upgrade_5G_daily_data(fn, zipref, nr_db_operator)
                if "4G" in fn:
                    if "Hour" in fn:
                        upgrade_4G_hourly_cell_data(fn, zipref, lte_db_operator)
                    else:
                        upgrade_4G_daily_cell_data(fn, zipref, lte_db_operator)
                if "2G" in fn:
                    if "Hour" in fn:
                        upgrade_2G_hourly_cell_data(fn, zipref, gsm_db_operator)
                    else:
                        upgrade_2G_daily_cell_data(fn, zipref, gsm_db_operator)


def update_ep(fn="ep.csv"):
    df = pd.read_csv(fn, usecols=["Base Station Name", "Cell Name", "Downlink bandwidth"],
                     dtype={"Cell Name": str, "Base Station Name": str})
    df["_id"] = df["Cell Name"]
    df["bandwidth"] = df.apply(lambda row: get_bandwidth(row), axis=1)
    df["layer"] = df.apply(lambda row: get_layer(row), axis=1)
    df["eNodeB Name"] = df["Base Station Name"]
    df.drop(columns=["Downlink bandwidth", "Cell Name", "Base Station Name"], inplace=True)
    myclient = pymongo.MongoClient(MONGO_CLIENT_URL)
    mydb = myclient[EP_DB_NAME]
    mycol = mydb[LTE_EP_COL_NAME]
    mycol.create_index([("eNodeB Name", pymongo.ASCENDING)], background=True)
    data = df.to_dict(orient='records')  # Here's our added param..
    bulk_operations = []
    for d in data:
        bulk_operations.append(pymongo.UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
    mycol.bulk_write(bulk_operations, ordered=False)
    myclient.close()


def update_cluster_definition(fn="offcial_cluster.xlsx"):
    df = pd.read_excel(fn, dtype={"Site": str, "Cluster": str})
    df["_id"] = df["Site"]
    data = df.to_dict(orient='records')  # Here's our added param..
    myclient = pymongo.MongoClient(MONGO_CLIENT_URL)
    mydb = myclient[EP_DB_NAME]
    mycol = mydb[CLUTSER_COL_NAME]
    bulk_operations = []
    mycol.create_index([("Cluster", pymongo.ASCENDING)], background=True)
    for d in data:
        bulk_operations.append(pymongo.UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
    mycol.bulk_write(bulk_operations, ordered=False)
    myclient.close()


def update_isdp_report(fn="ISDP report.xlsx"):
    df = pd.read_excel(fn, na_values=["N/A"], dtype={"Customer Site ID": str, "DU ID": str, "Customer Site Name": str})
    df.drop(index=0, inplace=True)
    df.dropna(subset=['Customer Site ID'], inplace=True)
    for col in ["800 On Air", "E2E-MS12A: 4G SITE COMMERCIALISED (BIS)", "LTE TTO PM Status Date",
                "Antenna Works Completed", "E2E-MS7: BUILD STARTED (BLDST)", "NR BIS"]:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].astype(object).where(df[col].notnull(), None)
    df.rename(columns={"Customer Site ID": "Site ID", "Customer Site Name": "Site Name"}, inplace=True)
    df["_id"] = df["DU ID"]
    print(df.dtypes)
    data = df.to_dict(orient='records')  # Here's our added param..
    myclient = pymongo.MongoClient(MONGO_CLIENT_URL)
    mydb = myclient[EP_DB_NAME]
    mycol = mydb[ISDP_COL_NAME]
    bulk_operations = []
    mycol.create_index([("Site ID", pymongo.ASCENDING)], background=True)

    for d in data:
        bulk_operations.append(pymongo.UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
    mycol.bulk_write(bulk_operations, ordered=False)
    myclient.close()


if __name__ == "__main__":
    # update_ep()
    # update_cluster_definition()
    update_isdp_report()
    # nr_db_operator = operate_database.Db_operator(auto_complete_fields=[("Cell Name", "NR_CELL_NAMES"),
    #                                                                     ("gNodeB Name", "NR_GNODEB_NAMES")],
    #                                               tech="5G",
    #                                               MONGO_CLIENT_URL=MONGO_CLIENT_URL,
    #                                               DB_NAME=NR_DB_NAME,
    #                                               )
    # lte_db_operator = operate_database.Db_operator(auto_complete_fields=[("Cell Name", "LTE_CELL_NAMES"),
    #                                                                      ("eNodeB Name", "LTE_ENODEB_NAMES")],
    #                                                tech="4G",
    #                                                MONGO_CLIENT_URL=MONGO_CLIENT_URL,
    #                                                DB_NAME=LTE_DB_NAME,
    #                                                )
    # gsm_db_operator = operate_database.Db_operator(auto_complete_fields=[("Cell Name", "GSM_CELL_NAMES"),
    #                                                                      ("Site Name", "GSM_SITE_NAMES")],
    #                                                tech="2G",
    #                                                MONGO_CLIENT_URL=MONGO_CLIENT_URL,
    #                                                DB_NAME=GSM_DB_NAME,
    #                                                )
    # if os.path.exists(os.path.join("extract")):
    #     shutil.rmtree(os.path.join("extract"))
    # else:
    #     os.makedirs(os.path.join("extract"))
    # print("start:", datetime.datetime.now())
    # update_data()
    # print("finish:", datetime.datetime.now())
