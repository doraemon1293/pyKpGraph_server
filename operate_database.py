import pandas as pd
import pymongo
import os
import collections
import time
import re
import io


class Db_operator():
    def __init__(self, auto_complete_fields, tech, MONGO_CLIENT_URL, DB_NAME):
        self.MONGO_CLIENT_URL = MONGO_CLIENT_URL
        self.DB_NAME = DB_NAME
        self.auto_complete_fields = auto_complete_fields
        self.auto_complete_existed_sets = {}
        self.tech = tech
        self.load_auto_complete_set()
        self.to_add_auto_complete_sets = collections.defaultdict(set)

    def load_auto_complete_set(self):
        myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
        mydb = myclient[self.DB_NAME]
        for auto_complete_field, auto_complete_collection in self.auto_complete_fields:
            self.auto_complete_existed_sets[auto_complete_collection] = set(
                [doc["_id"] for doc in mydb[auto_complete_collection].find()])
        myclient.close()

    def insert_to_add_auto_complete_set(self):

        # upsert cell name/ gnodeb name /enodeb name etc....
        myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
        mydb = myclient[self.DB_NAME]
        for auto_complete_collection in self.to_add_auto_complete_sets:
            d = [{"_id": v} for v in self.to_add_auto_complete_sets[auto_complete_collection]]
            try:
                if d:
                    mydb[auto_complete_collection].insert_many(d, ordered=False)
            except pymongo.errors.BulkWriteError as e:
                print(e.details)
        myclient.close()
        self.to_add_auto_complete_sets = collections.defaultdict(set)

    # def write_bulk_op(self,mydb, bulk_operations_dict, unique_index, indice):
    #     for collection_name, bulk_operations in bulk_operations_dict.items():
    #         # Create collection if not exist
    #         if collection_name not in mydb.list_collection_names():
    #             mycol = mydb[collection_name]
    #             # Create index
    #             if unique_index:
    #                 index_ = [(field, pymongo.ASCENDING) for field in unique_index]
    #                 mycol.create_index(index_, unique=True, background=True)
    #             # Create compound index
    #             if indice:
    #                 for index in indice:
    #                     index_ = [(field, pymongo.ASCENDING) for field in index]
    #                     mycol.create_index(index_, background=True)
    #         mycol = mydb.get_collection(collection_name,write_concern=pymongo.WriteConcern(w=0))
    #         res = mycol.bulk_write(bulk_operations, ordered=False)
    #         print(collection_name, len(bulk_operations))
    #         print("nUpserted ", res.bulk_api_result["nUpserted"])

    def create_collection(self, collection_name, indice):
        myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
        mydb = myclient[self.DB_NAME]
        mycol = mydb[collection_name]
        for index in indice:
            index_ = [(field, pymongo.ASCENDING) for field in index]
            mycol.create_index(index_, background=True)
        myclient.close()

    def upsert_data(self, zipref, collection_name_prefix, filename, skiprows,
                    parse_dates, indice=[], na_values=["NIL", "/0"], drop_columns=["eNodeB Function Name"],
                    rename_columns={"Local cell name": "Cell Name"}):
        # skip if file read already
        myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
        mydb = myclient[self.DB_NAME]
        mycol = mydb["read_files"]
        if mycol.find_one({"_id": filename}) != None:
            print("{} has been loaded already".format(filename))
            myclient.close()
            return
        myclient.close()

        # read df
        st = time.time()
        file_extention = os.path.splitext(filename)[1]

        if file_extention == ".csv":
            # skip Total XXX Records
            temp = io.StringIO()
            with zipref.open(filename) as f:
                for line in f.readlines():
                    line = line.decode("utf-8")
                    if not line.startswith("Total"):
                        temp.write(line)
            temp.seek(0)
            df = pd.read_csv(temp, parse_dates=parse_dates, skiprows=skiprows, na_values=na_values)
        elif file_extention == ".xlsx":
            zipref.extract(filename, "extract")
            df = pd.read_excel(os.path.join("extract", filename), parse_dates=parse_dates, na_values=na_values)
        # if Date and Time are split into 2 columsn, combine them
        if "Date" in df.columns and "Time" in df.columns:
            df["Date"] = df.apply(lambda row: row["Date"].replace(" DST", ""), axis=1)
            df["Date"] = pd.to_datetime(df["Date"])
            df["Time"] = pd.to_timedelta(df["Time"])
            df["Time"] = df["Time"] - pd.to_timedelta(df["Time"].dt.days, unit='d')
            df["Time"] = df["Date"] + df["Time"]

        # rename columns
        df = df.rename(columns=rename_columns)

        # convert Cell name / Site name to string
        for col in ["Cell Name", "Site Name", "eNodeB Name", "gNodeB Nam"]:
            if col in df.columns:
                df = df.astype({col: str})
        # remove () in kpi name for 4g/5g
        if self.tech == "4G" or self.tech == "5G":
            p = re.compile("\(.+\)")
            columns = [p.sub("", x) for x in df.columns]
        # remove suffix after : in kpi name for 2g
        if self.tech == "2G":
            columns = [x.split(":")[0] for x in df.columns]
        df.columns = columns
        # drop columns
        df.drop(columns=[col for col in drop_columns if col in df.columns], inplace=True)

        print("load", time.time() - st)

        # # run agg
        # st = time.time()
        # if agg_function:
        #     agg = {}
        #     for field in agg_function:
        #         if field in df.columns:
        #             agg[field] = agg_function[field]
        #     df = df.groupby(parse_dates + unique_index, as_index=False).agg(agg)
        # print("agg", time.time() - st)

        # deal with auto_complete
        st = time.time()
        for auto_complete_field, auto_complete_collection in self.auto_complete_fields:
            if auto_complete_field in df.columns:
                s = set(df[auto_complete_field].unique()) - self.auto_complete_existed_sets[auto_complete_collection]
                self.auto_complete_existed_sets[auto_complete_collection].update(s)
                self.to_add_auto_complete_sets[auto_complete_collection].update(s)
        print("deal with auto_complete", time.time() - st)

        # divide df by time and insert onebyone
        st = time.time()
        df.columns = [x.replace(".", "_") for x in df.columns]
        time_col = parse_dates[0]
        for dt in df[time_col].unique():
            t_df = df[df[time_col] == dt]
            dt = dt.astype('M8[ms]').astype('O')
            if time_col == "Time":
                collection_name = collection_name_prefix + dt.strftime("%Y%m%d%H")
            if time_col == "Date":
                collection_name = collection_name_prefix + dt.strftime("%Y%m%d")
            data = t_df.to_dict(orient='records')  # Here's our added param..
            print("trans df with {} rows in {}:".format(len(data), collection_name), time.time() - st)
            st = time.time()
            myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
            mydb = myclient[self.DB_NAME]
            if collection_name not in mydb.list_collection_names():
                self.create_collection(collection_name, indice)
            mycol = mydb.get_collection(collection_name, write_concern=pymongo.WriteConcern(w=0))
            mycol.insert_many(data, ordered=False)
            myclient.close()
            print("Insert {} rows in {}".format(len(data), collection_name), time.time() - st)

        # insert filename in to read_files
        myclient = pymongo.MongoClient(self.MONGO_CLIENT_URL)
        mydb = myclient[self.DB_NAME]
        mycol = mydb["read_files"]
        mycol.insert_one({"_id": filename})
        myclient.close()
        print("{} inserted".format(filename))
        self.insert_to_add_auto_complete_set()
