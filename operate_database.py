import pandas as pd
import pymongo
import os
import collections
import datetime
import io


def foo(row):
    print(type(row["Time"]))


def re_create_collection(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME, unique_index=[], single_indice=[]):
    # Create DB, drop DB if exists
    myclient = pymongo.MongoClient(MONGO_CLIENT_URL)
    dblist = myclient.list_database_names()
    # if DB_NAME in dblist:
    #     myclient.drop_database(DB_NAME)
    #     print(("Database {} is dropped".format(DB_NAME)))
    # Create Collections
    if DB_NAME in dblist:
        mydb = myclient[DB_NAME]
        print(("Database {} exits".format(DB_NAME)))
    else:
        mydb = myclient[DB_NAME]
        print(("Database {} is created".format(DB_NAME)))

    if COLLECTIONS_NAME in mydb.list_collection_names():
        mycol = mydb[COLLECTIONS_NAME]
        mycol.delete_many({})
        print("Collecion {} existed and is truncated".format(COLLECTIONS_NAME))
    else:
        mycol = mydb[COLLECTIONS_NAME]
        print("Collecion {} is created".format(COLLECTIONS_NAME))

    # Create index
    if unique_index:
        temp = [(index, pymongo.ASCENDING) for index in unique_index]
        mycol.create_index(temp, unique=True, background=True)

    for index in single_indice:
        mycol.create_index(index, background=True)

    print("Index is created")
    myclient.close()


def upsert_data(MONGO_CLIENT_URL, DB_NAME, COLLECTIONS_NAME, filename, skiprows, unique_index,
                parse_dates, auto_complete_fields=[], na_values=["NIL"]):
    myclient = pymongo.MongoClient(MONGO_CLIENT_URL)
    mydb = myclient[DB_NAME]
    mycol = mydb[COLLECTIONS_NAME]
    file_extention = os.path.splitext(filename)[1]
    if file_extention == ".csv":
        # skip Total XXX Records
        import io
        temp = io.StringIO()
        with open(filename, "rb") as f:
            for line in f.readlines():
                line = line.decode("utf-8")
                if not line.startswith("Total"):
                    temp.write(line)
        temp.seek(0)
        df = pd.read_csv(temp, parse_dates=parse_dates, skiprows=skiprows, na_values=na_values)
    elif file_extention == ".xlsx":
        df = pd.read_excel(filename, parse_dates=parse_dates, na_values=na_values)
    df["_id"] = df.apply(lambda row: "".join(
        [row[field].strftime("%Y%m%d%H%M%S") if field in parse_dates else row[field] for field in unique_index]),
                         axis=1)

    df.columns = [x.replace(".", "_") for x in df.columns]
    data = df.to_dict(orient='records')  # Here's our added param..

    auto_complete_field_to_add_sets = collections.defaultdict(set)
    bulk_operations = []
    for row in data:
        for auto_complete_field, auto_complete_collection in auto_complete_fields:
            if mydb[auto_complete_collection].find_one({"_id": row[auto_complete_field]}) == None:
                auto_complete_field_to_add_sets[auto_complete_collection].add(row[auto_complete_field])
        filter = {"_id": row["_id"]}
        bulk_operations.append(pymongo.UpdateOne(filter, {"$set": row}, upsert=True))
    mycol.bulk_write(bulk_operations, ordered=False)

    # upsert cell name/ gnodeb name /enodeb name etc....
    for auto_complete_collection in auto_complete_field_to_add_sets:
        d = [{"_id": v} for v in auto_complete_field_to_add_sets[auto_complete_collection]]
        try:
            mydb[auto_complete_collection].insert_many(d, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            print(e.details)

    print("upsert {} rows".format(len(data)))

    myclient.close()
