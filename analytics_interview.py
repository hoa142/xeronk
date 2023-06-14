import glob
import gzip
import json
from datetime import datetime

from pymongo import MongoClient
from pymongo.operations import UpdateOne


def parse_and_insert(file_path: str, collection, batch_size: int = 1000) -> None:
    """
    Parse a file.gz then insert into a Mongo collection.

    :param file_path: e.g. `raw-bid-win/2017/01/11/00/59/HVuIhurmB5909SKcwGMX.gz`
    :param collection: Mongo collection
    :param batch_size: number of documents in a batch to insert into the collection
    :return: None
    """
    documents = []

    with gzip.open(file_path, "rt") as gz_file:
        for line in gz_file:
            try:
                data = json.loads(line)

                # Extraction
                auction_id = data.get("auctionId", "NA")
                campaign_id = data.get("biddingMainAccount", "NA")
                creative_id = data.get("bidResponseCreativeName", "NA")
                adgroup_id = data.get("biddingSubAccount", "NA")
                bid_request_string = json.loads(data.get("bidRequestString", "{}"))

                user_agent = bid_request_string.get("userAgent", "Others")
                site = bid_request_string.get("url", "Others")
                geo = (
                    bid_request_string.get("device", {})
                    .get("geo", {})
                    .get("country", "Others")
                )
                if geo == "Others":
                    geo = (
                        bid_request_string.get("device", {})
                        .get("ext", {})
                        .get("geo_criteria_id", "Others")
                    )
                exchange = bid_request_string.get("exchange", "Others")
                price = float(data.get("winPrice", "0").replace("USD/1M", ""))
                time = bid_request_string.get("timestamp")
                time = (
                    int(datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
                    if time
                    else 0
                )

                # Document to insert
                document = {
                    "auctionId": auction_id,
                    "campaignId": campaign_id,
                    "creativeId": creative_id,
                    "adgroupId": adgroup_id,
                    "userAgent": user_agent,
                    "site": site,
                    "geo": geo,
                    "exchange": exchange,
                    "price": price,
                    "time": time,
                }

                documents.append(document)
                if len(documents) >= batch_size:
                    collection.insert_many(documents)
                    documents = []

            except json.JSONDecodeError:
                continue


def aggregate_and_upsert(source_collection, target_collection) -> None:
    """
    Aggregate data in a source collection then upsert into a target collection.

    :param source_collection: e.g. `individualWins`
    :param target_collection: e.g. `geoAggregation`
    :return: None
    """
    group_key = {
        "campaignId": "$campaignId",
        "creativeId": "$creativeId",
        "adgroupId": "$adgroupId",
        "geo": "$geo",
        "time": "$time",
    }

    pipeline = [
        {
            "$group": {
                "_id": group_key,
                "totalPrice": {"$sum": "$price"},
                "minPrice": {"$min": "$price"},
                "maxPrice": {"$max": "$price"},
                "totalCount": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "campaignId": "$_id.campaignId",
                "creativeId": "$_id.creativeId",
                "adgroupId": "$_id.adgroupId",
                "geo": "$_id.geo",
                "time": "$_id.time",
                "totalPrice": 1,
                "minPrice": 1,
                "maxPrice": 1,
                "totalCount": 1,
            }
        },
    ]

    result = source_collection.aggregate(pipeline)
    bulk_operations = []

    for doc in result:
        filter_query = {
            "campaignId": doc["campaignId"],
            "creativeId": doc["creativeId"],
            "adgroupId": doc["adgroupId"],
            "geo": doc["geo"],
            "time": doc["time"],
        }

        update_query = {
            "$set": {
                "totalPrice": doc["totalPrice"],
                "minPrice": doc["minPrice"],
                "maxPrice": doc["maxPrice"],
                "totalCount": doc["totalCount"],
            }
        }

        bulk_operations.append(UpdateOne(filter_query, update_query, upsert=True))

    if bulk_operations:
        target_collection.bulk_write(bulk_operations)


def dump_to_json(file_path: str, collection) -> None:
    """
    Dump out the data in JSON format from a collection.

    :param file_path: e.g. `dump_file.json`
    :param collection: e.g. `geoAggregation`
    :return: None
    """
    data = list(collection.find())

    with open(file_path, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    client = MongoClient()

    db = client.analyticsInterview
    individual_wins_collection = db.individualWins
    geo_aggregation_collection = db.geoAggregation

    file_paths = glob.glob("raw-bid-win/**/*.gz", recursive=True)
    for file_path in file_paths:
        parse_and_insert(file_path, individual_wins_collection)

    aggregate_and_upsert(individual_wins_collection, geo_aggregation_collection)
    dump_to_json("result_dump.json", geo_aggregation_collection)

    client.close()
