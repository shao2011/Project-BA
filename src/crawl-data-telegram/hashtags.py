from pymongo import MongoClient
from collections import defaultdict

# Kết nối tới MongoDB nguồn
MONGO_SOURCE_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017"
source_client = MongoClient(MONGO_SOURCE_URI)
source_db = source_client["cdp_database"]
tweets_collection = source_db["tweets"]
projects_collection = source_db["projects_social_media"]

MONGO_LOCAL_URI = "mongodb://localhost:27017"
local_client = MongoClient(MONGO_LOCAL_URI)
custom_local_db = local_client["hashtag"]  

def aggregate_hashtags_to_local():
    """
    Tổng hợp hashtags từ các bài tweet của từng project và lưu vào MongoDB local.
    """
    projects = projects_collection.find({}, {"projectId": 1, "twitter.id": 1})
    local_project_data = []

    for project in projects:
        project_id = project["projectId"]
        twitter_id = project.get("twitter", {}).get("id")

        if not twitter_id:
            print(f"Project '{project_id}' không có Twitter ID. Bỏ qua.")
            continue

        print(f"Đang xử lý project: {project_id} (Twitter ID: {twitter_id})")
        tweets = tweets_collection.find({"authorName": twitter_id}, {"hashTags": 1})

        hashtags = set()  
        for tweet in tweets:
            if "hashTags" in tweet:
                hashtags.update(tweet["hashTags"])

        hashtags_list = list(hashtags)

        project_data = {
            "project_id": project_id,
            "twitter_id": twitter_id,
            "all_hashtags": hashtags_list
        }
        local_project_data.append(project_data)
        print(f"Lưu {len(hashtags_list)} hashtag cho project '{project_id}'.")

    if local_project_data:
        custom_local_db["project_hashtags"].insert_many(local_project_data)
        print(f"Lưu {len(local_project_data)} project vào database mới 'custom_database'.")

aggregate_hashtags_to_local()
