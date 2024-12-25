from telethon import TelegramClient
from pymongo import MongoClient
from collections import defaultdict


api_id = "..."
api_hash = "..."

# Kết nối tới MongoDB nguồn
MONGO_SOURCE_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017"
source_client = MongoClient(MONGO_SOURCE_URI)
source_db = source_client["cdp_database"]

# Kết nối tới MongoDB local
MONGO_LOCAL_URI = "mongodb://localhost:27017"
local_client = MongoClient(MONGO_LOCAL_URI)
local_db = local_client["hashtag"]  
telegram_db = local_client["telegram_data"] 


client = TelegramClient("session_name", api_id, api_hash)


def get_keywords_for_project(project_id):
    """
    Lấy danh sách keywords từ local database dựa vào project_id.
    """
    project = local_db["project_hashtags"].find_one({"project_id": project_id}, {"all_hashtags": 1})
    if project and "all_hashtags" in project:
        return project["all_hashtags"]
    return [] 


def extract_keywords(content, project_keywords):
    """
    Trích xuất keywords từ nội dung tin nhắn dựa trên danh sách keyword của project.
    """
    return [word for word in project_keywords if word.lower() in content.lower()]


def calculate_engagement(message):
    replies = message.replies.replies if message.replies else 0
    reactions = sum(reaction.count for reaction in message.reactions.results) if message.reactions else 0
    mentions = count_mentions(message.text or "")
    return replies + reactions + mentions


def analyze_sentiment(content):
    positive_words = ["good", "great", "excellent", "amazing", "positive"]
    negative_words = ["bad", "terrible", "horrible", "negative"]
    if any(word in content.lower() for word in positive_words):
        return "positive"
    elif any(word in content.lower() for word in negative_words):
        return "negative"
    else:
        return "neutral"


def count_mentions(content):
    return content.count("@") if content else 0


async def crawl_all_projects():
    """
    Pipeline crawl dữ liệu Telegram từ tất cả các project và lưu vào MongoDB local.
    """
    projects = source_db["projects_social_media"].find({}, {"projectId": 1, "telegram.id": 1})
    projects = list(projects)

    if not projects:
        print("Không tìm thấy project nào trong database nguồn.")
        return

    print(f"Đã tìm thấy {len(projects)} project để crawl dữ liệu.")

    for project in projects:
        project_id = project.get("projectId")
        telegram_id = project.get("telegram", {}).get("id")

        if not telegram_id:
            print(f"Project '{project_id}' không có thông tin Telegram.")
            continue

        print(f"Bắt đầu crawl dữ liệu cho project: {project_id} (Group ID: {telegram_id})")

        try:
            group = await client.get_entity(telegram_id)
            print(f"Truy cập thành công nhóm: {group.title if hasattr(group, 'title') else 'N/A'}")
            project_keywords = get_keywords_for_project(project_id)
            print(f"Keywords cho project '{project_id}': {project_keywords}")

            user_data = defaultdict(lambda: {
                "user_id": None,
                "username": None,
                "first_name": None,
                "last_name": None,
                "is_bot": False,
                "project_id": project_id,
                "total_messages": 0,
                "engagement_score": 0,
                "keywords_used": [],
                "most_active_times": {"hour": [], "day": []},
                "sentiment_summary": {"positive": 0, "negative": 0, "neutral": 0},
                "message_ids": []
            })
            messages_data = []

            async for message in client.iter_messages(group, limit=1500):
                sender = await message.get_sender()
                if sender:
                    user_id = sender.id
                    username = sender.username
                    first_name = sender.first_name if hasattr(sender, "first_name") else None
                    last_name = sender.last_name if hasattr(sender, "last_name") else None
                    is_bot = sender.bot if hasattr(sender, "bot") else False
                    content = message.text or ""

                    # Phân tích tin nhắn
                    keywords = extract_keywords(content, project_keywords)
                    sentiment = analyze_sentiment(content)
                    engagement_score = calculate_engagement(message)

                    messages_data.append({
                        "message_id": message.id,
                        "sender_id": user_id,
                        "content": content,
                        "timestamp": message.date,
                        "project_id": project_id,
                        "group_id": telegram_id,
                        "keywords": keywords,
                        "sentiment": sentiment,
                        "engagement_metrics": {
                            "replies": message.replies.replies if message.replies else 0,
                            "mentions": count_mentions(content),
                            "reactions": sum(reaction.count for reaction in message.reactions.results) if message.reactions else 0
                        }
                    })

                    if user_id not in user_data:
                        user_data[user_id].update({
                            "user_id": user_id,
                            "username": username,
                            "first_name": first_name,
                            "last_name": last_name,
                            "is_bot": is_bot,
                            "project_id": project_id
                        })
                    user_data[user_id]["total_messages"] += 1
                    user_data[user_id]["engagement_score"] += engagement_score
                    user_data[user_id]["keywords_used"].extend(keywords)
                    user_data[user_id]["sentiment_summary"][sentiment] += 1
                    user_data[user_id]["most_active_times"]["hour"].append(message.date.hour)
                    user_data[user_id]["most_active_times"]["day"].append(message.date.strftime("%A"))
                    user_data[user_id]["message_ids"].append(message.id)

            user_data_list = list(user_data.values())

            if messages_data:
                telegram_db["telegram_messages"].insert_many(messages_data)
                print(f"Lưu {len(messages_data)} tin nhắn từ nhóm {group.title}.")
            if user_data_list:
                telegram_db["telegram_users"].insert_many(user_data_list)
                print(f"Lưu {len(user_data_list)} người dùng từ nhóm {group.title}.")

        except Exception as e:
            print(f"Lỗi khi truy cập nhóm Telegram của project '{project_id}': {e}")


async def main():
    await crawl_all_projects()


with client:
    client.loop.run_until_complete(main())
