import requests
import time
from pymongo import MongoClient
from conf import MONGO_URI, DATABASE_NAME, COLLECTION_NAME

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Fetch all anime from MongoDB
anime_cursor = collection.find({})

for anime in anime_cursor:
    mal_id = anime.get("info", {}).get("mal_id")
    series_name = anime.get("info", {}).get("series")
    
    if mal_id == "not found" or not mal_id:
        print(f"Skipping {series_name} (MAL ID not found)")
        continue
    
    print(f"Processing {series_name} (MAL ID: {mal_id})")
    
    # Step 1: Get all episodes from Jikan
    page = 1
    episode_ids = []
    
    while True:
        jikan_url = f"https://api.jikan.moe/v4/anime/{mal_id}/episodes?page={page}"
        resp = requests.get(jikan_url)
        
        if resp.status_code != 200:
            print(f"Error fetching Jikan data for {series_name}")
            break
        
        data = resp.json()
        episodes = data.get("data", [])
        
        if not episodes:
            break
        
        for ep in episodes:
            episode_ids.append(ep["mal_id"])
        
        if not data["pagination"]["has_next_page"]:
            break
        
        page += 1
    
    if not episode_ids:
        print(f"No episodes found for {series_name}")
        continue
    
    print(f"Total episodes: {episode_ids[-1]}")
    
    # Step 2: Fetch AniSkip data for each episode
    skip_times_dict = {}
    for ep_id in episode_ids:
        aniskip_url = f"https://api.aniskip.com/v1/skip-times/{mal_id}/{ep_id}?types=op&types=ed"
        resp = requests.get(aniskip_url)
        
        skip_data = []
        if resp.status_code == 200:
            data = resp.json()
            if data.get("found") and data.get("results"):
                skip_data = [
                    {
                        "skip_type": item["skip_type"],
                        "start_time": item["interval"]["start_time"],
                        "end_time": item["interval"]["end_time"]
                    }
                    for item in data["results"]
                ]
        if skip_data:
            skip_times_dict[str(ep_id)] = skip_data
        
        time.sleep(0.2)  # avoid rate limits
    
    # Step 3: Update MongoDB document with skip_times outside episodes
    if skip_times_dict:
        collection.update_one(
            {"_id": anime["_id"]},
            {"$set": {"skip_times": skip_times_dict}}
        )
    
    print(f"Saved skip_times for {series_name}")
