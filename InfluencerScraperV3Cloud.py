import streamlit as st
#from apify_client import ApifyClient  # Keep this import if you need to use the client functions
import openai
from apify_client import ApifyClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import numpy as np
import json

# ---------------------------------------------------
# 1. CONFIGURATIONS & SETUP
# ---------------------------------------------------

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Set up your OpenAI API key from secrets (if needed elsewhere)
openai.api_key = st.secrets["openai"]["api_key"]

# Apify token from secrets
APIFY_API_TOKEN = st.secrets["apify"]["api_token"]

# Google Sheets Setup
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
service_account_info = json.loads(st.secrets["google"]["service_account"])
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, SCOPE)
gc = gspread.authorize(CREDS)

# Replace with your actual spreadsheet name or key
SPREADSHEET_NAME = "InfluencerGiftingSourcing"

# Try opening the sheet
try:
    sh = gc.open(SPREADSHEET_NAME)
    logging.info(f"Successfully opened Google Sheet: {SPREADSHEET_NAME}")
except Exception as e:
    logging.error(f"Error opening Google Sheet: {e}")
    raise e

# Main worksheet for influencer data
try:
    main_worksheet = sh.worksheet("Main")
    logging.info("Main worksheet found.")
except Exception:
    main_worksheet = sh.add_worksheet(title="Main", rows=1000, cols=20)
    header = [
        "Profile Pic URL", "Username", "Posts Count", "Followers Count", 
        "Biography", "Instagram Profile Link",
        "Median Comments (last 5)", "Median Likes (last 5)", "Engagement Rate"
    ]
    main_worksheet.insert_row(header, 1)
    logging.info("Main worksheet created with header row.")

# Second worksheet for hashtags
try:
    hashtag_worksheet = sh.worksheet("Hashtags")
    logging.info("Hashtags worksheet found.")
except Exception:
    hashtag_worksheet = sh.add_worksheet(title="Hashtags", rows=1000, cols=10)
    hashtag_header = ["Timestamp", "Hashtags Entered", "Used Hashtags"]
    hashtag_worksheet.insert_row(hashtag_header, 1)
    logging.info("Hashtags worksheet created with header row.")

# ---------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------

def append_hashtags_to_sheet(input_str: str, hashtags: list):
    """
    Store the entered hashtags in the 'Hashtags' worksheet.
    """
    from datetime import datetime
    try:
        hashtags_str = ", ".join(hashtags)
        row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_str, hashtags_str]
        hashtag_worksheet.append_row(row)
        logging.info("Hashtags appended to 'Hashtags' worksheet.")
    except Exception as e:
        logging.error(f"Error appending hashtags to sheet: {e}")

def fetch_owner_usernames_from_hashtags(hashtags: list, results_limit: int) -> set:
    """
    Call the Apify Instagram Hashtag Scraper for each hashtag,
    gather owner usernames from posts, and return a set of unique usernames.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    unique_usernames = set()
    for htag in hashtags:
        logging.info(f"Scraping hashtag: {htag}")
        try:
            run_input = {
                "hashtags": [htag],
                "resultsType": "posts",
                "resultsLimit": results_limit
            }
            run = client.actor("reGe1ST3OBgYZSsZJ").call(run_input=run_input)
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                if "ownerUsername" in item:
                    unique_usernames.add(item["ownerUsername"])
        except Exception as e:
            logging.error(f"Error scraping hashtag {htag}: {e}")
    logging.info(f"Total unique usernames found: {len(unique_usernames)}")
    return unique_usernames

def user_already_in_sheet(username: str) -> bool:
    """
    Checks if a username is already present in the main worksheet.
    """
    try:
        usernames_col = main_worksheet.col_values(2)  # Username is in the second column.
        return username in usernames_col
    except Exception as e:
        logging.error(f"Error checking existing usernames in sheet: {e}")
        return False

def scrape_profile_info(username: str):
    """
    Scrape Instagram profile info using Apify and return the profile data as a dictionary.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    try:
        run_input = {"usernames": [username]}
        run = client.actor("dSCLg0C3YEZ83HzYX").call(run_input=run_input)
        data_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not data_items:
            logging.warning(f"No profile data returned for {username}")
            return None
        profile_data = data_items[0]
        return {
            "username": username,
            "profile_pic_url": profile_data.get("profilePicUrl", ""),
            "posts_count": profile_data.get("postsCount", 0),
            "followers_count": profile_data.get("followersCount", 0),
            "biography": profile_data.get("biography", "")
        }
    except Exception as e:
        logging.error(f"Error scraping profile info for {username}: {e}")
        return None

def append_profile_to_sheet(profile_data: dict, median_comments: int, median_likes: int, engagement_rate: float):
    """
    Append the qualifying profile data along with engagement metrics to the main worksheet.
    """
    row = [
        profile_data["profile_pic_url"],
        profile_data["username"],
        profile_data["posts_count"],
        profile_data["followers_count"],
        profile_data["biography"],
        f"https://www.instagram.com/{profile_data['username']}",
        str(median_comments),
        str(median_likes),
        f"{engagement_rate:.2f}"
    ]
    main_worksheet.append_row(row)
    logging.info(f"Stored profile data for {profile_data['username']}")

def get_last_5_posts_stats(username: str, limit: int = 30):
    """
    Use Apify to scrape the user's most recent posts.
    Return the median likes and median comments for the last 5 (or fewer) posts.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    try:
        run_input = {
            "username": [username],
            "resultsLimit": limit
        }
        run = client.actor("nH2AHrwxeTRJoN5hX").call(run_input=run_input)
        posts = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not posts:
            logging.warning(f"No posts found for {username}")
            return 0, 0
        posts.sort(key=lambda x: x.get("takenAtTimestamp", 0), reverse=True)
        recent_posts = posts[:8]
        likes_list = [p.get("likesCount", 0) for p in recent_posts]
        comments_list = [p.get("commentsCount", 0) for p in recent_posts]
        if not likes_list:
            return 0, 0
        median_likes = int(np.median(likes_list))
        median_comments = int(np.median(comments_list))
        return median_likes, median_comments
    except Exception as e:
        logging.error(f"Error scraping posts for {username}: {e}")
        return 0, 0

# ---------------------------------------------------
# 3. STREAMLIT APP
# ---------------------------------------------------
def main():
    st.title("Instagram IB Influencer Sourcing Automation")
    
    st.write(
        "Please enter comma-separated hashtags (e.g. #InternationalBaccalaureate, #IBExams, #IBDiploma):"
    )
    hashtags_input = st.text_input("Hashtags", "")
    
    results_limit = st.number_input("How many posts per hashtag to scrape?", min_value=1, max_value=1000, value=50)
    
    if st.button("Scrape Influencers"):
        if not hashtags_input.strip():
            st.error("Please enter at least one hashtag.")
            return
        
        # Parse the direct hashtags provided by the user
        hashtags = [tag.strip() for tag in hashtags_input.split(",") if tag.strip()]
        if not hashtags:
            st.error("No valid hashtags entered.")
            return
        
        st.success(f"Using {len(hashtags)} hashtags: {', '.join(hashtags)}")
        
        # Append the entered hashtags to the Hashtags worksheet
        append_hashtags_to_sheet(hashtags_input, hashtags)
        
        # Fetch unique owner usernames from the scraped hashtags
        unique_usernames = fetch_owner_usernames_from_hashtags(hashtags, results_limit)
        
        # Process each username: scrape profile info, calculate engagement, and append qualifying profiles to the sheet.
        for username in unique_usernames:
            if user_already_in_sheet(username):
                logging.info(f"Skipping {username}, already in sheet.")
                continue
            
            profile_data = scrape_profile_info(username)
            if profile_data is None:
                continue
            
            # Filtering criteria for the IB/ed-tech space: lower thresholds are applied.
            if profile_data["followers_count"] > 1000 and profile_data["posts_count"] > 5:
                median_likes, median_comments = get_last_5_posts_stats(username, limit=30)
                if profile_data["followers_count"] > 0:
                    engagement_rate = ((median_likes + median_comments) / profile_data["followers_count"]) * 100
                else:
                    engagement_rate = 0
                
                # Only include profiles with an engagement rate of at least 0.5%
                if engagement_rate < 0.5:
                    logging.info(f"Skipping {username} due to low engagement rate: {engagement_rate:.2f}%")
                    continue
                
                append_profile_to_sheet(profile_data, median_comments, median_likes, engagement_rate)
        
        st.success("Scraping and data append complete. Please check Google Sheets for results.")

if __name__ == "__main__":
    main()
