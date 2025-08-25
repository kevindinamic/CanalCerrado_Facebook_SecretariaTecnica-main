#C:\Users\kevin\Documents\DINAMIC 2\secretaria_ecuador\CanalCerrado_Facebook_BancoGuayaquil-main\posts_bg.py
import asyncio
from apify_client import ApifyClient
import json
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv, find_dotenv

# Clear any existing environment variables
keys = ['apify_key']

for key in keys:
    if key in os.environ:
        del os.environ[key]

# Load dotenv()
try:
    load_dotenv(find_dotenv())
    print("Environment variables loaded successfully")
except Exception as e:
    print(f"An error occurred while loading the environment variables: {e}")

# Accesing Environment Variables
apify_key = os.getenv("apify_token")

print("Environment Variables Loaded from Functions:")
print(f"APIFY_API_KEY: {apify_key}")

# Initialize the ApifyClient with your API token
client = ApifyClient(apify_key)

def save_data_to_json(data, file_name):
    if not os.path.exists(file_name):
        # If the file does not exist, create a new one and write the data as a list
        with open(file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the file exists, load the existing data
        with open(file_name, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
            
            # Append the new data to the list
            existing_data.append(data)
            
            # Set the file cursor to the beginning, truncate the file, and save the updated list
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()

    print(f"Data saved to {file_name}")

# Function to load existing data and create sets for existing post_ids and post_dates           #-----------New function--------------#
def load_existing_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                post_dates = set()  # Changed to a set
                existing_data = json.load(file)
                post_ids = {post.get('id') for post in existing_data}
                post_dates_raw = {post.get('created_at') for post in existing_data}
                for guayaquil_time_str in post_dates_raw:
                    guayaquil_time = datetime.strptime(guayaquil_time_str, "%Y-%m-%d %H:%M:%S")
                    guayaquil_time_adjusted = guayaquil_time - timedelta(days=1)
                    date_str = guayaquil_time_adjusted.strftime("%Y-%m-%d")
                    post_dates.add(date_str)  # Correct usage of .add() with a set
                return post_ids, post_dates
        except json.JSONDecodeError:
            return set(), set()
    else:
        return set(), set()

#Revisar si la amyoría de sus entradas son null
def is_valid_object(obj):
    # Lista de las claves que deben ser no nulas
    keys_to_check = ["url", "id", "caption", "commentsCount"]
    
    # Verificar si todos los campos clave no son null y que created_at no sea "Unknown time"
    all_fields_valid = all(obj[key] is not None for key in keys_to_check)
    valid_created_at = obj.get("created_at") != "Unknown time"
    
    # Devolver True solo si todos los campos son válidos y created_at no es "Unknown time"
    return all_fields_valid and valid_created_at

# Initializes sets with previously calculated values ​​or with an empty set                     ------------New------------------
seen_posts, dates = load_existing_data('facebook_posts.json')

seconds_for_next_run = 20000 # 8 hours

NewerThan = max(dates) if dates else "2025-07-01"  # Define the date from which to retrieve posts

#Cont to determine if we have to retrieve posts or reels
cont = 0

async def fetch_facebook_posts():
    global NewerThan  # Declare NewerThan as global so we can modify it
    global cont
    print(f"NewerThan: {NewerThan}")


    # Define the maximum number of posts to retrieve
    results = 6
    # Prepare the Actor input
    run_input = {
    "addParentData": False,
    "startUrls": [
        {
            "url": "https://www.facebook.com/maria.d.munoz.969",
            "method": "GET",
        },
        {
            "url": "https://www.facebook.com/InfanciaEc",
            "method": "GET",
        }
    ],
    "onlyPostsNewerThan": NewerThan,
    "resultsLimit": results
}

    try:
        # Run the Actor and wait for it to finish
        run = client.actor("KoJrdxJCTtpon81KY").call(run_input=run_input)

        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            print(f"Processing item: {item}")
            post_id = item.get('postId', None)

            # Check if the post has already been seen
            if post_id not in seen_posts:
               
                facebook_post = {}

                created_at = item.get('timestamp', None)

                if isinstance(created_at, (int, float)):  # Verifica que sea timestamp numérico
                    utc_time = datetime.utcfromtimestamp(created_at)

                # created_at = item.get('timestamp', None)

                # if created_at:
                #     # Parse the timestamp as a datetime object in UTC
                #     utc_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")

                    # Define the timezone for Guayaquil, Ecuador (which is UTC-5)
                    guayaquil_tz = pytz.timezone('America/Guayaquil')

                    # Convert the UTC time to Guayaquil time
                    guayaquil_time = utc_time.replace(tzinfo=pytz.utc).astimezone(guayaquil_tz)

                    # Subtract one day from the date
                    guayaquil_time_adjusted = guayaquil_time - timedelta(days=1)

                    # Convert to the desired format yyyy-mm-dd and add to the dates set
                    date_str = guayaquil_time_adjusted.strftime("%Y-%m-%d")
                    print(f"Post date: {date_str}")
                    dates.add(date_str)

                    guayaquil_time_str = guayaquil_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    guayaquil_time_str = "Unknown time"

                post_url = item.get('url', None)
                post_text = item.get('text', None)
                post_commentsCount = item.get('comments', None)

                facebook_post['url'] = post_url
                facebook_post['id'] = post_id
                facebook_post['caption'] = post_text
                facebook_post['commentsCount'] = post_commentsCount
                facebook_post['created_at'] = guayaquil_time_str

                #Verificamos que sea valido
                if is_valid_object(facebook_post):
                    # Add the post ID to the seen_posts set
                    seen_posts.add(post_id)
                
                    print(f"Post {[post_text]} extracted")
                    print(seen_posts)

                    save_data_to_json(facebook_post, 'facebook_posts.json')
                else:
                    print("Invalid post")

            else:
                print(f"Post {post_id} already extracted sometime ago" if post_id else "Post ID not found in the dataset")

        # Find the most recent date in the dates set
        if dates:
            NewerThan = max(dates)
            print(f"NewerThan updated to: {NewerThan}")


        #updating the count variable after the extraction 
        cont += 1

    except Exception as e:
        print(f"An error occurred while fetching Facebook posts: {e}")

async def main():
    while True:
        try:
            await fetch_facebook_posts()
            print("Waiting for 8 hours before the next execution... \n\n\n")
            await asyncio.sleep(seconds_for_next_run)  # Sleep for 8 hours (28800 seconds)
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            await asyncio.sleep(seconds_for_next_run)  # Wait before trying again to avoid rapid failure loop

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred while running the main function: {e}")
