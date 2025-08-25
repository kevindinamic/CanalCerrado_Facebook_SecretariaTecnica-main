#C:\Users\kevin\Documents\DINAMIC 2\secretaria_ecuador\CanalCerrado_Facebook_BancoGuayaquil-main\comments_bg.py
import asyncio
from apify_client import ApifyClient
import json
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv, find_dotenv
import aiohttp
from openai import OpenAI
import pandas as pd

# Clear any existing environment variables
keys = ['apify_key', 'canal_cerrado_telegram_bot_token', 'canal_cerrado_telegram_chat_id','openai_key']

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
canal_cerrado_telegram_bot_token = os.getenv('canal_cerrado_telegram_bot_token')
canal_cerrado_telegram_chat_id = os.getenv('canal_cerrado_telegram_chat_id')
openai_key = os.getenv('openai_key')

print("Environment Variables Loaded from Functions:")
print(f"APIFY_API_KEY: {apify_key}")
print(f"canal_cerrado_telegram_bot_token: {canal_cerrado_telegram_bot_token}")
print(f"canal_cerrado_telegram_chat_id: {canal_cerrado_telegram_chat_id}")
print(f"openai_key: {openai_key}")


# Initialize the ApifyClient with your API token
client = ApifyClient(apify_key)

# Cliente de Open AI
client_openai = OpenAI(api_key=openai_key)


def clasificacion_texto(texto):
    completion = client_openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {
                "role": "system",
                "content": (
                    "Eres un sistema que clasifica comentarios dirigidos a la Secretaría Técnica "
                    "Ecuador Crece Sin Desnutrición Infantil. Debes responder únicamente con una "
                    "de las siguientes categorías: 'Positivo' (actitud positiva, emojis de apoyo, etc), "
                    "'Negativo' (descontento o crítica, emojis de molestia), 'Neutral' (comentario neutral) y 'Sin Contexto' (emojis o texto que no permite clasificar adecuadamente) "
                  
                )
            },
            {
                "role": "user",
                "content": (
                    f"En el contexto de comentarios dirigidos a la Secretaría Técnica Ecuador Crece "
                    f"Sin Desnutrición Infantil, clasifica el siguiente comentario como 'Positivo', "
                    f"'Negativo', 'Sin Contexto'  o 'Neutral'. Responde únicamente con una de estas tres: {texto}"
                )
            }
        ]
    )
    respuesta = completion.choices[0].message.content.strip()
    return respuesta

# Send telegram message to Canal Cerrado
async def send_telegram_message_async_canal_cerrado(post_comments, bot, chat_id):
    comment = post_comments.get('comment', '')
    if comment is None or not comment.strip():
        print("No se envía mensaje: comentario vacío o None")
        return

    sentimiento = post_comments.get('clasificacion', '')
    if sentimiento == "Positivo":
        emoji = "🟢"
    elif sentimiento == "Negativo":
        emoji = "🔴"
    elif sentimiento == "Neutral":
        emoji = "⚪"
    else:
        emoji = ""

    url = f"https://api.telegram.org/bot{bot}/sendMessage"
    message_text = (
        "🔵 Facebook \n\n"
        f"📅🕒 Fecha y Hora: {post_comments['created_at']}\n"
        f"👤 Nombre: {post_comments['user']}\n\n"
        f"📝 Comment: {comment}\n\n"
        f"🌍 Comment URL: {post_comments['commentUrl']}\n\n"
        f"   Sentimiento: {sentimiento} {emoji}\n\n"
    )

    data = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'HTML'
    }

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get("Retry-After", 31))
                        print(f"Too many requests. Retrying in {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                    elif response.status != 200:
                        response_data = await response.text()
                        print(f"Failed to send message: {response_data}")
                    else:
                        print("Message sent successfully to Canal Cerrado!")
                        break
        except aiohttp.ClientError as e:
            print(f"An error occurred: {e}")
            await asyncio.sleep(31)



def save_data_to_json(data, local_file_name, consolidated_file_name):
    # Save to local JSON file for each social media
    if not os.path.exists(local_file_name):
        # If the file does not exist, create a new one and write the data as a list
        with open(local_file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the file exists, load the existing data
        with open(local_file_name, 'r+') as file:
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

    print(f"Data saved to {local_file_name}")

    # Ensure the folder for the consolidated file exists
    consolidated_folder = os.path.dirname(consolidated_file_name)
    if not os.path.exists(consolidated_folder):
        os.makedirs(consolidated_folder)
        print(f"Created folder: {consolidated_folder}")

    # Save to consolidated JSON file
    if not os.path.exists(consolidated_file_name):
        # If the consolidated file does not exist, create it
        with open(consolidated_file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the consolidated file exists, load its data
        with open(consolidated_file_name, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

            # Append the new data to the list
            existing_data.append(data)

            # Write the updated data back to the file
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()

    print(f"Data also saved to {consolidated_file_name}\n")



# Function to load existing data and create a set for existing post_ids                       #-----------New function--------------#
def load_existing_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                post_ids = {post.get('comment_id') for post in existing_data}
                return post_ids
        except json.JSONDecodeError:
            return set()
    else:
        return set()

# Function to extract the last 3 URLs, adding recent ones if they exist, or just the most recent one if none are recent
def extract_recent_urls(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    urls = []
    current_date = datetime.now()
    three_weeks_ago = current_date - timedelta(weeks=3)
    
    # Extract URLs with their corresponding creation dates
    for post in data:
        created_at_str = post.get("created_at")
        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
        url = post.get("url")
        if url:
            urls.append((created_at, url))
    
    # Sort URLs by date, most recent first
    urls.sort(reverse=True, key=lambda x: x[0])
    
    # Extract the last 3 URLs
    last_three_urls = urls[:3]
    
    # Filter URLs that are within the last two weeks
    recent_urls = [url for date, url in last_three_urls if date >= three_weeks_ago]
    
    # If none are recent, just add the latest URL
    if not recent_urls:
        recent_urls = [last_three_urls[0][1]]
    else:
        recent_urls = [url for _, url in last_three_urls]
    
    return recent_urls

#Function to load or create a csv file
def load_existing_csv(filename):
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        df = pd.DataFrame(columns=['Red social', 'Comentario', 'Clasificación', 'URL'])
    return df


# Get today's date
today_date = datetime.now().date()

# Initializes a dataframe to save the information

# initializes a set with previously calculated values ​​or with an empty set                          #---------New-----------------#
seen_comments = load_existing_data('facebook_comments.json')
print(f"Loaded {len(seen_comments)} existing comments")

seconds_for_next_run =2000 # 1 hour

async def fetch_facebook_comments():
    try:
        # Define the maximum number of comments to retrieve
        results = 1000

        # Determine the URLs of the most recent facebook posts
        urls = extract_recent_urls('facebook_posts.json')
        print(f"Extracting comments from {len(urls)} recent facebook posts")

        if not urls:
            print("No recent URLs found or failed to extract URLs.")
            return

        # Build startUrls list
        start_urls = [{"url": url, "method": "GET"} for url in urls]

        run_input = {
            "includeNestedComments": True,
            "resultsLimit": results,
            "startUrls": start_urls,
            "viewOption": "RECENT_ACTIVITY"
        }

        run = client.actor("us5srxAYnsrkgUv2v").call(run_input=run_input)

        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            
            comment_id = item.get('id')

            if comment_id not in seen_comments and comment_id is not None:
                post_comments = {}

                created_at = item.get('date')
                if created_at:
                    utc_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                    guayaquil_tz = pytz.timezone('America/Guayaquil')
                    guayaquil_time = utc_time.replace(tzinfo=pytz.utc).astimezone(guayaquil_tz)
                    guayaquil_time_str = guayaquil_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    guayaquil_time_str = "Unknown time"

                comment = item.get('text')
                clasificacion = clasificacion_texto(comment)

                post_comments.update({
                    "postUrl": item.get('facebookUrl'),
                    "commentUrl": item.get('commentUrl'),
                    "comment_id": comment_id,
                    "comment": comment,
                    "user": item.get('profileName'),
                    "user_profile": item.get('profilePicture'),
                    "profileUrl": item.get('profileUrl'),
                    "created_at": guayaquil_time_str,
                    "likes_count": item.get('likesCount'),
                    "clasificacion": clasificacion,
                    "red_social": "facebook",
                    "postTitle": item.get('postTitle'),
                    "inputUrl": item.get('inputUrl'),
                    "facebookId": item.get('facebookId'),
                    "threadingDepth": item.get('threadingDepth'),
                })

                print(f"processing comment: {post_comments}")

                seen_comments.add(comment_id)
                print(f"Comment {[comment_id]} extracted")

                save_data_to_json(post_comments, 'facebook_comments.json', "../aggregated_data/all_comments.json")

                if clasificacion == "Sin Contexto":
                    print("No se envía el tweet 'Sin Contexto'")
                    continue

                await send_telegram_message_async_canal_cerrado(post_comments, canal_cerrado_telegram_bot_token, canal_cerrado_telegram_chat_id)
            else:
                print(f"Comment {comment_id} already extracted sometime ago")
    except Exception as e:
        print(f"An error occurred while fetching facebook comments: {e}")

async def main():
    while True:
        try:
            await fetch_facebook_comments()
            print("Waiting for 1 hour before the next execution... \n\n\n")
            await asyncio.sleep(seconds_for_next_run)  # Sleep for 1 hour (3600 seconds)
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            await asyncio.sleep(seconds_for_next_run)  # Wait before trying again to avoid rapid failure loop

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred while running the main function: {e}")
