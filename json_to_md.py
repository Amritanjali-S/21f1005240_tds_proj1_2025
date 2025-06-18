import html2text
import time
from bs4 import BeautifulSoup
from PIL import Image
import google.generativeai as genai
import os
import requests
import json
from datetime import datetime, timezone
from urllib.parse import urljoin, urlencode
from google.genai import types

# ========== CONFIGURATION ==========
INPUT_JSON_DIR = "discourse_json"
OUTPUT_MD_DIR = "markdowns"
BASE_IMAGE_DIR = "discourse_json/img"
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
# ====================================

class RateLimiter:
    def __init__(self, requests_per_minute=60, requests_per_second=2):
        self.requests_per_minute = requests_per_minute
        self.requests_per_second = requests_per_second
        self.request_times = []
        self.last_request_time = 0
    
    def wait_if_needed(self):
        current_time = time.time()
        
        # Per-second rate limiting
        time_since_last = current_time - self.last_request_time
        if time_since_last < (1.0 / self.requests_per_second):
            sleep_time = (1.0 / self.requests_per_second) - time_since_last
            time.sleep(sleep_time)
        
        # Per-minute rate limiting
        current_time = time.time()
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        if len(self.request_times) >= self.requests_per_minute:
            sleep_time = 60 - (current_time - self.request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # Clean up old requests after sleeping
                current_time = time.time()
                self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        self.request_times.append(current_time)
        self.last_request_time = current_time

rate_limiter = RateLimiter(requests_per_minute=5, requests_per_second=2)

def get_image_description(image_bytes, max_retries: int = 3):
    # try:
    #     model = genai.GenerativeModel(model_name="gemini-2.0-flash")
    #     genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

    #     result = model.generate_content([
    #         {"mime_type": "image/jpeg", "data": image_bytes},
    #         "Caption this image."
    #     ])
    #     return result.text

    # except Exception as e:
    #     print(f"Failed to caption image: {e}")
    #     return None
    # try:
    #     genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))  # configure once at the top instead
    #     model = genai.GenerativeModel("gemini-2.0-flash")

    #     # Correct way to format the image as a Part
    #     image_part = {
    #         "mime_type": "image/jpeg",
    #         "data": image_bytes
    #     }

    #     result = model.generate_content([
    #         image_part,
    #         "Caption this image."
    #     ])
    #     return result.text
    # except Exception as e:
    #     print(f"Failed to caption image: {e}")
    #     return None
    
    # client = genai.Client()#api_key=os.getenv("GOOGLE_API_KEY"))


    # my_file = client.files.upload(file= image_path)

    # # with open(os.path.join('pdsaiitm.github.io', image_path), 'rb') as img_file:
    # #     image_data = img_file.read()


    # response = client.models.generate_content(
    #     model="gemini-2.0-flash",
    #     contents=[my_file, "Caption this image."],
    # )
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel(model_name="gemini-2.0-flash")
            genai.configure(api_key="AIzaSyAF6Tnbq5eDvT1mU67WtzhbZTanK9Ajstw")
            rate_limiter.wait_if_needed()
            #genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

            result = model.generate_content([
            {"mime_type": "image/jpeg", "data": image_bytes},
            "Describe this image." ])

            return result.text
        except Exception as e:
            print(f"Failed to caption image: {e}")
            if "rate limit" in str(e).lower() or "quota" in str(e).lower():

                # Exponential backoff for rate limit errors
                wait_time = 2 ** attempt
                print(f"Rate limit hit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
            elif attempt == max_retries - 1:
                print(f"Failed to get embedding after {max_retries} attempts: {e}")
                raise
            else:
                    print(f"Attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(1)
        
        raise Exception("Max retries exceeded")
        #return None

def get_image_description_from_url(image_url):
    try:
        response = requests.get(image_url)
        response.raise_for_status()
        image_bytes = response.content
        return get_image_description(image_bytes)
    except Exception as e:
        print(f"Failed to download or caption image from {image_url}: {e}")
        return None

def get_image_description_from_file(image_path):
    try:
        with open(image_path, "rb") as f:
            image_bytes = f.read()
        return get_image_description(image_bytes)
    except Exception as e:
        print(f"Failed to read or caption local image {image_path}: {e}")
        return None

def convert_json_to_md(json_path, base_image_dir, output_dir):
    with open(json_path, 'r', encoding='utf-8') as f:
        topic_data = json.load(f)

    posts = topic_data.get("post_stream", {}).get("posts", [])
    md_content = ""

    for post in posts:
        html = post.get("cooked", "")
        soup = BeautifulSoup(html, "html.parser")
        imgs = soup.find_all("img")
        md_body = html2text.html2text(html)

        for img in imgs:
            src = img.get("src")
            local_path = os.path.join(base_image_dir, src.lstrip("/"))
            if os.path.exists(local_path):
                caption = get_image_description_from_file(local_path)
            elif src.startswith("http"):
                caption = get_image_description_from_url(src)
            else:
                caption = None

            if caption:
                md_body += f"\n\n![{caption}]({src})\n"

        md_content += md_body + "\n\n---\n\n"

    output_file = os.path.join(output_dir, os.path.basename(json_path).replace(".json", ".md"))
    os.makedirs(output_dir, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(md_content)

def main():
    print("Converting JSON to Markdown...")
    os.makedirs(OUTPUT_MD_DIR, exist_ok=True)
    for root, dirs, files in os.walk(INPUT_JSON_DIR):
        for file in files:
            if file.endswith(".json"):
                json_path = os.path.join(root, file)
                convert_json_to_md(json_path, base_image_dir=BASE_IMAGE_DIR, output_dir=OUTPUT_MD_DIR)

if __name__ == "__main__":
    main()
