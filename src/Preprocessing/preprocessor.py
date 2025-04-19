import json
import pandas as pd
from datetime import datetime
import re
import os

def get_filename(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_json_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data


def clean_text(text):
    text = text.lower()  # Lowercase
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'[\n\r\t\f\v]', ' ', text)  # Remove escape characters (replace with space)
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def create_preprocessed_json_by_blocks_of_days(file_path):
    filename = get_filename(file_path)
    data = get_json_data(file_path)

    #dictionary to store the posts and comments by blocks of days
    posts_by_blocks_of_days = list()
    # posts_by_blocks_of_days = {i: [] for i in range(0, 11)}  

    current_day = -1
    block_list = []
    date_set = set()

    for post in data:

        # Skip if the post has no body
        body = post.get('body', '')
        if not body.strip():         
            continue
        
        # Spliting into date and time
        created_utc_str = post['created_utc']
        dt = datetime.fromisoformat(created_utc_str)
        date_part = dt.date().isoformat() 
        time_part = dt.time().isoformat()
        day_part = dt.day
        month_part = dt.month
        
        date_set.add(date_part)

        day_of_year = month_part * 31 + day_part
        if current_day == -1:
            current_day = day_of_year
        
        if day_of_year > current_day + 3:
            current_day = day_of_year
            posts_by_blocks_of_days.append(block_list)
            block_list = []
        print (f"Current day: {current_day}, Post day: {day_part}, Block list length: {len(block_list)}")
        post_data = {
            'post_id': post['post_id'],
            # 'author': post['author'],
            # 'url': post['url'],
            # 'created_utc': created_utc_str,
            'title': clean_text(post['title']),
            'date' : date_part,
            'time' : time_part,
            'score': post['score'],
            'num_comments': post['num_comments'],
            'flair': post['flair'],
            # 'body': clean_text(post.get('body', '')), 
            'combined_text': clean_text(post['title'] + ' ' + post.get('body', '')),
            'comments': []
        }

        for comment in post["comments"]:
            comment_data = {
                # 'post_id': post['post_id'],
                # 'comment_author': comment['author'],
                # 'comment_created_utc': comment['created_utc']
                'comment_body': clean_text(comment['body']),
                'comment_score': comment['score'],
                'date' : date_part,
                'time' : time_part
            }
            post_data['comments'].append(comment_data)
        
        block_list.append(post_data)

    # print(date_set, len(date_set))

    # Save the preprocessed data
    processed_dir = f'../../data/processed/{filename}_preprocessed'
    os.makedirs(processed_dir, exist_ok=True)
    with open(os.path.join(processed_dir, 'posts_by_blocks_of_days.json'), 'w') as f:
        json.dump(posts_by_blocks_of_days, f, indent=2)

