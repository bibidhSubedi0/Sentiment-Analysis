import os
import re

import praw
import json
import logging
from datetime import datetime, timedelta
import sys
from collections import defaultdict

logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for verbose logging
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('reddit_scraper.log')
    ]
)

class RedditScraper:
    def __init__(self, client_id, client_secret, user_agent, get_comments = False):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False
        )
        self.get_comments = get_comments
        self.flair_list = []
        logging.info("RedditScraper initialized successfully")

    def collect_posts(self, subreddit_name: str, count: int, processed: bool = False):
        collected_data = defaultdict(lambda: {"posts": []})
        seen_posts = set()

        logging.info(f"Starting to collect posts from r/{subreddit_name}")

        for sort_method in ['hot', 'new', 'top', 'rising', 'controversial']:
            try:
                logging.info(f"Fetching {count} posts with sort method: {sort_method}")
                posts = self._fetch_batch(subreddit_name, sort_method, count, processed)

                for post in posts:
                    if post['post_id'] not in seen_posts:
                        seen_posts.add(post['post_id'])
                        month_key = datetime.fromisoformat(post['created_utc']).strftime('%Y-%m')
                        collected_data[month_key]['posts'].append(post)

            except Exception as e:
                logging.error(f"Failed {sort_method}: {str(e)}")
        output = self._format_output(collected_data, processed)
        filename = self.save_data(output, subreddit_name, processed)
        return output , filename



    def _fetch_batch(self, subreddit_name: str, sort_method: str, limit: int, processed: bool = False):
        subreddit = self.reddit.subreddit(subreddit_name)
        method = getattr(subreddit, sort_method, None)
        if method is None:
            logging.error(f"Invalid sort method: {sort_method}")
            return []
        try:
            posts = list(method(limit=limit))
        except Exception as e:
            logging.error(f"Error fetching posts: {str(e)}")
            return []
        logging.debug(f"Fetched {len(posts)} posts from r/{subreddit_name}")
        return [self.__process_post(p) for p in posts] if processed else [self._transform_post(p) for p in posts]


    def __process_post(self, post):
        logging.debug(f"Processing post {post.id}")
        return {
            'post_id': post.id,
            'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),

            'title': post.title,
            'score': post.score,
            'body': post.selftext,
            'num_comments': post.num_comments,
            'flair': post.link_flair_text,
            'nsfw': post.over_18,
            'awards': post.total_awards_received,
            'created_hour': datetime.fromtimestamp(post.created_utc).hour,
            'created_dayofweek': datetime.fromtimestamp(post.created_utc).weekday(),
            'combined_text': f"{post.title} {post.selftext}",
            'cleaned_text': re.sub(r'\W+', ' ', re.sub(r'http\S+', '', f"{post.title} {post.selftext}")),
            'flair_encoded': self._encode_flair(post.link_flair_text),
        }

    def _transform_post(self, post) -> dict:
        logging.debug(f"Transforming post {post.id}")
        return {
            'title': post.title,
            'author': str(post.author),
            'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
            'score': post.score,
            'num_comments': post.num_comments,
            'awards': post.total_awards_received,
            'body': post.selftext,
            'url': post.url,
            'flair': post.link_flair_text,
            'post_id': post.id,
            'permalink': f"https://www.reddit.com{post.permalink}",
            'comments': self._fetch_top_comments(post.id) if self.get_comments else [],
        }

    def _encode_flair(self, flair_text: str) -> int:
        if flair_text is None:
            return 0
        if flair_text not in self.flair_list:
            self.flair_list.append(flair_text)
        return self.flair_list.index(flair_text) + 1

    def _fetch_top_comments(self, post_id: str, limit: int = 3):
        logging.debug(f"Fetching top {limit} comments for post {post_id}")
        submission = self.reddit.submission(id=post_id)
        submission.comment_sort = 'top'
        submission.comment_limit = limit
        top_comments = []

        submission.comments.replace_more(limit=0)
        for comment in submission.comments[:limit]:
            top_comments.append({
                'author': str(comment.author),
                'body': comment.body,
                'score': comment.score,
                'created_utc': datetime.fromtimestamp(comment.created_utc).isoformat()
            })

        logging.debug(f"Fetched {len(top_comments)} top comments for post {post_id}")
        return top_comments

    def _format_output(self, collected_data, processed=False):
        if processed:
            flattened = []
            for month_data in collected_data.values():
                for post in month_data['posts']:
                    filtered_post = {k: v for k, v in post.items()
                                     if k not in ['post_id', 'created_utc']}
                    flattened.append(filtered_post)
            return flattened
        else:
            output = []
            for month, data in collected_data.items():
                output.append({
                    'month': month,
                    'num_posts': len(data['posts']),
                    'posts': data['posts']
                })
            return sorted(output, key=lambda x: x['month'], reverse=True)

    def save_data(self, data, subreddit_name, processed: bool = False):
        dir_path = f"data/{'processed' if processed else 'raw'}"
        os.makedirs(dir_path, exist_ok=True)
        filename = f"{dir_path}/{subreddit_name}_grouped.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved grouped data to {filename}")
        return filename

    def __del__(self):
        logging.info("RedditScraper instance destroyed")
