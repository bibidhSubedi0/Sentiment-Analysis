import praw
import json
import logging
from datetime import datetime, timedelta
import sys
from enum import Enum
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
    def __init__(self, client_id, client_secret, user_agent):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False
        )
        logging.info("RedditScraper initialized successfully")

    def collect_posts(self, subreddit_name: str, count: int):
        collected_data = defaultdict(lambda: {"posts": []})
        seen_posts = set()

        logging.info(f"Starting to collect posts from r/{subreddit_name}")

        for sort_method in ['hot', 'new', 'top', 'rising', 'controversial']:
            try:
                logging.info(f"Fetching {count} posts with sort method: {sort_method}")
                posts = self._fetch_batch(subreddit_name, sort_method, count)

                for post in posts:
                    if post['post_id'] not in seen_posts:
                        seen_posts.add(post['post_id'])
                        month_key = datetime.fromisoformat(post['created_utc']).strftime('%Y-%m')
                        collected_data[month_key]['posts'].append(post)

            except Exception as e:
                logging.error(f"Failed {sort_method}: {str(e)}")
        output = self._format_output(collected_data)
        logging.info(f"Collected {len(output)} months of data")
        filename = self.save_data(output, subreddit_name)
        return output , filename

    def _fetch_batch(self, subreddit_name: str, sort_method: str, limit: int):
        subreddit = self.reddit.subreddit(subreddit_name)
        method = getattr(subreddit, sort_method)

        posts = list(method(limit=limit))
        logging.debug(f"Fetched {len(posts)} posts from r/{subreddit_name}")
        return [self._transform_post(p) for p in posts]

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
            'comments': self._fetch_top_comments(post.id)
        }

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

    def _format_output(self, collected_data):
        output = []
        for month, data in collected_data.items():
            start_time = datetime.strptime(month, '%Y-%m')
            end_time = (start_time + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            output.append({
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'month': month,
                'num_posts': len(data['posts']),
                'posts': data['posts']
            })
            output.sort(key=lambda x: x['month'], reverse=True)
        return output

    def save_data(self, data, subreddit_name):
        filename = f"data/raw/{subreddit_name}_grouped.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved grouped data to {filename}")
        return filename

    def __del__(self):
        logging.info("RedditScraper instance destroyed")
