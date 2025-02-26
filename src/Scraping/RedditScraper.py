import praw  # type: ignore
import json
import logging
from datetime import datetime
from enum import Enum

logging.basicConfig(
    filename='reddit_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class RedditScraper:
    """RedditScraper class to fetch posts from Reddit using PRAW"""
    class TimeFilter(Enum):
        """Enum to constrain time filtering"""
        ALL = 'all'
        DAY = 'day'
        HOUR = 'hour'
        MONTH = 'month'
        WEEK = 'week'
        YEAR = 'year'

    class SortBy(Enum):
        """Enum to constrain sorting"""
        HOT = 'hot'
        NEW = 'new'
        TOP = 'top'
        RISING = 'rising'
        CONTROVERSIAL = 'controversial'

    def __init__(self, client_id="your_client_id", client_secret="your_client_secret", user_agent="your_user_agent"):
        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                check_for_async=False
            )
            logging.info("RedditScraper initialized successfully")
        except Exception as e:
            logging.error(f"Initialization failed: {str(e)}")
            raise

    def get_posts(self, subreddit_name, sort_by=SortBy.TOP,
                  limit=100, time_filter=TimeFilter.ALL):
        """Fetch posts with enum-constrained sorting and time filtering"""
        try:
            logging.info(f"Fetching {limit} {sort_by.value} posts from r/{subreddit_name}")

            subreddit = self.reddit.subreddit(subreddit_name)
            method = getattr(subreddit, sort_by.value)

            # Handle different method signatures
            if sort_by in [self.SortBy.TOP, self.SortBy.CONTROVERSIAL]:
                posts = method(limit=limit, time_filter=time_filter.value)
            else:
                posts = method(limit=limit)

            return self._save_posts(posts, subreddit_name, sort_by, time_filter)

        except Exception as e:
            logging.error(f"Processing error: {str(e)}")
            raise

    @staticmethod
    def _save_posts(posts, subreddit_name, sort_by, time_filter):
        """Internal method to handle data serialization"""
        posts_data = []
        for post in posts:
            posts_data.append({
                'title': post.title,
                'score': post.score,
                'id': post.id,
                'url': post.url,
                'author': str(post.author),
                'body': post.selftext,
                'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                'num_comments': post.num_comments,
                'permalink': post.permalink,
                'flair': post.link_flair_text,
                'nsfw': post.over_18,
                'awards': post.total_awards_received
            })

        filename = f"data/raw/{subreddit_name}_{sort_by.value}_{time_filter.value}_{datetime.now().strftime('%Y%m%d')}.json"

        with open(filename, 'w', encoding='utf-8') as file:
            # noinspection PyTypeChecker
            json.dump(posts_data, file, indent=2, ensure_ascii=False)

        logging.info(f"Saved {len(posts_data)} posts to {filename}")
        return filename

    def __del__(self):
        logging.info("RedditScraper instance destroyed")
