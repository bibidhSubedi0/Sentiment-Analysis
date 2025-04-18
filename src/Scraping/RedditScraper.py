import json
import logging
from datetime import datetime, timedelta, timezone
import sys
from typing import Tuple, List, Dict
import os

import praw


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('reddit_scraper.log')
    ]
)


class RedditScraper:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, get_comments: bool = False):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False
        )
        self.get_comments = get_comments
        logging.info("RedditScraper initialized successfully")

    def collect_posts(self, subreddit_name: str, count: int) -> Tuple[List[Dict], str]:
        """Collect recent posts from a subreddit with deduplication"""
        collected_posts = []
        seen_ids = set()
        start_date = datetime.now(timezone.utc) - timedelta(days=30)
        sort_methods = ['hot', 'new', 'top', 'rising', 'controversial']

        logging.info(f"Collecting posts from r/{subreddit_name} from last 30 days")

        for sort_method in sort_methods:
            try:
                time_filter = 'month' if sort_method in ('top', 'controversial') else None
                posts = self._fetch_batch(
                    subreddit_name,
                    sort_method,
                    count * 2,
                    time_filter=time_filter
                )

                for post in posts:
                    post_date = datetime.fromisoformat(post['created_utc'])
                    if post_date >= start_date and post['post_id'] not in seen_ids:
                        seen_ids.add(post['post_id'])
                        collected_posts.append(post)
                        logging.debug(f"Added post from {post_date}")
                    else:
                        logging.debug(f"Skipped post from {post_date}")

            except Exception as e:
                logging.error(f"Failed {sort_method}: {str(e)}", exc_info=True)

        logging.info(f"Collected {len(collected_posts)} valid posts from last 30 days")
        return self._format_flattened(collected_posts, subreddit_name)

    def _fetch_batch(self, subreddit_name: str, sort_method: str, limit: int, time_filter: str = None) -> List[Dict]:
        """Fetch a batch of posts using specified sorting method"""
        subreddit = self.reddit.subreddit(subreddit_name)
        method = getattr(subreddit, sort_method, None)

        if not method:
            logging.error(f"Invalid sort method: {sort_method}")
            return []

        try:
            params = {'limit': limit}
            if time_filter and hasattr(method, 'time_filter'):
                params['time_filter'] = time_filter

            return [self._transform_post(p) for p in method(**params)]
        except Exception as e:
            logging.error(f"Error fetching {sort_method}: {e}", exc_info=True)
            return []

    def _format_flattened(self, posts: List[Dict], subreddit_name: str) -> Tuple[List[Dict], str]:
        """Format and save posts to JSON file"""
        flattened = [{
            "title": p['title'],
            "author": p['author'],
            "created_utc": p['created_utc'],
            "score": p['score'],
            "num_comments": p['num_comments'],
            "awards": p['awards'],
            "body": p['body'],
            "url": p['url'],
            "flair": p['flair'],
            "post_id": p['post_id'],
            "permalink": p['permalink'],
            "comments": p['comments']
        } for p in posts]

        filename = f"data/raw/{subreddit_name}.json"
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(flattened, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logging.error(f"Failed to save data: {str(e)}", exc_info=True)
            raise

        return flattened, filename

    def _transform_post(self, post: praw.models.Submission) -> Dict:
        """Transform PRAW submission object to dictionary"""
        return {
            'title': post.title,
            'author': str(post.author) if post.author else '[deleted]',
            'created_utc': datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
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

    def _fetch_top_comments(self, post_id: str, limit: int = 3) -> List[Dict]:
        """Fetch top comments from a post"""
        logging.info(f"Fetching top {limit} comments for post {post_id}")
        submission = self.reddit.submission(id=post_id)
        submission.comment_sort = 'top'
        submission.comment_limit = limit * 2  # Fetch extra to account for removed comments

        comments = []
        try:
            submission.comments.replace_more(limit=0)
            valid_comments = [c for c in submission.comments if not c.body == '[removed]']
            for comment in valid_comments[:limit]:
                comments.append({
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'body': comment.body,
                    'score': comment.score,
                    'created_utc': datetime.utcfromtimestamp(comment.created_utc).isoformat() + 'Z'
                })
        except Exception as e:
            logging.error(f"Failed to fetch comments: {str(e)}", exc_info=True)

        logging.info(f"Fetched {len(comments)} top comments for post {post_id}")
        return comments

    def __del__(self):
        logging.info("RedditScraper instance destroyed")
