import json
import logging
from datetime import datetime, timedelta, timezone
import sys
from typing import Tuple, List, Dict
import os
import time

import praw

logging.basicConfig(
    level=logging.INFO,
    # format='%(asctime)s - %(levelname)s - %(message)s',
    format='%(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        # logging.FileHandler('reddit_scraper.log')
    ]
)
logger = logging.getLogger(__name__)


class RedditScraper:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, get_comments: bool = False):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False
        )
        self.get_comments = get_comments
        logger.debug(
            "Initializing RedditScraper with client ID: %s...%s, User-Agent: %s, Comments enabled: %s",
            client_id[:2], client_id[-2:],  # Obscure full client ID in logs
            user_agent,
            get_comments
        )

    def collect_posts(self, subreddit_name: str, count: int) -> Tuple[List[Dict], str]:
        """Collect recent posts from a subreddit with deduplication"""
        logger.info("🚀 Starting collection for r/%s (target: %d posts)", subreddit_name, count)
        start_time = time.time()
        collected_posts = []
        seen_ids = set()
        start_date = datetime.now(timezone.utc) - timedelta(days=30)

        logger.debug("Filtering posts newer than %s (UTC)", start_date.isoformat())

        sort_methods = [
                        # 'hot',
                        # 'new',
                        'top',
                        # 'rising',
                        'controversial'
                        ]
        total_processed = 0
        total_duplicates = 0
        total_expired = 0

        for sort_method in sort_methods:
            try:
                batch_start = time.time()
                logger.info("🔍 Processing '%s' sort method...", sort_method)

                time_filter = 'month' if sort_method in ('top', 'controversial') else None
                posts = self._fetch_batch(subreddit_name, sort_method, count * 2, time_filter)

                logger.debug("Retrieved %d posts from '%s' method", len(posts), sort_method)
                batch_duplicates = 0
                batch_expired = 0

                for post in posts:
                    total_processed += 1
                    post_date = datetime.fromisoformat(post['created_utc'])

                    if post_date < start_date:
                        batch_expired += 1
                        logger.debug("Skipping expired post ID %s (created: %s)",
                                     post['post_id'], post_date.isoformat())
                        continue

                    if post['post_id'] in seen_ids:
                        batch_duplicates += 1
                        logger.debug("Duplicate post ID %s found", post['post_id'])
                        continue

                    seen_ids.add(post['post_id'])
                    collected_posts.append(post)
                    logger.debug("✅ Added post ID %s (Score: %d, Comments: %d)",
                                 post['post_id'], post['score'], post['num_comments'])

                total_duplicates += batch_duplicates
                total_expired += batch_expired
                logger.info((
                    "🏁 Batch complete: %d new, %d duplicates, %d expired "
                    "(%.2fs) | Total: %d"
                ), len(posts) - batch_duplicates - batch_expired,
                    batch_duplicates, batch_expired,
                   time.time() - batch_start, len(collected_posts))

            except Exception as e:
                logger.error("❌ Failed %s method: %s", sort_method, str(e), exc_info=True)
                continue

        logger.info((
            "📊 Collection complete for r/%s\n"
            "Total processed: %d | Valid: %d | Duplicates: %d | Expired: %d\n"
            "Elapsed time: %.2fs"
        ), subreddit_name, total_processed, len(collected_posts),
            total_duplicates, total_expired, time.time() - start_time)

        return self._format_flattened(collected_posts, subreddit_name)

    def _fetch_batch(self, subreddit_name: str, sort_method: str, limit: int, time_filter: str = None) -> List[Dict]:
        """Fetch a batch of posts using specified sorting method"""
        logger.debug("Fetching batch: sub=%s, sort=%s, limit=%d, time_filter=%s",
                     subreddit_name, sort_method, limit, time_filter)

        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            # method = getattr(subreddit, sort_method, None)
            #
            # if not method:
            #     logger.warning("Invalid sort method '%s' for subreddit %s",
            #                    sort_method, subreddit_name)
            #     return []
            #
            # params = {'limit': limit}
            # if time_filter and hasattr(method, 'time_filter'):
            #     params['time_filter'] = time_filter
            if sort_method == 'top':
                posts = list(subreddit.top(time_filter=time_filter, limit=limit))
            elif sort_method == 'controversial':
                posts = list(subreddit.controversial(time_filter=time_filter, limit=limit))
            elif sort_method == 'hot':
                posts = list(subreddit.hot(limit=limit))
            elif sort_method == 'new':
                posts = list(subreddit.new(limit=limit))
            else:
                raise Exception(f"Invalid sort method: {sort_method}")

            # logger.debug("Executing API request with params: %s", params)
            logger.debug("Received %d raw posts", len(posts))

            return [self._transform_post(p) for p in posts]

        except praw.exceptions.APIException as api_error:
            logger.error("API Error: %s (HTTP %d)", api_error.message, api_error.error_type)
            return []
        except Exception as e:
            logger.error("Unexpected error in batch fetch: %s", str(e), exc_info=True)
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


        flattened.sort(key= lambda p: p['created_utc'], reverse=False)

        filename = f"data/raw/{subreddit_name}.json"
        logging.debug(f"Saving to {filename}")
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
        logger.debug("Transforming post ID %s", post.id)
        try:
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
        except Exception as e:
            logger.warning("Failed to transform post ID %s: %s", post.id, str(e))
            return {}

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
        logging.debug("RedditScraper instance destroyed")