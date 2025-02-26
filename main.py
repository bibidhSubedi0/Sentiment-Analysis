import src.test as test
from src.Scraping.RedditScraper import RedditScraper
from dotenv import dotenv_values  # type: ignore


def main():
    config = dotenv_values(".env")
    reddit = RedditScraper(client_id=config["CLIENT_ID"], client_secret=config["CLIENT_SECRET"],
                           user_agent=config["USER_AGENT"])

    # Fetch top 100 posts of all time
    reddit.get_posts("NepalStock", sort_by=RedditScraper.SortBy.TOP, limit=100,
                     time_filter=RedditScraper.TimeFilter.ALL)

    # Fetch controversial posts from last month
    reddit.get_posts("NepalStock",
                     sort_by=RedditScraper.SortBy.CONTROVERSIAL,
                     time_filter=RedditScraper.TimeFilter.MONTH, limit=100)

    # Fetch hot posts (no time filter)
    reddit.get_posts("NepalStock", sort_by=RedditScraper.SortBy.HOT, limit=100,
                     time_filter=RedditScraper.TimeFilter.ALL)

    test.main()


if __name__ == '__main__':
    main()
else:
    raise Exception("This file should be run as the main file.")
