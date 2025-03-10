import logging
from src.Scraping.RedditScraper import RedditScraper
from dotenv import dotenv_values  # type: ignore


def main():
    config = dotenv_values(".env")
    scraper = RedditScraper(client_id=config["CLIENT_ID"], client_secret=config["CLIENT_SECRET"],
                           user_agent=config["USER_AGENT"])
    data, filename = scraper.collect_posts("facebook", 5)

    # find count of posts
    count = 0
    for month in data:
        count += len(month['posts'])
    logging.info(f"Collected {count} posts")


if __name__ == '__main__':
    main()
else:
    raise Exception("This file should be run as the main file.")
