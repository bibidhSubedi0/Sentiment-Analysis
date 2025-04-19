import logging
import time
from src.Preprocessing import preprocessor
from src.Scraping.RedditScraper import RedditScraper
from dotenv import dotenv_values  # type: ignore


def main():
    config = dotenv_values(".env")
    scraper = RedditScraper(
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
        user_agent=config["USER_AGENT"],
        get_comments=False
    )

    start_time = time.time()
    logging.info("Starting scraping...")

    # Collect posts from various subreddits
    name_subreddit = [
        "facebook",
        "nvidia",
        "teslamotors",
        "Amd",
        "spacex",]
    # Collect posts from various subreddits

    try:

        data = [scraper.collect_posts(subreddit, 100) for subreddit in name_subreddit]
        # data_facebook, filename_facebook = scraper.collect_posts("facebook", 100)
        # data_nvidia, filename_nvidia = scraper.collect_posts("nvidia", 100)
        # data_tesla, filename_tesla = scraper.collect_posts("teslamotors", 100)
        # data_Amd, filename_Amd = scraper.collect_posts("Amd", 100)
        # data_spacex, filename_spacex = scraper.collect_posts("spacex", 100)

    finally:
        total_time = time.time() - start_time
        # Convert to hours, minutes, seconds
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.info(
            f"Total scraping time: {int(hours)}h {int(minutes)}m {seconds:.2f}s"
        )

    # Preprocess the data
    for posts, filename in data:
        logging.info(f"Preprocessing data from {filename}")
        preprocessor.create_preprocessed_json_by_blocks_of_days(filename)



if __name__ == '__main__':
    main()
else:
    raise Exception("This file should be run as the main file.")