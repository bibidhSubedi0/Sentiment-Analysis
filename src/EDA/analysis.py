
import numpy as np
import pandas as pd
import nltk as nlt
import json
import seaborn as sns
import matplotlib.pyplot as plt
import os
import json
from collections import Counter
import re
from nltk.corpus import stopwords
from nltk.util import ngrams
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer



file_path = "data/processed/Amd.json"

def get_filename(path):
    return os.path.splitext(os.path.basename(path))[0]



def CompleteAnalysis(file_path):

    filepathbase=f"data/PostEDA/{get_filename(file_path)}/"
        # Ensure the folder exists
    if not os.path.exists(filepathbase):
        os.makedirs(filepathbase)

    with open(file_path, "r", encoding="utf-8") as file:
        json_data = json.load(file)

    # Add block index to each post
    processed = []
    for block_index, block in enumerate(json_data):
        for post in block:
            post['block_index'] = block_index  # You can also add a date range or tag
            processed.append(post)

    # Create DataFrame
    df = pd.DataFrame(processed)

    # Optional: Create datetime column
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])

    # Now you can group by 'block_index' to get back the structure
    df.head()



    # # Sentiment over time

    df['date'] = pd.to_datetime(df['date'])

    # Group by date and sum the scores
    daily_scores = df.groupby(df['date'].dt.date)['score'].sum().reset_index()

    # Rename columns for clarity
    daily_scores.columns = ['day', 'score']



    daily_scores['rolling_score'] = daily_scores['score'].rolling(window=3).mean()

    plt.figure(figsize=(12, 6))
    plt.plot(daily_scores['day'], daily_scores['score'], alpha=0.4, label='Daily Score')
    plt.plot(daily_scores['day'], daily_scores['rolling_score'], color='red', label='3-Day Rolling Avg')
    plt.title("Reddit Post Scores with Rolling Average")
    plt.xlabel("Date")
    plt.ylabel("Score")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{filepathbase}RollingScore.png')


    # # Text Based Analysis

    text = df["combined_text"]

    

    # Total character counts per post
    character_count = list(map(lambda x:len(x),text))

    df["character_count"] = character_count

    # Total word count per post
    word_counts = list(map(lambda s: len(s.split()), text))
    df["Word_count"] = word_counts

    # Seperated Words from total list
    words = [word.lower() for t in text for word in re.findall(r'\b\w+\b', t)]
    # df["Sepeated_words"] = words

    # Sepeated words PER list
    words_per_list = []
    for t in text:
        words_per_list.append([text.lower() for text in re.findall(r'\b\w+\b', t)])

    df["Seperated_List"] = words_per_list

    # Frequncy of words in the entire dataframe
    word_freq_counts = Counter(words)

    # nlt.download('stopwords',download_dir="/src/EDA")
    df.head()

    
    stop_words = set(stopwords.words('english'))  # Load English stopwords
    words = [w for w in words if w not in stop_words]
    word_freq_counts = Counter(words)
    most_common = word_freq_counts.most_common(20)  

    common_words_df = pd.DataFrame(most_common, columns=['Word', 'Frequency'])
    common_words_df.head()

    

    # Generate unigrams, bigrams, trigrams
    unigrams = list(ngrams(words, 1))
    bigrams = list(ngrams(words, 2))
    trigrams = list(ngrams(words, 3))

    # Count most common n-grams

    grams_df = pd.DataFrame()
    grams_df["bigrams"] = Counter(bigrams).most_common(20)
    grams_df["trigrams"] = Counter(trigrams).most_common(20)
    grams_df.head()


    
    sia = SentimentIntensityAnalyzer()

    sent = df['combined_text'].apply(lambda post: sia.polarity_scores(post)['compound'])
    df["Sentiment"] = sent

    df.head()


    # Make sure these columns exist — adjust as needed
    if 'body' not in df.columns and 'combined_text' in df.columns:
        df['body'] = df['combined_text']

    # Create post length column
    df['post_length'] = df['body'].apply(len)

    # Extract hour from the 'time' column
    df['hour'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.hour

    # Compute popularity metric
    df['popularity'] = df['score'] + df['num_comments']

    # --- Plot: Sentiment vs Post Length ---
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='post_length', y='Sentiment')
    plt.title('Sentiment vs Post Length')
    plt.xlabel('Post Length (characters)')
    plt.ylabel('Sentiment')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{filepathbase}SentVlen.png')

    # --- Plot: Sentiment vs Hour of Day ---
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='hour', y='Sentiment')
    plt.title('Sentiment vs Time of Day')
    plt.xlabel('Hour')
    plt.ylabel('Sentiment')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{filepathbase}SentVTime.png')

    # --- Plot: Sentiment vs Popularity ---
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='popularity', y='Sentiment')
    plt.title('Sentiment vs Popularity (Score + Comments)')
    plt.xlabel('Popularity')
    plt.ylabel('Sentiment')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{filepathbase}SentVPop.png')

    # --- Correlation Analysis ---
    correlations = df[['post_length', 'hour', 'popularity', 'Sentiment']].corr()


    relevant_columns = ['date', 'score', 'num_comments', 'flair','character_count','Word_count', 'Sentiment','post_length','hour', "popularity","block_index"]
    new_df = df[relevant_columns]
    new_df.head()

    sentiment_vs_date = new_df[['date', 'Sentiment']]

    # Filter for year 2025
    sentiment_vs_date_2025 = sentiment_vs_date[sentiment_vs_date['date'].dt.year == 2025]


    

    # Sort by date
    sentiment_vs_date_2025 = sentiment_vs_date_2025.sort_values("date")

    # Apply a 3-day rolling average for smoothing
    sentiment_vs_date_2025['Smoothed Sentiment'] = sentiment_vs_date_2025['Sentiment'].rolling(window=3).mean()

    # Set style
    sns.set(style="whitegrid")

    # Plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=sentiment_vs_date_2025, x='date', y='Smoothed Sentiment', marker='o', color='steelblue')
    plt.title('Sentiment Trend Over Time (2025)', fontsize=16)
    plt.xlabel('Date')
    plt.ylabel('Average Sentiment (3-Day Rolling)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{filepathbase}SentOverTime.png')


    new_df.head()

    new_df.head()


    # Count how many posts are in each block
    block_counts = df['block_index'].value_counts().sort_index()

    # Plot
    plt.figure(figsize=(8, 5))
    sns.barplot(x=block_counts.index, y=block_counts.values, palette="viridis")
    plt.title("Number of Posts per 3-Day Block (block_index)")
    plt.xlabel("Block Index")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    plt.grid(axis='y')
    plt.savefig(f'{filepathbase}PostPerBlock.png')


    grouped = new_df.groupby('block_index')
    grouped.head()



    stop_words = set(stopwords.words('english'))


    exportDF = pd.DataFrame()
    exportDF['date'] = grouped['date'].first().values        # Date of first post in the block
    exportDF['company'] = 'Facebook'                          
    exportDF['avg_sentiment'] = grouped['Sentiment'].mean().values
    exportDF['num_comments'] = grouped['num_comments'].sum().values
    exportDF['num_posts'] = grouped.size().values             # Number of rows in each group


    exportDF.head()

    common_words_df.head()

    # Define path
    path = "data/PostEDA"
    processed_dir = path
    os.makedirs(processed_dir, exist_ok=True)

    # Convert date columns to string (ISO format)
    exportDF_serializable = exportDF.copy()
    exportDF_serializable['date'] = exportDF_serializable['date'].astype(str)

    common_words_serializable = common_words_df.copy()
    if 'date' in common_words_serializable.columns:
        common_words_serializable['date'] = common_words_serializable['date'].astype(str)



    # Dump exportDF
    with open(os.path.join(filepathbase, 'Export.json'), 'w') as f:
        json.dump(exportDF_serializable.to_dict(orient='records'), f, indent=2)

    # Dump common_words_df
    with open(os.path.join(filepathbase, 'Words.json'), 'w') as f:
        json.dump(common_words_serializable.to_dict(orient='records'), f, indent=2)


    return filepathbase



if __name__ == '__main__':
    CompleteAnalysis(file_path)