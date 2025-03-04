# def main():
#     print("Hello World")

from textblob import TextBlob  
from afinn import Afinn  

# Sample text  
text = "I don't dislike this product."  

# Perform sentiment analysis  
blob = TextBlob(text)  
sentiment_score = blob.sentiment.polarity  # Ranges from -1 (negative) to 1 (positive)  

# Determine sentiment category  
if sentiment_score > 0:
    sentiment = "Positive"
elif sentiment_score < 0:
    sentiment = "Negative"
else:
    sentiment = "Neutral"

print(f"Text: {text}")
print(f"Sentiment Score: {sentiment_score}")
print(f"Overall Sentiment: {sentiment}")

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  

analyzer = SentimentIntensityAnalyzer()  
sentiment = analyzer.polarity_scores(text)  

print(sentiment)

afinn = Afinn()  
score = afinn.score(text)  

print(f"Sentiment Score: {score}")