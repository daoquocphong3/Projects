# Vietnamese Sentiment Analysis

## Crawl Data
- Using selenium to crawl data of mobile phones from Thegioididong and Shopee.

- Using API crawl data from FPT Shop (handle security key of the API)

## Preprocessing Data
- Remove emoji
- Remove sentence about the shop, clerk, or delivery, etc.
- Change short forms(Acronyms) of the word to normal forms.
- Using Google Doc to correct spelling.
- Using regex to perform sentence segmentation.

## Sentiment Analysis
Perform 3 kinds of Word Embedding: 
- TF-IDF
- Word2Vec
- PhoBert

Build model to predict the label
- Logistic Regression for TF-IDF
- CNN for Word2Vec
- PhoBert 
