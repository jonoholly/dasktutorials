# parsing unstructured data from fine food reviews with Bags

import dask.bag as bag
import os
from dask.delayed import delayed
from dask.diagnostics import ProgressBar
import numpy as np
from dask import array as dask_array

os.chdir("/data")
raw_data = bag.read_text("finefoods.txt", encoding="cp1252")  # note - fails with UTF-8
# raw_data.count().compute()

# okay, now implement it ourselves


def get_next_part(file, start_index, span_index=0, blocksize=1000):
    file.seek(start_index)
    buffer = file.read(blocksize + span_index).decode("cp1252")
    delimiter_position = buffer.find("\n\n")
    if delimiter_position == -1:
        return get_next_part(file, start_index, span_index + blocksize)
    else:
        file.seek(start_index)
        return start_index, delimiter_position


with open("finefoods.txt", "rb") as file_handle:
    size = file_handle.seek(0, 2) - 1
    more_data = True
    output = []
    current_position = next_position = 0
    while more_data:
        if current_position >= size:
            more_data = False
        else:
            current_position, next_position = get_next_part(
                file_handle, current_position, 0
            )
            output.append((current_position, next_position))
            current_position = current_position + next_position + 2


def get_item(filename, start_index, delimiter_position, encoding="cp1252"):
    with open(filename, "rb") as file_handle:
        file_handle.seek(start_index)
        text = file_handle.read(delimiter_position).decode(encoding)
        elements = text.strip().split("\n")
        key_value_pairs = [
            (
                (element.split(": ")[0], element.split(": ")[1])
                if len(element.split(": ")) > 1
                else ("unknown", element)
            )
            for element in elements
        ]
        return dict(key_value_pairs)


reviews = bag.from_sequence(output).map(lambda x: get_item("finefoods.txt", x[0], x[1]))
# reviews.take(3)  # see examples


# extract values from dictionary
def get_score(element):
    score_numeric = float(element["review/score"])
    return score_numeric


# apply scoring
review_scores = reviews.map(get_score)
review_scores.take(10)
# filter
specific_item = reviews.filter(
    lambda element: element["product/productId"] == "B001E4KFG0"
)
specific_item.take(5)


# example - filter out unhelpful helpfulness. compare helpful to unhelpful reviews
def is_helpful(element):
    helpfulness = element["review/helpfulness"].strip().split("/")
    number_of_helpful_votes = float(helpfulness[0])
    number_of_total_votes = float(helpfulness[1])
    # Watch for divide by 0 errors
    if number_of_total_votes > 1:
        return number_of_helpful_votes / number_of_total_votes > 0.75
    else:
        return False


helpful_reviews = reviews.filter(is_helpful)
helpful_reviews.take(2)
helpful_review_scores = helpful_reviews.map(get_score)

with ProgressBar():
    all_mean = review_scores.mean().compute()
    helpful_mean = helpful_review_scores.mean().compute()
print(
    f"Mean Score of All Reviews: {round(all_mean, 2)}\nMean Score of Helpful Reviews: {round(helpful_mean,2)}"
)


# compare length of helpful/unhelpful reviews
def get_length(element):
    return len(element["review/text"])


with ProgressBar():
    review_length_helpful = helpful_reviews.map(get_length).mean().compute()
    review_length_unhelpful = (
        reviews.filter(lambda review: not is_helpful(review))
        .map(get_length)
        .mean()
        .compute()
    )
print(
    f"Mean Length of Helpful Reviews: {round(review_length_helpful, 2)}\nMean Length of Unhelpful Reviews: {round(review_length_unhelpful,2)}"
)


# folding example
def count(accumulator, element):
    return accumulator + 1


def combine(total1, total2):
    return total1 + total2


# key -  get_score calculates the number from 1-5
# binop - count specifies what to do (add 1). ignores element, but this could be used if it was a groupby over something else.
# 0 is the initial value for accumulator
# combine - combine says what to do with results
# 0 is initial value for combine
with ProgressBar():
    count_of_reviews_by_score = reviews.foldby(
        get_score, count, 0, combine, 0
    ).compute()
count_of_reviews_by_score


# get results and parse into a dataframe
def get_score_and_helpfulness(element):
    score_numeric = float(element["review/score"])
    helpfulness = element["review/helpfulness"].strip().split("/")
    number_of_helpful_votes = float(helpfulness[0])
    number_of_total_votes = float(helpfulness[1])
    # Watch for divide by 0 errors
    if number_of_total_votes > 0:
        helpfulness_percent = number_of_helpful_votes / number_of_total_votes
    else:
        helpfulness_percent = 0.0
    return (score_numeric, helpfulness_percent)


scores_and_helpfulness = reviews.map(get_score_and_helpfulness).to_dataframe(
    meta={"Review Scores": float, "Helpfulness Percent": float}
)
with ProgressBar():
    scores_and_helpfulness_stats = scores_and_helpfulness.describe().compute()
scores_and_helpfulness_stats

# application - use NLTK to filter reviews
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from functools import partial
import nltk

# download package stopwords
nltk.download("stopwords")

tokenizer = RegexpTokenizer(r"\w+")


def extract_reviews(element):
    return element["review/text"].lower()


def filter_stopword(word, stopwords):
    return word not in stopwords


def filter_stopwords(tokens, stopwords):
    return list(filter(partial(filter_stopword, stopwords=stopwords), tokens))


stopword_set = set(stopwords.words("english"))

review_text = reviews.map(extract_reviews)
review_text_tokens = review_text.map(tokenizer.tokenize)
review_text_clean = review_text_tokens.map(
    partial(filter_stopwords, stopwords=stopword_set)
)
review_text_clean.take(1)


def make_bigrams(tokens):
    return set(nltk.bigrams(tokens))


review_bigrams = review_text_clean.map(make_bigrams)
review_bigrams.take(2)

all_bigrams = review_bigrams.flatten()
all_bigrams.take(10)
all_bigrams.persist()

with ProgressBar():
    top10_bigrams = (
        all_bigrams.foldby(lambda x: x, count, 0, combine, 0)
        .topk(10, key=lambda x: x[1])
        .compute()
    )
top10_bigrams

# add some more stopwords to get rid of html and urls
more_stopwords = {"br", "amazon", "com", "http", "www", "href", "gp"}
all_stopwords = stopword_set.union(more_stopwords)

filtered_bigrams = (
    review_text_tokens.map(partial(filter_stopwords, stopwords=all_stopwords))
    .map(make_bigrams)
    .flatten()
)

with ProgressBar():
    top10_bigrams = (
        filtered_bigrams.foldby(lambda x: x, count, 0, combine, 0)
        .topk(10, key=lambda x: x[1])
        .compute()
    )
top10_bigrams


# okay now use this to do ML
def tag_positive_negative_by_score(element):
    if float(element["review/score"]) > 3:
        element["review/sentiment"] = "positive"
    else:
        element["review/sentiment"] = "negative"
    return element


tagged_reviews = reviews.map(tag_positive_negative_by_score)
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from functools import partial

tokenizer = RegexpTokenizer(r"\w+")


def extract_reviews(element):
    element["review/tokens"] = element["review/text"].lower()
    return element


def tokenize_reviews(element):
    element["review/tokens"] = tokenizer.tokenize(element["review/tokens"])
    return element


def filter_stopwords(element, stopwords):
    element["review/tokens"] = list(
        filter(partial(filter_stopword, stopwords=stopwords), element["review/tokens"])
    )
    return element


def extract_tokens(element):
    return element["review/tokens"]


stopword_set = set(stopwords.words("english"))
more_stopwords = {"br", "amazon", "com", "http", "www", "href", "gp"}
all_stopwords = stopword_set.union(more_stopwords)

review_extracted_text = tagged_reviews.map(extract_reviews)

review_tokens = review_extracted_text.map(tokenize_reviews)
review_text_clean = review_tokens.map(
    partial(filter_stopwords, stopwords=all_stopwords)
)
extracted_tokens = review_text_clean.map(extract_tokens)
unique_tokens = extracted_tokens.flatten().distinct()

with ProgressBar():
    number_of_tokens = unique_tokens.count().compute()
number_of_tokens


# find top 100 words
def count(accumulator, element):
    return accumulator + 1


def combine(total_1, total_2):
    return total_1 + total_2


with ProgressBar():
    token_counts = (
        extracted_tokens.flatten().foldby(lambda x: x, count, 0, combine, 0).compute()
    )

top_tokens = sorted(token_counts, key=lambda x: x[1], reverse=True)
top_100_tokens = list(map(lambda x: x[0], top_tokens[:100]))


# apply binary vectorization
def vectorize_tokens(element):
    vectorized_tokens = np.where(
        np.isin(top_100_tokens, element["review/tokens"]), 1, 0
    )
    element["review/token_vector"] = vectorized_tokens
    return element


def prep_model_data(element):
    return {
        "target": 1 if element["review/sentiment"] == "positive" else 0,
        "features": element["review/token_vector"],
    }


# map in the token vectors to 'review/token_vector', then convert to numpy form
model_data = review_text_clean.map(vectorize_tokens).map(prep_model_data)

model_data.take(5)


def stacker(partition):
    return dask_array.concatenate([element for element in partition])


with ProgressBar():
    feature_arrays = (
        model_data.pluck("features")
        .map(lambda x: dask_array.from_array(x, 1000).reshape(1, -1))
        .reduction(perpartition=stacker, aggregate=stacker)
    )
    feature_array = feature_arrays.compute()
feature_array

with ProgressBar():
    feature_array.rechunk(5000).to_zarr("/data/sentiment_feature_array.zarr")
    # feature_array = dask_array.from_zarr("/data/sentiment_feature_array.zarr")


with ProgressBar():
    target_arrays = (
        model_data.pluck("target")
        .map(lambda x: dask_array.from_array(x, 1000).reshape(-1, 1))
        .reduction(perpartition=stacker, aggregate=stacker)
    )
    target_arrays.compute().rechunk(5000).to_zarr("/data/sentiment_target_array.zarr")
