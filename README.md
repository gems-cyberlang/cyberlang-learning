# cyberlang-learning

Small programs we write as we teach ourselves new technologies.

Each folder should be a `VENV` and should contain a `requirements.txt` if in python.

Some tips for using PRAW:
- Use `reddit.subreddit("all")` to get a `Subreddit` representing all of Reddit
    (useful if you want to search across all of Reddit)
- Use `reddit.subreddit("AskReddit+memes")` to get a `Subreddit` representing
  multiple subreddits
- In kwargs, pass the `params` argument to pass arguments directly to the API
