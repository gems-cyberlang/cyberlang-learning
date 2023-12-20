# CYB3RL4NG COMMENT COLLECTOR v0.01
# You will need you enter your config into config.ini
import os
import typing
import configparser
import praw

# Absolsutes
CONF_FILE: str = "config.ini"
REQURIED_CONFIG: dict = {
        "REDDIT": ['user_name', 'user_pass', 'app_uid', 'app_seceret']}

class CommentCollector:
    def __init__(self):
        """__init__(self)

        Initialize config. Loading of modules is done separately.
        """
        self.authenticated = False 
        self.reddit = None
        self.subreddit = None

        # Load config file
        config = configparser.ConfigParser()
        config.read(CONF_FILE)
        
        # Check configuration loaded and has all fields
        # Ill do this later
        self.config = config; 

    def run(self):
        """def authenticate() -> bool:
        Authenticates the comment collector with reddit. Based on config
        TODO: ADD ABILITY TO REAUTH
        """ 
        self.reddit = praw.Reddit(
            client_id = self.config["REDDIT"]["app_uid"],
            client_secret = self.config["REDDIT"]["app_seceret"],
            username = self.config["REDDIT"]["user_name"],
            password = self.config["REDDIT"]["user_pass"],
            user_agent = self.config["REDDIT"]["user_agent"]
        )
        print(self.reddit.user.me())
        self.subreddit = self.reddit.subreddit("test")
        print(self.subreddit.display_name)
        #submitions = self.subreddit.sort("hi", "relevance")
        submitions = self.subreddit.top()
        
 
        for submition in submitions:
            print(submition.title)
            submition.comments.replace_more(limit=None)
            for comment in submition.comments.list():
                print(comment.body)

if(__name__ == "__main__"):
    commment_collector = CommentCollector()
    commment_collector.run()
