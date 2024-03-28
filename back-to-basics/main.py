import argparse
import requests
import json
import time

BASE_URL = "https://www.reddit.com"

HEADERS = {
    'User-Agent': 'cyberlang reaserach',
}
class scraper_2000:
    def __init__(self,  subreddit:str, word:str, max:int = 100) -> None:
        self.subreddit = subreddit
        self.word = word
        self.max = max
        self.max_retries = 5
        self.count = 0
        self.retries = 0
        self.file = open("./testFile", "+w")

    def subreddit_search(self, q:str, sub_reddit:str, after:str = None, sort:str = "new", limit:int = 10, restrict_sr:bool = True):
        url = BASE_URL + "/r/" + sub_reddit + "/search.json"
        params = {'q': q,
                  'sort': sort,
                  'limit': limit,
                  'restrict_sr': restrict_sr}

        if(after != None):
            params['after'] = after
        
        resp = requests.get(url, params=params, headers=HEADERS)
        if(resp.status_code == 200):
            self.count = self.count + int(resp.json()['data']['dist'])

        return resp
    
    def resp_err(self, resp:requests.Response):
        print(f'ERROR: url {resp.url}, status {resp.status_code}, content {resp.content}')
        exit()

    def run(self):
        raw_file = open("./out.json", "+w")
        time_file = open("./time.txt", "+w")
        # First get a starting point the most recent post
        resp = self.subreddit_search(self.word, self.subreddit, limit=1)
        
        if(resp.status_code != 200): self.resp_err()

        json.dump(resp.json(), raw_file, indent=True)
        item = resp.json()['data']['children'][0]

        print(time.ctime(item['data']['created']))
        time_file.write(time.ctime(item['data']['created']) + "\n")

        curr_after = item['data']['name']

        while self.count < self.max:
            resp =  self.subreddit_search(self.word, self.subreddit, after=curr_after, limit=100)
            while(resp.status_code != 200 and self.retries < self.max_retries):
                if(resp.status_code == 409): # to many requests 
                    time.sleep(6000) 
                    print("Waiting")

                resp =  self.subreddit_search(self.word, self.subreddit, after=curr_after, limit=100)

                self.retries += 1
            
            if(self.retries == self.max_retries):
                json.dump(resp.json(), raw_file, indent=True)

                for post in resp.json()['data']['children']:
                    print(time.ctime(post['data']['created']))
                    time_file.write(time.ctime(post['data']['created']) + "\n")

                print("reties exeded leaving")
                exit()
            else:
                self.retries = 0

            json.dump(resp.json(), raw_file, indent=True)

            for post in resp.json()['data']['children']:
                print(time.ctime(post['data']['created']))
                time_file.write(time.ctime(post['data']['created']) + "\n")

            item = resp.json()['data']['children'][0]
            curr_after = item['data']['name']

        raw_file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--subreddit', metavar='SUBREDDIT_NAME', help='Subreddit to get data from', default="unix")
    parser.add_argument('--query', help='Search term', default="unix")
    parser.add_argument('--end', metavar='ID', help='ID of the last post, to search from backwards', default=None)
    args = parser.parse_args()
    scrapy = scraper_2000(args.subreddit, args.query, max=500)
    scrapy.run()