#TODO: add logs and error handling
from json import dumps
from kafka import KafkaProducer
import configparser
import praw
import threading

threads = []
# Financial subreddits focused on stock discussions (most active ones)
financial_subreddit_list = ['wallstreetbets', 'stocks', 'ValueInvesting', 'pennystocks', 'SecurityAnalysis', 'investing', 'StockMarket', 'options']

# Dynamic stock detection - no preset list, discover stocks mentioned in context

def detect_stock_tickers(text: str) -> list[str]:
    """Dynamic stock ticker detection - discovers any stocks mentioned in financial context"""
    import re
    detected = []
    
    # Convert text to uppercase for pattern matching
    text_upper = text.upper()
    
    # Pattern 1: $TICKER (most reliable - almost always a stock)
    dollar_tickers = re.findall(r'\$([A-Z]{1,5})', text_upper)
    detected.extend(dollar_tickers)
    
    # Pattern 2: Strong financial context patterns
    # "TICKER stock", "TICKER shares", "TICKER equity"
    stock_context = re.findall(r'\b([A-Z]{1,5})\s+(?:stock|shares|equity|share)\b', text_upper)
    detected.extend(stock_context)
    
    # Pattern 3: Trading action patterns
    # "buy TICKER", "sell TICKER", "bought TICKER", etc.
    trading_actions = re.findall(r'\b(?:buy|sell|bought|sold|holding|holds|own|owns|long|short)\s+([A-Z]{1,5})\b', text_upper)
    detected.extend(trading_actions)
    
    # Pattern 4: Options patterns (very stock-specific)
    # "TICKER calls", "TICKER puts", "TICKER options"
    options_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:calls|puts|call|put|options|option)\b', text_upper)
    detected.extend(options_patterns)
    
    # Pattern 5: Price movement patterns
    # "TICKER up", "TICKER down", "TICKER mooning", "TICKER dipped"
    price_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:up|down|mooning|moon|dipped|dip|pumped|dumped|rallied|crashed|soared|tanked)\b', text_upper)
    detected.extend(price_patterns)
    
    # Pattern 6: Financial metrics patterns
    # "TICKER earnings", "TICKER revenue", "TICKER PE ratio"
    metrics_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:earnings|revenue|profit|pe|pb|dividend|yield|eps)\b', text_upper)
    detected.extend(metrics_patterns)
    
    # Pattern 7: Company reference patterns
    # "TICKER company", "TICKER CEO", "TICKER announced"
    company_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:company|corp|inc|ceo|cfo|announced|reports?|beats?|misses?)\b', text_upper)
    detected.extend(company_patterns)
    
    # Exclude obvious non-stock words (but much smaller list than before)
    exclude_words = {
        'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'WAS', 'GET', 'HAS', 
        'HIM', 'HER', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'TWO', 'WHO', 
        'YES', 'YET', 'BUY', 'OUT', 'USE', 'WAY', 'WIN', 'GOT', 'PUT', 'RUN', 'SIT', 'TRY',
        'CEO', 'CFO', 'CTO', 'USA', 'USD', 'SEC', 'FDA', 'WSB', 'DD', 'FUD', 'LOL', 'OMG'
    }
    
    # Remove duplicates and filter out excluded words
    seen = set()
    unique_detected = []
    for ticker in detected:
        if ticker not in seen and ticker not in exclude_words and len(ticker) >= 1:
            seen.add(ticker)
            unique_detected.append(ticker)
    
    return unique_detected[:15]  # Allow more tickers since we're discovering dynamically

class RedditProducer:

    def __init__(self, subreddit_list: list[str], cred_file: str="secrets/credentials.cfg"):

        self.subreddit_list = subreddit_list
        self.reddit = self.__get_reddit_client__(cred_file)
        self.producer = KafkaProducer(bootstrap_servers=['kafkaservice:9092'],
                            value_serializer=lambda x:
                            dumps(x).encode('utf-8')
                        )


    def __get_reddit_client__(self, cred_file) -> praw.Reddit:

        config = configparser.ConfigParser()
        config.read_file(open(cred_file))

        try:
            client_id: str = config.get("reddit", "client_id")
            client_secret: str = config.get("reddit", "client_secret")
            user_agent: str = config.get("reddit", "user_agent")
        except configparser.NoSectionError:
            raise ValueError("The config file does not contain a reddit credential section.")
        except configparser.NoOptionError as e:
            raise ValueError(f"The config file is missing the option {e}")
        
        return praw.Reddit(
            user_agent = user_agent,
            client_id = client_id,
            client_secret = client_secret
        )


    def start_stream(self, subreddit_name) -> None:
        subreddit = self.reddit.subreddit(subreddit_name)
        comment: praw.models.Comment
        stock_comment_count = 0
        total_comment_count = 0
        
        for comment in subreddit.stream.comments(skip_existing=True):
            try:
                total_comment_count += 1
                
                # Skip deleted/removed comments
                if comment.body in ['[deleted]', '[removed]']:
                    continue
                
                # Detect stock tickers mentioned in the comment
                mentioned_tickers = detect_stock_tickers(comment.body)
                
                # Only process comments that mention stocks (filter noise)
                if mentioned_tickers:
                    # Optional: Filter by engagement (comment score) to focus on meaningful discussions
                    comment_score = comment.ups - comment.downs
                    
                    # Process all stock mentions, but highlight high-engagement ones
                    stock_comment_count += 1
                    
                    comment_json: dict[str, str] = {
                        "id": comment.id,
                        "name": comment.name,
                        "author": comment.author.name,
                        "body": comment.body,
                        "subreddit": comment.subreddit.display_name,
                        "upvotes": comment.ups,
                        "downvotes": comment.downs,
                        "over_18": comment.over_18,
                        "timestamp": comment.created_utc,
                        "permalink": comment.permalink,
                        "companies": mentioned_tickers,
                    }
                    
                    self.producer.send("redditcomments", value=comment_json)
                    
                    # Highlight high-engagement stock discussions
                    engagement_icon = "🔥" if comment_score >= 5 else "📈"
                    print(f"{engagement_icon} {subreddit_name}: {mentioned_tickers} | Score: {comment_score} | Text: {comment.body[:100]}...")
                
                # Print stats every 50 comments
                if total_comment_count % 50 == 0:
                    stock_ratio = (stock_comment_count / total_comment_count) * 100
                    print(f"📊 {subreddit_name} Stats: {stock_comment_count}/{total_comment_count} stock mentions ({stock_ratio:.1f}%)")
                    
            except Exception as e:
                print(f"❌ Error in {subreddit_name}:", str(e))
    
    def start_streaming_threads(self):
        for subreddit_name in self.subreddit_list:
            thread = threading.Thread(target=self.start_stream, args=(subreddit_name,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    reddit_producer = RedditProducer(financial_subreddit_list)
    reddit_producer.start_streaming_threads()
