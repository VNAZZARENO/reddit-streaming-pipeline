from flask import Flask, jsonify, render_template
from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import logging
import json
from collections import defaultdict, Counter
import traceback

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection
def get_cassandra_session():
    try:
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect()
        session.set_keyspace('reddit')
        return session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        return None

def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/stocks')
def get_stocks():
    """Get all stocks with recent sentiment data"""
    session = get_cassandra_session()
    if not session:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Get recent comments with companies mentioned (last hour)
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        
        query = """
        SELECT subreddit, body, sentiment_score, upvotes, downvotes, api_timestamp
        FROM comments 
        WHERE api_timestamp > ? 
        ALLOW FILTERING
        """
        
        rows = session.execute(query, [one_hour_ago])
        
        # Process results to extract stock mentions and calculate sentiment
        stock_data = defaultdict(lambda: {
            'mentions': 0,
            'sentiment_scores': [],
            'total_engagement': 0,
            'subreddits': Counter(),
            'latest_mention': None
        })
        
        for row in rows:
            # Extract potential stock tickers from body
            tickers = extract_tickers_from_text(row.body)
            
            if tickers:
                engagement = (row.upvotes or 0) - (row.downvotes or 0)
                sentiment = safe_float(row.sentiment_score)
                
                for ticker in tickers:
                    stock_data[ticker]['mentions'] += 1
                    stock_data[ticker]['sentiment_scores'].append(sentiment)
                    stock_data[ticker]['total_engagement'] += engagement
                    stock_data[ticker]['subreddits'][row.subreddit] += 1
                    stock_data[ticker]['latest_mention'] = row.api_timestamp.isoformat() if row.api_timestamp else None
        
        # Calculate averages and format response
        results = []
        for ticker, data in stock_data.items():
            if data['mentions'] >= 2:  # Only include stocks mentioned at least twice
                avg_sentiment = sum(data['sentiment_scores']) / len(data['sentiment_scores'])
                
                results.append({
                    'ticker': ticker,
                    'mentions': data['mentions'],
                    'avg_sentiment': round(avg_sentiment, 3),
                    'sentiment_label': get_sentiment_label(avg_sentiment),
                    'total_engagement': data['total_engagement'],
                    'top_subreddit': data['subreddits'].most_common(1)[0][0] if data['subreddits'] else 'unknown',
                    'subreddit_count': len(data['subreddits']),
                    'latest_mention': data['latest_mention']
                })
        
        # Sort by mentions descending
        results.sort(key=lambda x: x['mentions'], reverse=True)
        
        return jsonify({
            'stocks': results[:50],  # Top 50 most mentioned
            'total_count': len(results),
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_stocks: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500
    finally:
        if session:
            session.shutdown()

@app.route('/api/trending')
def get_trending():
    """Get trending stocks (most mentions in last hour)"""
    session = get_cassandra_session()
    if not session:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Get last hour of data
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        
        query = """
        SELECT body, sentiment_score, upvotes, downvotes
        FROM comments 
        WHERE api_timestamp > ? 
        ALLOW FILTERING
        """
        
        rows = session.execute(query, [one_hour_ago])
        
        trending_stocks = Counter()
        sentiment_by_stock = defaultdict(list)
        
        for row in rows:
            tickers = extract_tickers_from_text(row.body)
            sentiment = safe_float(row.sentiment_score)
            
            for ticker in tickers:
                trending_stocks[ticker] += 1
                sentiment_by_stock[ticker].append(sentiment)
        
        # Format trending results
        trending_results = []
        for ticker, count in trending_stocks.most_common(10):
            avg_sentiment = sum(sentiment_by_stock[ticker]) / len(sentiment_by_stock[ticker])
            trending_results.append({
                'ticker': ticker,
                'mentions_last_hour': count,
                'avg_sentiment': round(avg_sentiment, 3),
                'sentiment_label': get_sentiment_label(avg_sentiment)
            })
        
        return jsonify({
            'trending': trending_results,
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_trending: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500
    finally:
        if session:
            session.shutdown()

@app.route('/api/sentiment/live')
def get_live_sentiment():
    """Get real-time sentiment updates (last 5 minutes)"""
    session = get_cassandra_session()
    if not session:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Get last 5 minutes of data
        five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
        
        query = """
        SELECT subreddit, body, sentiment_score, api_timestamp
        FROM comments 
        WHERE api_timestamp > ? 
        ALLOW FILTERING
        """
        
        rows = session.execute(query, [five_minutes_ago])
        
        live_updates = []
        for row in rows:
            tickers = extract_tickers_from_text(row.body)
            if tickers:
                sentiment = safe_float(row.sentiment_score)
                live_updates.append({
                    'tickers': tickers,
                    'subreddit': row.subreddit,
                    'sentiment_score': sentiment,
                    'sentiment_label': get_sentiment_label(sentiment),
                    'timestamp': row.api_timestamp.isoformat() if row.api_timestamp else None,
                    'body_preview': row.body[:100] + '...' if len(row.body) > 100 else row.body
                })
        
        # Sort by timestamp descending
        live_updates.sort(key=lambda x: x['timestamp'] or '', reverse=True)
        
        return jsonify({
            'live_updates': live_updates[:20],  # Last 20 updates
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_live_sentiment: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500
    finally:
        if session:
            session.shutdown()

def extract_tickers_from_text(text):
    """Extract stock tickers from text using same logic as producer"""
    import re
    detected = []
    text_upper = text.upper()
    
    # Pattern 1: $TICKER (most reliable)
    dollar_tickers = re.findall(r'\$([A-Z]{1,5})', text_upper)
    detected.extend(dollar_tickers)
    
    # Pattern 2: Strong financial context patterns
    stock_context = re.findall(r'\b([A-Z]{1,5})\s+(?:stock|shares|equity|share)\b', text_upper)
    detected.extend(stock_context)
    
    # Pattern 3: Trading actions
    trading_actions = re.findall(r'\b(?:buy|sell|bought|sold|holding|holds|own|owns|long|short)\s+([A-Z]{1,5})\b', text_upper)
    detected.extend(trading_actions)
    
    # Pattern 4: Options patterns
    options_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:calls|puts|call|put|options|option)\b', text_upper)
    detected.extend(options_patterns)
    
    # Pattern 5: Price movement patterns
    price_patterns = re.findall(r'\b([A-Z]{1,5})\s+(?:up|down|mooning|moon|dipped|dip|pumped|dumped|rallied|crashed|soared|tanked)\b', text_upper)
    detected.extend(price_patterns)
    
    # Exclude common words
    exclude_words = {
        'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'WAS', 'GET', 'HAS',
        'CEO', 'CFO', 'USA', 'USD', 'SEC', 'FDA', 'WSB', 'DD', 'FUD', 'LOL', 'OMG'
    }
    
    # Remove duplicates and filter
    seen = set()
    unique_detected = []
    for ticker in detected:
        if ticker not in seen and ticker not in exclude_words and len(ticker) >= 1:
            seen.add(ticker)
            unique_detected.append(ticker)
    
    return unique_detected[:10]

def get_sentiment_label(score):
    """Convert sentiment score to human-readable label"""
    if score > 0.1:
        return 'Bullish' if score > 0.3 else 'Slightly Bullish'
    elif score < -0.1:
        return 'Bearish' if score < -0.3 else 'Slightly Bearish'
    else:
        return 'Neutral'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)