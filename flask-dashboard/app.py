from flask import Flask, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta
import logging
import json
from collections import defaultdict, Counter
import traceback
import subprocess
import csv
import io

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_cassandra_direct(query):
    """Query Cassandra directly using kubectl exec and cqlsh"""
    try:
        # Validate query is not empty
        if not query or not query.strip():
            logger.error("Empty query provided")
            return []
        
        # Get cassandra pod name
        pod_result = subprocess.run(['kubectl', 'get', 'pods', '-n', 'redditpipeline', '-l', 'k8s.service=cassandra', '-o', 'jsonpath={.items[0].metadata.name}'], 
                                  capture_output=True, text=True, timeout=10)
        
        if pod_result.returncode != 0:
            logger.error("Failed to get Cassandra pod name")
            return []
            
        pod_name = pod_result.stdout.strip()
        if not pod_name:
            logger.error("No Cassandra pod found")
            return []
        
        # Clean the query - remove empty statements
        statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
        
        if not statements:
            logger.error("No valid statements found in query")
            return []
        
        # First execute USE statement if present
        if statements and statements[0].upper().startswith('USE'):
            # For USE statement, we switch keyspace in connection
            if len(statements) > 1:
                # Execute the main query in the reddit keyspace
                main_query = statements[1]
                if not main_query.strip():
                    logger.error("Empty main query after USE statement")
                    return []
                cmd = ['kubectl', 'exec', pod_name, '-n', 'redditpipeline', '-c', 'cassandra', '--', 'cqlsh', '-k', 'reddit', '-e', main_query]
            else:
                logger.error("USE statement found but no main query")
                return []
        else:
            # Execute the full query as is with reddit keyspace
            final_query = query.strip()
            if not final_query:
                logger.error("Final query is empty after cleaning")
                return []
            cmd = ['kubectl', 'exec', pod_name, '-n', 'redditpipeline', '-c', 'cassandra', '--', 'cqlsh', '-k', 'reddit', '-e', final_query]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            logger.error(f"CQL query failed: {result.stderr}")
            return []
        
        # Parse the output
        lines = result.stdout.strip().split('\n')
        data_lines = []
        start_parsing = False
        
        for line in lines:
            if '---' in line:  # Header separator in cqlsh output
                start_parsing = True
                continue
            if start_parsing and line.strip() and '(' not in line and 'rows)' not in line:
                # Skip empty lines and result count lines
                if line.strip():
                    data_lines.append(line.strip())
        
        return data_lines
        
    except Exception as e:
        logger.error(f"Error querying Cassandra: {e}")
        return []

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

@app.route('/api/debug/subreddits')
def get_subreddits():
    """Debug endpoint to see which subreddits are actually being fetched"""
    try:
        # Query to see which subreddits have data
        query = "USE reddit; SELECT DISTINCT subreddit, COUNT(*) as comment_count FROM comments GROUP BY subreddit;"
        
        data_lines = query_cassandra_direct(query)
        
        subreddits = []
        for line in data_lines:
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 2:
                    subreddit, count_str = parts[0], parts[1]
                    count = int(count_str.strip()) if count_str.strip().isdigit() else 0
                    subreddits.append({
                        'subreddit': subreddit,
                        'comment_count': count
                    })
            except:
                continue
        
        # Also get recent comments to see what's actually being processed
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        timestamp_str = one_hour_ago.strftime('%Y-%m-%d %H:%M:%S+0000')
        
        recent_query = f"USE reddit; SELECT subreddit, body, api_timestamp FROM comments WHERE api_timestamp > '{timestamp_str}' LIMIT 10 ALLOW FILTERING;"
        
        recent_lines = query_cassandra_direct(recent_query)
        recent_comments = []
        
        for line in recent_lines:
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:
                    subreddit, body, timestamp = parts[:3]
                    recent_comments.append({
                        'subreddit': subreddit,
                        'body': body[:100] + '...' if len(body) > 100 else body,
                        'timestamp': timestamp
                    })
            except:
                continue
        
        return jsonify({
            'all_subreddits': sorted(subreddits, key=lambda x: x['comment_count'], reverse=True),
            'recent_comments_sample': recent_comments,
            'total_subreddits': len(subreddits),
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_subreddits: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stocks')
def get_stocks():
    """Get all stocks with recent sentiment data from REAL Reddit data"""
    try:
        # Query REAL data from Cassandra - last 2 hours
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        timestamp_str = two_hours_ago.strftime('%Y-%m-%d %H:%M:%S+0000')
        
        query = f"USE reddit; SELECT subreddit, body, sentiment_score, upvotes, downvotes, api_timestamp FROM comments WHERE api_timestamp > '{timestamp_str}' ALLOW FILTERING;"
        
        data_lines = query_cassandra_direct(query)
        
        # Process REAL Reddit data to extract stock mentions
        stock_data = defaultdict(lambda: {
            'mentions': 0,
            'sentiment_scores': [],
            'total_engagement': 0,
            'subreddits': Counter(),
            'latest_mention': None
        })
        
        for line in data_lines:
            try:
                # Parse the line (format: subreddit | body | sentiment_score | upvotes | downvotes | timestamp)
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 6:
                    subreddit, body, sentiment_str, upvotes_str, downvotes_str, timestamp_str = parts[:6]
                    
                    # Extract tickers from REAL Reddit comment
                    tickers = extract_tickers_from_text(body)
                    
                    if tickers:
                        sentiment = safe_float(sentiment_str.strip())
                        upvotes = int(upvotes_str.strip()) if upvotes_str.strip().isdigit() else 0
                        downvotes = int(downvotes_str.strip()) if downvotes_str.strip().isdigit() else 0
                        engagement = upvotes - downvotes
                        
                        for ticker in tickers:
                            stock_data[ticker]['mentions'] += 1
                            stock_data[ticker]['sentiment_scores'].append(sentiment)
                            stock_data[ticker]['total_engagement'] += engagement
                            stock_data[ticker]['subreddits'][subreddit] += 1
                            stock_data[ticker]['latest_mention'] = timestamp_str
                            
            except Exception as parse_error:
                logger.warning(f"Error parsing line: {parse_error}")
                continue
        
        # Calculate averages from REAL data
        results = []
        for ticker, data in stock_data.items():
            if data['mentions'] >= 1:  # Include any stock mentioned at least once
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
        
        logger.info(f"Found {len(results)} stocks with real Reddit mentions")
        
        return jsonify({
            'stocks': results[:50],
            'total_count': len(results),
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_stocks: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trending')
def get_trending():
    """Get trending stocks from REAL Reddit data (most mentions in last hour)"""
    try:
        # Query REAL data from last hour
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        timestamp_str = one_hour_ago.strftime('%Y-%m-%d %H:%M:%S+0000')
        
        query = f"USE reddit; SELECT body, sentiment_score FROM comments WHERE api_timestamp > '{timestamp_str}' ALLOW FILTERING;"
        
        data_lines = query_cassandra_direct(query)
        
        trending_stocks = Counter()
        sentiment_by_stock = defaultdict(list)
        
        for line in data_lines:
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 2:
                    body, sentiment_str = parts[0], parts[1]
                    
                    tickers = extract_tickers_from_text(body)
                    sentiment = safe_float(sentiment_str.strip())
                    
                    for ticker in tickers:
                        trending_stocks[ticker] += 1
                        sentiment_by_stock[ticker].append(sentiment)
                        
            except Exception as parse_error:
                continue
        
        # Format REAL trending results
        trending_results = []
        for ticker, count in trending_stocks.most_common(10):
            if sentiment_by_stock[ticker]:  # Make sure we have sentiment data
                avg_sentiment = sum(sentiment_by_stock[ticker]) / len(sentiment_by_stock[ticker])
                trending_results.append({
                    'ticker': ticker,
                    'mentions_last_hour': count,
                    'avg_sentiment': round(avg_sentiment, 3),
                    'sentiment_label': get_sentiment_label(avg_sentiment)
                })
        
        logger.info(f"Found {len(trending_results)} trending stocks from real Reddit data")
        
        return jsonify({
            'trending': trending_results,
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_trending: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sentiment/live')
def get_live_sentiment():
    """Get REAL-TIME sentiment updates from Reddit (last 10 minutes)"""
    try:
        # Query REAL data from last 10 minutes
        ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
        timestamp_str = ten_minutes_ago.strftime('%Y-%m-%d %H:%M:%S+0000')
        
        query = f"USE reddit; SELECT subreddit, body, sentiment_score, api_timestamp, permalink FROM comments WHERE api_timestamp > '{timestamp_str}' ALLOW FILTERING;"
        
        data_lines = query_cassandra_direct(query)
        
        live_updates = []
        for line in data_lines:
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 5:
                    subreddit, body, sentiment_str, timestamp_str, permalink = parts[:5]
                    
                    tickers = extract_tickers_from_text(body)
                    if tickers:
                        sentiment = safe_float(sentiment_str.strip())
                        
                        live_updates.append({
                            'tickers': tickers,
                            'subreddit': subreddit,
                            'sentiment_score': sentiment,
                            'sentiment_label': get_sentiment_label(sentiment),
                            'timestamp': timestamp_str,
                            'body_preview': body[:100] + '...' if len(body) > 100 else body,
                            'reddit_url': f"https://reddit.com{permalink}"
                        })
                        
            except Exception as parse_error:
                continue
        
        # Sort by timestamp descending (most recent first)
        live_updates.sort(key=lambda x: x['timestamp'] or '', reverse=True)
        
        logger.info(f"Found {len(live_updates)} real-time stock mentions from Reddit")
        
        return jsonify({
            'live_updates': live_updates[:20],  # Last 20 real updates
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_live_sentiment: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

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

@app.route('/api/sentiment/chart')
def get_sentiment_chart_data():
    """Get time-series sentiment data for charting"""
    try:
        # Query REAL data from last 2 hours for chart
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        timestamp_str = two_hours_ago.strftime('%Y-%m-%d %H:%M:%S+0000')
        
        query = f"USE reddit; SELECT subreddit, body, sentiment_score, api_timestamp, permalink FROM comments WHERE api_timestamp > '{timestamp_str}' ALLOW FILTERING;"
        
        data_lines = query_cassandra_direct(query)
        
        chart_data = []
        for line in data_lines:
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 5:
                    subreddit, body, sentiment_str, timestamp_str, permalink = parts[:5]
                    
                    tickers = extract_tickers_from_text(body)
                    if tickers:
                        sentiment = safe_float(sentiment_str.strip())
                        
                        # Convert timestamp for chart
                        from datetime import datetime
                        timestamp_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f+0000')
                        
                        for ticker in tickers:
                            chart_data.append({
                                'ticker': ticker,
                                'timestamp': timestamp_dt.isoformat(),
                                'sentiment_score': sentiment,
                                'subreddit': subreddit,
                                'reddit_url': f"https://reddit.com{permalink}",
                                'body_preview': body[:50] + '...' if len(body) > 50 else body
                            })
                        
            except Exception as parse_error:
                continue
        
        # Sort by timestamp
        chart_data.sort(key=lambda x: x['timestamp'])
        
        logger.info(f"Found {len(chart_data)} data points for chart")
        
        return jsonify({
            'chart_data': chart_data,
            'last_updated': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in get_sentiment_chart_data: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

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