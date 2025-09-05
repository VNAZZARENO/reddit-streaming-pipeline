from flask import Flask, render_template, jsonify
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
import json
from datetime import datetime, timedelta

app = Flask(__name__)

# Global Cassandra session
cassandra_session = None

def init_cassandra():
    """Initialize Cassandra connection with proper error handling"""
    global cassandra_session
    try:
        cluster = Cluster(['localhost'])  # Connect to localhost (port-forwarded Cassandra)
        cluster.connection_class = LibevConnection  # Use libev for Python 3.12 compatibility
        session = cluster.connect()
        session.set_keyspace('reddit')
        cassandra_session = session
        print("✅ Cassandra connection successful!")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to Cassandra: {e}")
        cassandra_session = None
        return False

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/debug/tables')
def debug_tables():
    """Debug endpoint to see available tables and columns"""
    if not cassandra_session:
        return jsonify({'error': 'Database connection unavailable'}), 503
    
    try:
        # Query system tables to see what exists
        table_rows = cassandra_session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'reddit'")
        tables = [row.table_name for row in table_rows]
        
        # Get columns for each table
        table_info = {}
        for table in tables:
            column_rows = cassandra_session.execute(
                "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = 'reddit' AND table_name = %s",
                [table]
            )
            table_info[table] = [{'name': row.column_name, 'type': row.type} for row in column_rows]
        
        return jsonify({'tables': tables, 'table_info': table_info})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/subreddit_sentiment')
def subreddit_sentiment():
    """Get real sentiment data for financial subreddits"""
    if not cassandra_session:
        return jsonify({'error': 'Database connection unavailable'}), 503
    
    try:
        # Query real data from last 6 hours
        six_hours_ago = datetime.now() - timedelta(hours=6)
        
        query = """
        SELECT subreddit, sentiment_score_avg, ingest_timestamp
        FROM subreddit_sentiment_avg
        WHERE subreddit IN ('wallstreetbets', 'wallstreetbetsELITE', 'stocks', 'StockMarket', 'ValueInvesting', 'pennystocks')
        AND ingest_timestamp > %s
        ALLOW FILTERING
        """
        
        rows = cassandra_session.execute(query, [six_hours_ago])
        
        data = []
        for row in rows:
            data.append({
                'subreddit': row.subreddit,
                'sentiment': float(row.sentiment_score_avg) if row.sentiment_score_avg else 0,
                'timestamp': row.ingest_timestamp.isoformat() if row.ingest_timestamp else None
            })
        
        return jsonify(data)
        
    except Exception as e:
        print(f"❌ Error querying subreddit sentiment: {e}")
        return jsonify({'error': 'Failed to query data'}), 500

@app.route('/api/ticker_sentiment')
def ticker_sentiment():
    """Get real sentiment data for stock tickers"""
    if not cassandra_session:
        return jsonify({'error': 'Database connection unavailable'}), 503
    
    try:
        # First, let's check if the ticker_sentiment_avg table exists
        # If not, we'll extract ticker data from comments table instead
        
        # Try the ticker_sentiment_avg table first
        try:
            six_hours_ago = datetime.now() - timedelta(hours=6)
            query = """
            SELECT ticker, sentiment_score_avg, ingest_timestamp, subreddit
            FROM ticker_sentiment_avg
            WHERE ingest_timestamp > %s
            ALLOW FILTERING
            """
            rows = cassandra_session.execute(query, [six_hours_ago])
            
            data = []
            for row in rows:
                data.append({
                    'ticker': row.ticker,
                    'sentiment': float(row.sentiment_score_avg) if row.sentiment_score_avg else 0,
                    'timestamp': row.ingest_timestamp.isoformat() if row.ingest_timestamp else None,
                    'subreddit': row.subreddit
                })
            
            return jsonify(data)
            
        except Exception as table_error:
            # If ticker_sentiment_avg doesn't exist, extract from comments table
            print(f"⚠️ ticker_sentiment_avg table not found, extracting from comments: {table_error}")
            
            # Since companies column doesn't exist yet, return empty data for now
            print("⚠️ companies column not found in comments table, returning empty ticker data")
            return jsonify([])
        
    except Exception as e:
        print(f"❌ Error querying ticker sentiment: {e}")
        return jsonify({'error': 'Failed to query data'}), 500

@app.route('/api/recent_comments')
def recent_comments():
    """Get real recent comments with detected tickers"""
    if not cassandra_session:
        return jsonify({'error': 'Database connection unavailable'}), 503
    
    try:
        # Query real comments from last hour
        one_hour_ago = datetime.now() - timedelta(hours=1)
        
        query = """
        SELECT subreddit, body, sentiment_score, api_timestamp
        FROM comments
        WHERE api_timestamp > %s
        ALLOW FILTERING
        """
        
        rows = cassandra_session.execute(query, [one_hour_ago])
        
        data = []
        count = 0
        for row in rows:
            if count >= 50:  # Apply limit in Python since CQL LIMIT with ALLOW FILTERING has issues
                break
            # Simple ticker detection from comment body as temporary solution
            detected_tickers = []
            if row.body:
                import re
                # Find $TICKER patterns
                dollar_tickers = re.findall(r'\$([A-Z]{2,5})\b', row.body.upper())
                # Find standalone ticker patterns  
                word_tickers = re.findall(r'\b([A-Z]{2,5})\b', row.body.upper())
                # Common stock tickers to filter for
                common_tickers = {'AAPL', 'TSLA', 'GME', 'AMD', 'NVDA', 'SPY', 'QQQ', 'MSFT', 'AMZN', 'GOOGL', 'META'}
                all_found = set(dollar_tickers + word_tickers)
                detected_tickers = list(all_found.intersection(common_tickers))
            
            data.append({
                'subreddit': row.subreddit,
                'body': row.body[:200] + '...' if row.body and len(row.body) > 200 else row.body,
                'tickers': detected_tickers,  # Extract tickers from comment body
                'sentiment': float(row.sentiment_score) if row.sentiment_score else 0,
                'timestamp': row.api_timestamp.isoformat() if row.api_timestamp else None
            })
            count += 1
        
        return jsonify(data)
        
    except Exception as e:
        print(f"❌ Error querying recent comments: {e}")
        return jsonify({'error': 'Failed to query data'}), 500

if __name__ == '__main__':
    # Initialize Cassandra connection on startup
    print("🔄 Initializing Cassandra connection...")
    if init_cassandra():
        print("✅ Flask app starting with real database connection!")
    else:
        print("⚠️ Flask app starting without database connection. Check Cassandra port-forward.")
    
    app.run(host='0.0.0.0', port=5000, debug=True)