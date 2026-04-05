import os
import psycopg2
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Read Cloud Variables
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

@app.route('/')
def index():
    """Fetch the latest orders and display them on the homepage."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the 15 most recent orders, sorted by time
        cursor.execute("SELECT order_id, user_id, item, price, is_anomaly_injected FROM orders ORDER BY timestamp DESC LIMIT 15;")
        orders = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return render_template('index.html', orders=orders)
    except Exception as e:
        return f"Database connection error: {e}"

if __name__ == '__main__':
    # Run the web server on port 5000
    app.run(host='0.0.0.0', port=5000, debug=True)