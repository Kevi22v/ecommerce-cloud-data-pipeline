from flask import Flask, render_template
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    """Connect to the local Postgres database."""
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_db",
        user="admin",
        password="password123",
        port="5432"
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