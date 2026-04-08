import os
import psycopg2
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Kubernetes will automatically inject these via ConfigMap & Secret!
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "ecommerce_db")
DB_USER = os.environ.get("DB_USER", "dbadmin")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/live-sales')
def live_sales():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query the live_conversions table that PySpark is writing to
        cur.execute("""
            SELECT item, COUNT(*) as total_sales, SUM(price) as total_revenue
            FROM live_conversions
            WHERE order_time > NOW() - INTERVAL '5 minutes'
            GROUP BY item
            ORDER BY total_revenue DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Format data for the frontend chart
        data = [{"item": row[0], "sales": row[1], "revenue": float(row[2])} for row in rows]
        return jsonify(data)
    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Waiting for PySpark to write data..."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)