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

        # Main chart data
        cur.execute("""
            SELECT item, COUNT(*) as total_sales, SUM(price) as total_revenue
            FROM live_conversions
            WHERE order_time > NOW() - INTERVAL '15 minutes'
            GROUP BY item
            ORDER BY total_revenue DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()

        # KPI metrics
        cur.execute("""
            SELECT 
                COUNT(*) as total_orders,
                COALESCE(SUM(price),0) as total_revenue,
                COALESCE(AVG(price),0) as avg_order_value
            FROM live_conversions
            WHERE order_time > NOW() - INTERVAL '15 minutes';
        """)
        stats = cur.fetchone()

        cur.close()
        conn.close()

        return jsonify({
            "chart": [{"item": r[0], "sales": r[1], "revenue": float(r[2])} for r in rows],
            "stats": {
                "orders": stats[0],
                "revenue": float(stats[1]),
                "avg_order": float(stats[2])
            }
        })

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Waiting for data..."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)