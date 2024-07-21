from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Database connection details
DB_HOST = 'paralleldots.cnwuw6uo4cb2.ap-south-1.rds.amazonaws.com'
DB_NAME = 'paralleldots'
DB_USER = 'postgres'
DB_PASS = 'e1vq4e3nPe53A59k8XGO'

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

@app.route('/payments', methods=['GET'])
def get_payments():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute('SELECT * FROM ecom_api.payments limit 100')
    payments = cursor.fetchall()
    
    cursor.close()
    conn.close()

    return jsonify(payments)

@app.route('/orders', methods=['GET'])
def get_orders():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute('SELECT * FROM ecom_api.orders limit 100')
    payments = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(payments)

@app.route('/reviews', methods=['GET'])
def get_reviews():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute('SELECT * FROM ecom_api.reviews limit 100')
    payments = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return jsonify(payments)

if __name__ == '__main__':
    app.run(debug=True)
