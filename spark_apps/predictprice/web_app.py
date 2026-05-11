from flask import Flask, render_template, request, jsonify
import sqlite3
import plotly
import plotly.express as px
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'priceprediction.db')
logger.info(f"Database path: {DB_PATH}")

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def get_product_prices(keyword):
    conn = get_db_connection()
    try:
        query = """
        WITH product_matches AS (
            SELECT 
                id, 
                url, 
                source, 
                name, 
                condition,
                price_yen as current_price_yen,
                price_vnd as current_price_vnd,
                created_at
            FROM product_catalog_items
            WHERE LOWER(name) LIKE LOWER(?)
        ),
        price_history AS (
            SELECT 
                item_id,
                price_yen,
                price_vnd,
                record_time
            FROM product_catalog_prices_history
        )
        SELECT 
            p.url,
            p.source,
            p.name,
            COALESCE(h.price_yen, p.current_price_yen) as price_yen,
            COALESCE(h.price_vnd, p.current_price_vnd) as price_vnd,
            p.condition,
            COALESCE(h.record_time, p.created_at) as created_at
        FROM product_matches p
        LEFT JOIN price_history h ON p.id = h.item_id
        ORDER BY COALESCE(h.record_time, p.created_at) DESC
        """
        
        search_term = f'%{keyword}%'
        
        logger.info(f"Searching with term: {search_term}")
        
        try:
            df = pd.read_sql_query(query, conn, params=(search_term,))
            logger.info(f"Query executed successfully")
            logger.info(f"Raw data sample:\n{df.head()}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        
        df = df.rename(columns={
            'name': 'product_name',
            'created_at': 'date',
            'price_vnd': 'price'
        })
        
        # Xử lý dữ liệu null
        df['price'] = df['price'].fillna(0)
        df['price_yen'] = df['price_yen'].fillna(0)
        df['date'] = pd.to_datetime(df['date'])
        df['condition'] = df['condition'].fillna('Unknown')
        
        logger.info(f"Processed data sample:\n{df.head()}")
        logger.info(f"Price statistics:\n{df[['price', 'price_yen']].describe()}")
        logger.info(f"Found {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error in get_product_prices: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()
    finally:
        conn.close()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    keyword = request.form.get('product_name', '').strip()
    if not keyword:
        return jsonify({'error': 'Please enter a keyword'})
    
    logger.info(f"Searching for: {keyword}")
    
    df = get_product_prices(keyword)
    if df.empty:
        return jsonify({'error': 'No data found for this keyword'})
    
    df = df.sort_values('date')
    
    latest_prices = df.groupby(['source', 'condition', 'product_name', 'url']).last().reset_index()
    
    fig = px.box(latest_prices, 
                 x='source',
                 y='price',
                 color='condition',
                 title=f'Price Comparison for "{keyword}"',
                 labels={
                     'price': 'Price (VND)',
                     'source': 'Source',
                     'condition': 'Condition'
                 },
                 hover_data=['product_name', 'url', 'price_yen'])
    
    fig.update_layout(
        xaxis_title="Source",
        yaxis_title="Price (VND)",
        boxmode='group',
        showlegend=True,
        legend_title="Condition",
        hovermode='closest',
        height=600
    )
    
    fig.update_xaxes(
        categoryorder='array',
        categoryarray=df['source'].unique()
    )
    
    fig.update_traces(
        hovertemplate="<br>".join([
            "Source: %{x}",
            "Price: %{y:,.0f} VND",
            "Price (Yen): %{customdata[2]:,.0f}",
            "Condition: %{customdata[0]}",
            "Product: %{customdata[1]}",
            "URL: %{customdata[2]}"
        ])
    )
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    price_by_source = {}
    for source in df['source'].unique():
        source_df = df[df['source'] == source]
        if not source_df.empty:
            price_by_source[source] = {
                'min': float(source_df['price'].min()),
                'max': float(source_df['price'].max()),
                'mean': float(source_df['price'].mean())
            }
    
    price_by_condition = {}
    for condition in df['condition'].unique():
        condition_df = df[df['condition'] == condition]
        if not condition_df.empty:
            price_by_condition[condition] = {
                'min': float(condition_df['price'].min()),
                'max': float(condition_df['price'].max()),
                'mean': float(condition_df['price'].mean())
            }
    
    stats = {
        'min_price': float(df['price'].min()),
        'max_price': float(df['price'].max()),
        'avg_price': float(df['price'].mean()),
        'total_records': len(df),
        'products': df[['product_name', 'price', 'price_yen', 'source', 'condition', 'url']].to_dict('records'),
        'price_by_source': price_by_source,
        'price_by_condition': price_by_condition
    }
    
    return jsonify({
        'box_graph': graphJSON,
        'summary': stats
    })

if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        exit(1)
    
    logger.info(f"Starting web app with database at {DB_PATH}")
    app.run(debug=True, host='0.0.0.0', port=5000) 