from flask import Flask, request, jsonify
from flask_caching import Cache
from datetime import datetime
import duckdb
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Setup Cache (In-memory cache)
app.config['CACHE_TYPE'] = 'simple'  # Use in-memory cache
app.config['CACHE_DEFAULT_TIMEOUT'] = 3600  # Cache timeout for 1 hour
cache = Cache(app)

DATA_DIR = "parquet_data"

# Cache query results
@cache.cached(timeout=3600, query_string=True)
def get_data_from_parquet(start, end, downsample):
    query = f"""
    SELECT 
        to_timestamp(
            FLOOR(EXTRACT(EPOCH FROM isoTime) / {downsample}) * {downsample}
        ) AS isoTime,
        avg(randomNumber)::INT AS randomNumber
    FROM read_parquet('{DATA_DIR}/year=*/month=*/day=*/data.parquet')
    WHERE isoTime BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'
    GROUP BY 1
    ORDER BY 1
    """
    
    # Open DuckDB and execute query
    con = duckdb.connect()
    result = con.execute(query).fetchall()
    con.close()

    return result

@app.route('/data', methods=['GET'])
def get_data():
    start_str = request.args.get('start')
    end_str = request.args.get('end')
    downsample_str = request.args.get('downsample', '1')  # default to full resolution

    if not start_str or not end_str:
        return jsonify({"error": "Missing 'start' or 'end' parameters"}), 400

    try:
        start = datetime.fromisoformat(start_str)
        end = datetime.fromisoformat(end_str)
        downsample = int(downsample_str)
    except Exception:
        return jsonify({"error": "Invalid input format"}), 400

    try:
        downsample = max(downsample, 1)

        # Unified query logic
        if downsample == 1:
            # Full resolution (per second)
            query = f"""
            SELECT isoTime, randomNumber
            FROM read_parquet('{DATA_DIR}/year=*/month=*/day=*/data.parquet')
            WHERE isoTime BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'
            ORDER BY isoTime
            """
            con = duckdb.connect()
            result = con.execute(query).fetchall()
            con.close()
            formatted = [[row[0].isoformat(), row[1]] for row in result]
            return jsonify(formatted)
        else:
            # Downsampled by N seconds
            result = get_data_from_parquet(start, end, downsample)
            formatted = [[row[0].isoformat(), row[1]] for row in result]
            return jsonify(formatted)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)