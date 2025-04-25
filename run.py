# app.py

from flask import Flask, request, jsonify
from datetime import datetime
import duckdb

app = Flask(__name__)
DATA_DIR = "parquet_data"

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
        con = duckdb.connect()

        if downsample <= 1:
            # Full resolution (per second)
            query = f"""
            SELECT isoTime, randomNumber
            FROM read_parquet('{DATA_DIR}/year=*/month=*/day=*/data.parquet')
            WHERE isoTime BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'
            ORDER BY isoTime
            """
        else:
            # Downsampled by N seconds
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


        result = con.execute(query).fetchall()
        formatted = [[row[0].isoformat(), row[1]] for row in result]
        return jsonify(formatted)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
