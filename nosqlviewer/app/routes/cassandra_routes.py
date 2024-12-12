from datetime import datetime
from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3
import csv
import os

cassandra_routes = Blueprint('cassandra_routes', __name__)

DB_FILE = 'nosql_viewer.db'


def init_databases():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create relations table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_keyspace TEXT,
            from_table TEXT,
            from_column TEXT,
            to_keyspace TEXT,
            to_table TEXT,
            to_column TEXT,
            is_published TEXT
        )
    ''')

    # Create users table
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT,
                password TEXT
            )
        ''')

    # Create table_description table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS table_description (
            keyspace_name TEXT,
            table_name TEXT,
            note TEXT,
            tag TEXT,
            PRIMARY KEY (keyspace_name, table_name)
        )
    ''')

    # Create columns table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS columns (
                        keyspace_name TEXT,
                        table_name TEXT,
                        column_name TEXT,
                        clustering_order TEXT,
                        column_name_bytes TEXT,
                        kind TEXT,
                        position INTEGER,
                        type TEXT,
                        note TEXT DEFAULT 'no note',
                        tag TEXT DEFAULT 'no tags',
                        status TEXT DEFAULT 'active',
                        PRIMARY KEY (keyspace_name, table_name, column_name)
                    )
                ''')
    cursor.execute('''
            INSERT OR IGNORE INTO table_description (keyspace_name, table_name, note, tag)
            SELECT DISTINCT keyspace_name, table_name, 'no note', 'no tag'
            FROM columns
        ''')

    # Create update_logs table
    cursor.execute('''
                CREATE TABLE IF NOT EXISTS update_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_name TEXT,
                    timestamp TEXT,
                    existing_data TEXT,
                    updated_data TEXT
                )
            ''')

    conn.commit()
    conn.close()


init_databases()


def query_database(query, params=(), fetchall=True):
    """Helper function to execute database queries."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, params)
    results = cur.fetchall() if fetchall else cur.fetchone()
    conn.commit()
    conn.close()
    return [dict(row) for row in results] if fetchall else dict(results) if results else None


@cassandra_routes.route('/api/upload-file', methods=['POST'])
def upload_file():
    try:
        # Check if the 'file' is in the request
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400

        file = request.files['file']

        # Check if a file is selected
        if file.filename == '':
            return jsonify({'error': 'No file selected for uploading'}), 400

        # Check if the file is a CSV
        if not file.filename.lower().endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400

        # Save the uploaded file temporarily
        uploads_dir = os.path.join(os.getcwd(), 'uploads')
        os.makedirs(uploads_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')
        file_extension = os.path.splitext(file.filename)[1]
        filename = f"{timestamp}{file_extension}"
        file_path = os.path.join(uploads_dir, filename)
        file.save(file_path)

        # Process the CSV file
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            csv_data = [row for row in csv_reader]

        # Update database with the CSV data
        process_csv_data(csv_data)

        # Return a success response
        return jsonify({'message': 'File processed and database updated successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def process_csv_data(csv_data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Ensure the `columns` table exists
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS columns (
                keyspace_name TEXT,
                table_name TEXT,
                column_name TEXT,
                clustering_order TEXT,
                column_name_bytes TEXT,
                kind TEXT,
                position INTEGER,
                type TEXT,
                note TEXT DEFAULT 'no note',
                tag TEXT DEFAULT 'no tags',
                status TEXT DEFAULT 'active',
                PRIMARY KEY (keyspace_name, table_name, column_name)
            )
        ''')
    except Exception as e:
        print(f"Error creating 'columns' table: {e}")
        conn.close()
        return

    # Fetch existing records from sqliteDB
    cursor.execute('SELECT keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, '
                   'position, type, note, tag, status FROM columns')
    db_rows = cursor.fetchall()
    db_data = {f"{row[0]}.{row[1]}.{row[2]}": row for row in db_rows}

    new_data_keys = set()
    for row in csv_data:
        try:
            keyspace_name = row.get('keyspace_name', '').strip()
            table_name = row.get('table_name', '').strip()
            column_name = row.get('column_name', '').strip()
            clustering_order = row.get('clustering_order', '').strip()
            column_name_bytes = row.get('column_name_bytes', '').strip()
            kind = row.get('kind', '').strip()
            position = row.get('position', '')
            try:
                position = int(position)
            except (ValueError, TypeError):
                position = 0
            type = row.get('type', '').strip()
            note = row.get('note', 'no note').strip() or 'no note'
            tag = row.get('tag', 'no tags').strip() or 'no tags'
            status = row.get('status', 'active').strip() or 'active'
            identifier = f"{keyspace_name}.{table_name}.{column_name}"

            if not keyspace_name or not table_name or not column_name:
                continue  # Skip invalid rows

            new_data_keys.add(identifier)
            if identifier in db_data:
                db_row = db_data[identifier]
                if (clustering_order != db_row[3] or
                        column_name_bytes != db_row[4] or
                        kind != db_row[5] or
                        position != db_row[6] or
                        type != db_row[7] or
                        note != db_row[8] or
                        tag != db_row[9] or
                        status != db_row[10]):
                    existing_data = str(db_row)
                    updated_data = str((keyspace_name, table_name, column_name, clustering_order, column_name_bytes,
                                        kind, position, type, note, tag, 'active'))
                    cursor.execute('''
                        UPDATE columns
                        SET clustering_order = ?, column_name_bytes = ?, kind = ?, position = ?, type = ?, note = ?, 
                        tag = ?, status = 'active'
                        WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
                    ''', (clustering_order, column_name_bytes, kind, position, type, note, tag, keyspace_name,
                          table_name, column_name))
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute(''' 
                                        INSERT INTO update_logs (user_name, timestamp, existing_data, updated_data)
                                        VALUES ('admin', ?, ?, ?)
                                    ''', (timestamp, existing_data, updated_data))

            else:
                existing_data = "None"
                updated_data = str((keyspace_name, table_name, column_name, clustering_order, column_name_bytes,
                                    kind, position, type, note, tag, 'active'))
                # Insert the new row into the columns table
                cursor.execute('''
                    INSERT INTO columns (keyspace_name, table_name, column_name, clustering_order, column_name_bytes, 
                    kind, position,type, note, tag, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
                ''', (keyspace_name, table_name, column_name, clustering_order, column_name_bytes,
                      kind, position, type, note, tag))

                # Log the insertion in the update_logs table
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(''' 
                                INSERT INTO update_logs (user_name, timestamp, existing_data, updated_data)
                                VALUES ('admin', ?, ?, ?)
                            ''', (timestamp, existing_data, updated_data))
        except Exception as e:
            print(f"Error processing row: {row}, Error: {str(e)}")
            continue

    # Mark missing rows as deleted
    for identifier, db_row in db_data.items():
        if identifier not in new_data_keys:
            cursor.execute('''
                UPDATE columns
                SET status = 'deleted'
                WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
            ''', (db_row[0], db_row[1], db_row[2]))
            existing_data = str(db_row)
            updated_data = "none"
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(''' 
            INSERT INTO update_logs (user_name, timestamp, existing_data, updated_data)
            VALUES ('admin', ?, ?, ?)
            ''', (timestamp, existing_data, updated_data))

    conn.commit()
    conn.close()


@cassandra_routes.route('/api/keyspace_names', methods=['GET'])
def get_distinct_keyspace_names():
    try:
        query = "SELECT DISTINCT keyspace_name FROM columns WHERE keyspace_name IS NOT NULL"
        result = query_database(query)
        distinct_keyspace_names = [row['keyspace_name'] for row in result]
        return jsonify(distinct_keyspace_names)
    except Exception as e:
        print(f"Error retrieving keyspace names: {e}")
        return jsonify({"error": "Failed to retrieve keyspace names"}), 500


@cassandra_routes.route('/api/table_names', methods=['GET'])
def get_distinct_table_names():
    keyspace_name = request.args.get('keyspace_name')
    if not keyspace_name:
        return jsonify({"error": "keyspace_name parameter is required"}), 400

    try:
        query = """
            SELECT DISTINCT table_name
            FROM columns
            WHERE keyspace_name = ? AND table_name IS NOT NULL
        """
        result = query_database(query, params=(keyspace_name,))
        distinct_table_names = [row['table_name'] for row in result]
        return jsonify(distinct_table_names)

    except Exception as e:
        # Log the error
        print(f"Error retrieving table names: {e}")
        return jsonify({"error": "Failed to retrieve table names"}), 500


@cassandra_routes.route('/api/get_columns', methods=['GET'])
def get_distinct_columns():
    keyspace_name = request.args.get('keyspace_name')
    table_name = request.args.get('table_name')
    if not keyspace_name or not table_name:
        return jsonify({"error": "Both keyspace_name and table_name parameters are required"}), 400
    try:
        query = """
            SELECT DISTINCT column_name, clustering_order, column_name_bytes, kind, position, type, note, tag
            FROM columns
            WHERE keyspace_name = ? AND table_name = ? AND column_name IS NOT NULL
        """
        result = query_database(query, params=(keyspace_name, table_name))
        if not result:
            return jsonify({"error": f"No data found for keyspace '{keyspace_name}' and table '{table_name}'"}), 404
        # Return as JSON
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@cassandra_routes.route('/api/update_column_tag', methods=['PUT'])
def update_column():
    data = request.get_json()
    keyspace_name = data.get('keyspace_name')
    table_name = data.get('table_name')
    column_name = data.get('column_name')
    note = data.get('note')
    tag = data.get('tag')
    if not all([keyspace_name, table_name, column_name, note, tag]):
        return jsonify({"error": "keyspace_name, table_name, column_name, tag, and note are required"}), 400
    try:
        check_query = """
            SELECT 1 FROM columns 
            WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
            LIMIT 1
        """
        result = query_database(check_query, params=(keyspace_name, table_name, column_name))
        if not result:
            return jsonify(
                {"error": "No matching row found for the provided keyspace_name, table_name, and column_name"}), 404
        update_query = """
            UPDATE columns 
            SET tag = ?, note = ?
            WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
        """
        # Execute the update query
        query_database(update_query, params=(tag, note, keyspace_name, table_name, column_name))

        # Return success message
        return jsonify({"message": "Record updated successfully"}), 200

    except Exception as e:
        # Handle unexpected errors
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@cassandra_routes.route('/api/get_table_description', methods=['GET'])
def get_table_description():
    keyspace_name = request.args.get('keyspace_name')
    table_name = request.args.get('table_name')

    if not keyspace_name or not table_name:
        return jsonify({"error": "keyspace_name and table_name parameter is required"}), 400
    try:
        query = ''' SELECT keyspace_name, table_name, tag, note
        FROM table_description
        WHERE keyspace_name = ? AND table_name = ?
        LIMIT 1
            '''
        result = query_database(query, params=(keyspace_name, table_name))
        if not result:
            return jsonify({"error": f"No data found for keyspace '{keyspace_name}' and table '{table_name}'"}), 404
        record = result[0] if isinstance(result, list) else result
        response = {
            "keyspace_name": record.get("keyspace_name"),
            "table_name": record.get("table_name"),
            "tag": record.get("tag"),
            "note": record.get("note")
        }
        return jsonify(response), 200
    except Exception as e:
        # Handle unexpected errors
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@cassandra_routes.route('/api/update_table_description', methods=['PUT'])
def update_table_description():
    data = request.get_json()
    keyspace_name = data.get('keyspace_name')
    table_name = data.get('table_name')
    tag = data.get('tag')
    note = data.get('note')
    if not all([keyspace_name, table_name, tag, note]):
        return jsonify({"error": "keyspace_name, table_name, tag, and note are required"}), 400
    try:
        check_query = """
            SELECT 1 FROM table_description
            WHERE keyspace_name = ? AND table_name = ?
            LIMIT 1
        """
        result = query_database(check_query, params=(keyspace_name, table_name))
        if not result:
            return jsonify(
                {"error": "No matching row found for the provided keyspace_name, table_name, "}), 404
        update_query = """
            UPDATE table_description 
            SET tag = ?, note = ?
            WHERE keyspace_name = ? AND table_name = ?
        """
        # Execute the update query
        query_database(update_query, params=(tag, note, keyspace_name, table_name))

        # Return success message
        return jsonify({"message": "Record updated successfully"}), 200

    except Exception as e:
        # Handle unexpected errors
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@cassandra_routes.route('/api/filtered_data', methods=['GET'])
def get_filter_data():
    # Get search filter from query parameters
    search_filter = request.args.get('search', default='', type=str)
    threshold = 0.5  # You can adjust the threshold value based on your needs.

    try:
        # If no search filter is provided, return the first 50 records
        if not search_filter:
            query = "SELECT * FROM columns LIMIT 50"
            data = query_database(query)
            return jsonify(data)

        # Apply the search filter on the text columns using a LIKE query
        search_filter_lower = search_filter.lower()
        query = """
            SELECT * FROM columns
            WHERE LOWER(column_name) LIKE ? 
               OR LOWER(note) LIKE ? 
               OR LOWER(tag) LIKE ?
        """
        params = (f"%{search_filter_lower}%", f"%{search_filter_lower}%", f"%{search_filter_lower}%")
        data = query_database(query, params=params)

        # Apply data type adjustments (e.g., stripping tags or handling None for notes)
        for record in data:
            record['note'] = None if record.get('note') is None else record['note']
            record['tag'] = record.get('tag', "").strip() if isinstance(record.get('tag'), str) else "no tags"

        # Return the filtered data
        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@cassandra_routes.route('/api/save_relation', methods=['POST'])
def save_relation():
    try:
        # Get JSON data from the request
        data = request.get_json()

        # Validate all required fields are present
        required_fields = ['from_keyspace', 'from_table', 'from_column', 'to_keyspace', 'to_table', 'to_column',
                           'is_published']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400

        # Extract values
        from_keyspace = data['from_keyspace']
        from_table = data['from_table']
        from_column = data['from_column']
        to_keyspace = data['to_keyspace']
        to_table = data['to_table']
        to_column = data['to_column']
        is_published = data['is_published']

        # Insert into the SQLite database
        query = '''
                  INSERT INTO relations (from_keyspace, from_table, from_column, to_keyspace, to_table, to_column, is_published)
                  VALUES (?, ?, ?, ?, ?, ?, ?)
              '''
        query_database(query, (from_keyspace, from_table, from_column, to_keyspace, to_table, to_column, is_published),
                       fetchall=False)
        return jsonify({'message': 'Relation saved successfully'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@cassandra_routes.route('/api/get_relations', methods=['GET'])
def get_relations():
    from_keyspace = request.args.get('from_keyspace')
    from_table = request.args.get('from_table')
    if not from_keyspace or not from_table:
        return jsonify({'error': 'Both from_keyspace and from_table are required parameters'}), 400
    try:
        with sqlite3.connect(DB_FILE) as connection:
            cursor = connection.cursor()
            query = """
            SELECT from_keyspace, from_table, from_column, to_keyspace, to_table, to_column, is_published
            FROM relations
            WHERE from_keyspace = ? AND from_table = ?
            """
            cursor.execute(query, (from_keyspace, from_table))
            rows = cursor.fetchall()

            if not rows:
                return jsonify({'message': 'No relations found for the given keyspace and table'}), 404
            relations = [
                {
                    'from_keyspace': row[0],
                    'from_table': row[1],
                    'from_column': row[2],
                    'to_keyspace': row[3],
                    'to_table': row[4],
                    'to_column': row[5],
                    'is_published': row[6]
                }
                for row in rows
            ]

            return jsonify(relations), 200

    except sqlite3.Error as e:
        return jsonify({'error': f'Database error: {e}'}), 500
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred: {e}'}), 500


@cassandra_routes.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data or not data.get('email') or not data.get('password'):
        return jsonify({"msg": "Email and password are required"}), 400
    email = data['email']
    password = data['password']
    query = "SELECT * FROM users WHERE user_name = ?"
    user = query_database(query, (email,), fetchall=False)
    if user is None:
        return jsonify({"msg": "Invalid email or password"}), 401
    stored_password = user['password']
    if not check_password_hash(stored_password, password):
        return jsonify({"msg": "Invalid email or password"}), 401
    access_token = create_access_token(identity=email)
    return jsonify(success=True, access_token=access_token), 200


@cassandra_routes.route('/api/register', methods=['POST'])
def register():
    data = request.get_json()

    if not data or not data.get('email') or not data.get('password'):
        return jsonify({"msg": "Email and password are required"}), 400

    email = data['email']
    password = data['password']
    hashed_password = generate_password_hash(password)
    query = "INSERT INTO users (user_name, password) VALUES (?, ?)"
    query_database(query, (email, hashed_password), fetchall=False)
    return jsonify({"msg": "User created successfully"}), 201

@cassandra_routes.route( '/api/db_data',methods=['GET'])
def get_all_db_data():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return jsonify({"error": f"Table '{table_name}' does not exist"}), 404
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        return jsonify({"table": table_name, "data": data}), 200
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()








