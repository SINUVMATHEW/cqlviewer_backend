import os
from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required
from .routes.cassandra_routes import cassandra_routes
from dotenv import load_dotenv

load_dotenv()

def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
    jwt = JWTManager(app)
    @app.route('/protected', methods=['GET'])
    @jwt_required()
    def protected():
        return jsonify(message="You have access to this protected route")

    app.register_blueprint(cassandra_routes)
    return app
