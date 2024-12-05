from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required
from .routes.cassandra_routes import cassandra_routes


def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config['JWT_SECRET_KEY'] = 'your_jwt_secret_key your_jwt_secret_key'  
    jwt = JWTManager(app)
    @app.route('/protected', methods=['GET'])
    @jwt_required()
    def protected():
        return jsonify(message="You have access to this protected route")

    app.register_blueprint(cassandra_routes)

    return app
