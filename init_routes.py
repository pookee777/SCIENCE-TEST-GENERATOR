from routes import routes_bp

def init_routes(app):
    app.register_blueprint(routes_bp)
    return app
