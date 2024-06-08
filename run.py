from app import create_app


app = create_app()
print("hello")
# Maybe define routes here and load plugins after __name__ == __main__?
# app = setup_routes(app)

if __name__ == "__main__":
    # Initialize the database instance
    app.run(host="0.0.0.0", port=80, single_process=True)
