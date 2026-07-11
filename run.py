from flask import Flask
from auto_upload import auto_bp  # ← your file name without .py

app = Flask(__name__)

# Register the blueprint
app.register_blueprint(auto_bp)

if __name__ == "__main__":
    print("🚀 Auto YouTube Uploader running...")
    print("→ Health check: http://localhost:5000/auto/health")
    app.run(host="0.0.0.0", port=5000, debug=True)
