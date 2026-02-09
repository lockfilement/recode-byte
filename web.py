import threading
import os
import subprocess
import sys
from flask import Flask, render_template

app = Flask(__name__, template_folder="templates", static_folder="static")

@app.route("/")
def home():
    return render_template("index.html")


def start_bot():
    # Arranca tu programa real (NO el supervisor)
    subprocess.Popen([sys.executable, "main.py"])


if __name__ == "__main__":
    # Inicia el bot en segundo plano
    threading.Thread(target=start_bot, daemon=True).start()

    # Abre el servidor web para Render
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
