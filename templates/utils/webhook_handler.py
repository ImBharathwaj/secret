import dlt
from flask import Flask, request, abort
import json

app = Flask(__name__)

@app.route("/webhook", methods=['POST'])
def webhook():
    if request.method == 'POST':
        print(request.json)
        return json.dumps(request.json)
    else:
        abort(400)

if __name__ == '__main__':
    app.run()