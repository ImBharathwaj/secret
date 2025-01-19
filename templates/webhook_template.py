import dlt
from utils.webhook_handler import webhook
from flask import Flask, request, abort
import json

app = Flask(__name__)

@dlt.resource
@app.route("/webhook", methods=['POST'])
def webhook():
    if request.method == 'POST':
        print(request.json)
        data = json.dumps(request.json)
        webhook_loader(data)
        return 200
    else:
        abort(400)

pipeline = dlt.pipeline(
    pipeline_name="webhook_pipeline",
    destination="postgres",
    dataset_name="webhook_data",
)

def webhook_loader(payload):
    load_info = pipeline.run(payload)
    print(load_info)

if __name__ == '__main__':
    app.run()