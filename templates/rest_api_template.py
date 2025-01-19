import dlt
from dlt.sources.rest_api import rest_api_source

github_client = RESTClient(base_url="https://api.github.com")  # (1)

@dlt.resource
def get_issues():
    for page in github_client.paginate(
        "/repos/Datavault-UK/automate-dv/issues",
        params={
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        },
    ):
        yield page


pipeline = dlt.pipeline(
    pipeline_name="rest_api_pipeline",
    destination="postgres",
    dataset_name="rest_api_data",
)

load_info = pipeline.run(get_issues)

load_info = pipeline.run(get_issues)
print(load_info)