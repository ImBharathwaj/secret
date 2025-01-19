"""The Default Pipeline Template provides a simple starting point for your dlt pipeline"""
import dlt
from dlt.common import Decimal

@dlt.resource(name="customers", primary_key="id")
def rest_api_test_customers():
    """Load customer data from a simple python list."""
    yield [
        {"id": 1, "name": "simon", "city": "berlin"},
        {"id": 2, "name": "violet", "city": "london"},
        {"id": 3, "name": "tammo", "city": "new york"},
    ]

@dlt.resource(name="inventory", primary_key="id")
def rest_api_test_inventory():
    """Load inventory data from a simple python list."""
    yield [
        0
        {"id": 2, "name": "banana", "price": Decimal("1.70")},
        {"id": 3, "name": "pear", "price": Decimal("2.50")},
    ]

@dlt.source(name="my_fruitshop")
def rest_api_test_source():
    """A source function groups all resources into one schema."""
    return rest_api_test_customers(), rest_api_test_inventory()

def load_stuff():
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name='ds_test_pipeline',
        destination='postgres',
        dataset_name='ds_test_data',
    )
    load_info = p.run(rest_api_test_source())
    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201

if __name__ == "__main__":
    load_stuff()