from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

global flatten_columns


def parse(schema: dict, prefix: str = None):

    s_type = schema.get('type')

    if s_type == 'array':
        _schema = schema.get('elementType')
        parse(schema=_schema, prefix=prefix)
        return

    fields = schema.get('fields')

    for field in fields:
        _type = field.get('type')
        _name = field.get('name') if prefix is None else prefix + '.' + field.get('name')

        if _type != 'struct' and not isinstance(_type, dict):
            flatten_columns.append(_name)

        elif isinstance(field.get('type'), dict):
            parse(schema=_type, prefix=_name)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[3]') \
        .appName('flatten_json').getOrCreate()

    flatten_columns = []

    # Nested sample data from https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html

    nested_json = {
        "id": "0001",
        "type": "donut",
        "name": "Cake",
        "ppu": 0.55,
        "batters":
            {
                "batter":
                    [
                        {"id": "1001", "type": "Regular"},
                        {"id": "1002", "type": "Chocolate"},
                        {"id": "1003", "type": "Blueberry"},
                        {"id": "1004", "type": "Devil's Food"}
                    ]
            },
        "topping":
            [
                {"id": "5001", "type": "None"},
                {"id": "5002", "type": "Glazed"},
                {"id": "5005", "type": "Sugar"},
                {"id": "5007", "type": "Powdered Sugar"},
                {"id": "5006", "type": "Chocolate with Sprinkles"},
                {"id": "5003", "type": "Chocolate"},
                {"id": "5004", "type": "Maple"}
            ]
    }

    _rdd = spark.sparkContext.parallelize([nested_json])
    df = spark.read.option('multiline', 'true').json(_rdd)

    parse(df.schema.jsonValue())
    columns = [col(f).alias(str(f).replace('.', '_')) for f in flatten_columns]

    df.select(columns).show()

