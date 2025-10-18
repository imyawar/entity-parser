import json
import csv

from io import StringIO


class JSONMixin:
    @staticmethod
    def read_from_json_file(file_name):
        with open(file_name, 'r') as file:
            # Read the entire content of the file
            file_data = file.read()

        json_content = json.loads(file_data)
        return json_content

    @staticmethod
    def parse_json_to_csv(json_data, csv_columns):
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=csv_columns)
        writer.writeheader()
        for item in json_data:
            writer.writerow(item)
        return output.getvalue()
