from pymongo import MongoClient
import os
import json


def process_json_files(inputdir, outputdir):
    for filename in os.listdir(inputdir):
        if filename.endswith('.json'):
            file_path = os.path.join(inputdir, filename)
            with open(file_path, 'r', encoding='utf-8-sig') as file:  # Use utf-8-sig to handle BOM
                file_content = file.read()
                try:
                    # Load JSON data from file
                    data = json.loads(file_content)
                    processed_data = []
                    for item in data:
                        # Extract the alias and convert it into an object
                        alias_key = next(iter(item.keys()))  # Get the first key (alias)
                        alias_data = item[alias_key]
                        processed_data.append(alias_data)
                    # Save processed data back to the file
                    output_file_path = os.path.join(outputdir, filename)
                    with open(output_file_path, 'w') as newfile:
                        json.dump(processed_data, newfile, indent=2)
                except json.JSONDecodeError as error:
                    print(f"Error: {error}")

def import_json_to_mongodb(host, port, db_name):
    client = MongoClient(f"mongodb://{host}:{port}/")
    client.drop_database(db_name)
    db = client[db_name]
    files = []
    base_path = 'world'
    data =[]
    for i in os.listdir ( base_path ):
        print(i)
        if i.endswith ( '.json' ):
            full_path = '%s/%s' % (base_path, i)
            collection = full_path.split ( '.' )[0].split ( '/' )[1]
            os.system (
                f'mongoimport --host {host} -d {db_name} --port {port} --collection {collection} --file {full_path} --jsonArray' )
            # data = [json.loads ( line ) for line in open ( full_path ,'r' ,encoding='utf8')]
        # print(collection +str(len(data)))


process_json_files('rawdata','world')

import_json_to_mongodb('localhost', 27017, 'world2')