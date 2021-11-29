import sqlite3
import os
import matplotlib.pyplot as plt
import numpy as np
import unittest
import json
import requests
#hello

## Team Name: Women in Stem
## Team Members: Aimee Zheng, Sage Pei, Marina Sun

# Test google API
# Goal is to get all of our API info into JS format
def get_api1():
    response_API = requests.get('https://gmail.googleapis.com/$discovery/rest?version=v1')
    data = response_API.text
    parse_json = json.loads(data)
    info = parse_json['description']
    print("Info about API:\n", info)
    key = parse_json['parameters']['key']['description']
    print("\nDescription about the key:\n",key)
    pass

# idk just included in case we need it?
def readDataFromFile(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

def main():
    get_api1()

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
