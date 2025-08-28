import requests
import csv
from time import sleep

# Tree ID you're querying
#tree_id = 1159760

fieldnames = [
    "id", "latitude","longitude", "soortnaam", "soortnaamtop", "jaarvanaanleg",
    "typeobject", "typeeigenaarplus", "typebeheerderplus", "boomhoogteklasseactueel"
]

def parseTree(tree_id):
    print(f"Retrieving data for {tree_id}")

    # API endpoint
    url = f"https://bomen.amsterdam.nl/features.data?type=tree&id={tree_id}&filters=&_routes=routes%2Ffeatures"

    # Fake browser headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://bomen.amsterdam.nl/",
    }

    # Fetch the data
    try:
        response = requests.get(url, headers=headers)
        data = response.json()

        # Helper function to find value after a keyword in the list
        def find_value(key):
            try:
                return data[data.index(key) + 1]
            except ValueError:
                return None

        # Extract desired fields
        tree_info = {
            "id": find_value("id"),
            "latitude": data[data.index('coordinates') + 2],
            "longitude": data[data.index('coordinates') + 3],
            "soortnaam": f"{find_value("soortnaam")}",
            "soortnaamtop": f"{find_value("soortnaamtop")}",
            "jaarvanaanleg": find_value("jaarvanaanleg"),
            "typeobject": f"{find_value("typeobject")}",
            "typeeigenaarplus": f"{find_value("typeeigenaarplus")}",
            "typebeheerderplus": f"{find_value("typebeheerderplus")}",
            "boomhoogteklasseactueel": f"{find_value("boomhoogteklasseactueel")}",
        }

        # Display results
        for k, v in tree_info.items():
            print(f"{k:25s}: {v}")

        return tree_info

    except:
        return None
        pass


with open("amsterdam_trees.csv", mode="w", newline='', encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for tree_id in range(919999, 1179200):
        try:
            tree_info = parseTree(tree_id)
            if tree_info: 
                writer.writerow(tree_info)
        except:
            print("Something went wrong!")
            pass
        sleep(0.1) 
