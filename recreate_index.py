import os

import requests
import yaml

# set cwd to the src directory to ensure relative paths work correctly
script_dir = os.path.dirname(os.path.realpath(__file__))
cwd = os.path.join(script_dir, "src")
os.chdir(cwd)
search_config_path = os.path.join(cwd, "instance", "search-config.yaml")

index_name = "senotypes"


def main():
    with open(search_config_path, "r") as f:
        search_config = yaml.safe_load(f)

    index_config = search_config["indices"].get(index_name)
    if not index_config:
        print(f"Index '{index_name}' not found in search configuration.")
        return

    es_url = index_config["elasticsearch"]["url"].strip("/")

    public_index = index_config.get("public")
    if public_index:
        print(f"Public index name: {public_index}")

        # does the public index exist in Elasticsearch? If so, delete it
        rspn = requests.head(f"{es_url}/{public_index}")
        if rspn.ok:
            print(f"Public index '{public_index}' exists. Deleting it.")
            requests.delete(f"{es_url}/{public_index}")
        else:
            print(f"Public index '{public_index}' does not exist. No need to delete.")

        # create the public index with the specified mappings and settings
        with open(index_config["elasticsearch"]["mappings"], "r") as f:
            index_settings = yaml.safe_load(f)
        print(
            f"Creating public index '{public_index}' with settings "
            f"from '{index_config['elasticsearch']['mappings']}'"
        )
        requests.put(f"{es_url}/{public_index}", json=index_settings)
    else:
        print("No public index specified in search configuration.")

    private_index = index_config.get("private")
    if private_index:
        print(f"Private index name: {private_index}")

        # does the private index exist in Elasticsearch? If so, delete it
        rspn = requests.head(f"{es_url}/{private_index}")
        if rspn.ok:
            print(f"Private index '{private_index}' exists. Deleting it.")
            requests.delete(f"{es_url}/{private_index}")
        else:
            print(f"Private index '{private_index}' does not exist. No need to delete.")

        # create the private index with the specified mappings and settings
        with open(index_config["elasticsearch"]["mappings"], "r") as f:
            index_settings = yaml.safe_load(f)
        print(
            f"Creating private index '{private_index}' with settings "
            f"from '{index_config['elasticsearch']['mappings']}'"
        )
        requests.put(f"{es_url}/{private_index}", json=index_settings)
    else:
        print("No private index specified in search configuration.")


if __name__ == "__main__":
    main()
