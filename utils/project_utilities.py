from config import CONFIG


def get_key(key_name):
    with open(CONFIG["root_path"] + "/keys.txt", "r") as f:
        for line in f:
            name, key = line.strip().split("=")
            if name == key_name:
                return key
    raise ValueError(f"No key found for key named: {key_name}")
