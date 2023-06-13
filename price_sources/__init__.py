def get_api_key(api_name):
    with open('../keys.txt', 'r') as f:
        for line in f:
            name, key = line.strip().split('=')
            if name == api_name:
                return key
    raise ValueError(f"No key found for API: {api_name}")