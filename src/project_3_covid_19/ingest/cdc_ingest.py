import httpx

def fetch_cdc_data(url, order_by, limit=10000, offset=0):
    params = {
        '$limit': limit,
        '$offset': offset,
        "$order": order_by,
    }
    response = httpx.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def fetch_all_cdc_data(url, order_by):
    all_data = []
    offset = 0
    while True:
        batch = fetch_cdc_data(url=url, order_by=order_by, offset=offset)
        if not batch:
            break
        all_data.extend(batch)
        print(f'Fetched {len(all_data)} records so far...')
        offset += len(batch)
    return all_data