import requests

json_data = {
    "flow_runs": {
        "state": {
            "name": {
                "any_": ["Late"],
            },
        },
        "start_time": {
            "after_": "2025-12-10T23:44:17.000Z",
        },
    },
    "sort": "START_TIME_DESC",
    "limit": 100,
    "page": 1,
}

while True:
    response = requests.post(
        "http://10.10.10.101:4200/api/flow_runs/paginate",
        json=json_data,
        verify=False,
    )

    print(response.status_code)
    data = response.json()
    if not data["results"]:
        break
    for run in data["results"]:
        print(run)
        response = requests.delete(
            f"http://10.10.10.101:4200/api/flow_runs/{run['id']}",
            verify=False,
        )
        print(run["id"], response.status_code)
