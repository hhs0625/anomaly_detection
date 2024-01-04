import requests
import time

url = "https://pru1l7lvi6.execute-api.ap-northeast-2.amazonaws.com/dev/register_alarm"

headers = {
    "x-api-key": "P1fr2EBx5z6s0AAbdd8aQ52hD82ZFnmD3fExsaUM",
    "Content-Type": "application/json"
}

data = {
    "thing": "NavigationThing",
    "property": "Heading",
    "ship_number": "HDGRC7F_W",
    "priority": "WARNING",
    "m_code": "MNALAB",
    "desc": "description",
    "desc_detail": "description detail",
    "res": "res",
    "alarm_level": "point",
    "eas_group": "model version",
    "device": "model name",
    "area": "HiPOM",
    "state": "A"
}

state = "A"
data["state"] = state

if state == "A":
    data["alarm_time"] = str(int(time.time()))
    # Save alarm time to alarm_time.txt in the same folder.
    with open("alarm_time.txt", "w") as file:
        file.write(str(data["alarm_time"]))
else:
    # Read alarm time from alarm_time.txt in the same folder.
    with open("alarm_time.txt", "r") as file:
        data["alarm_time"] = file.read()

    data["normal_time"] = str(int(time.time()))

response = requests.post(url, json=data, headers=headers)

print("Json:", data)
print("Response Code:", response.status_code)
print("Response Content:", response.json())
