import json

for i in range(0, 118678):
    text = {

      "endpoint": f"https://bo.nalog.ru/advanced-search/organizations/?allFieldsMatch=false&period=2020&page={i}",

      "method": "GET",


      "repeat_count": 1

    }
    with open("req_bo.txt", "a") as file:
        file.write(json.dumps(text) + ",")
