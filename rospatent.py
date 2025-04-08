import json

for i in range(0, 2900000):
    text = {

      "endpoint": f"https://www.fips.ru/registers-doc-view/fips_servlet?DB=RUPAT&rn=7167&DocNumber={i}&TypeFile=html",

      "method": "GET",


      "repeat_count": 1

    }
    with open("req_rospatent.txt", "a") as file:
        file.write(f"{json.dumps(text)},\n")
