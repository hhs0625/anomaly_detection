# -*- coding: utf-8 -*-
# This is a sample Python script.
import datetime
import json
import requests

# Press Alt+Shift+X to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # �ּ�
    url = "https://ap-southeast-1.aws.data.mongodb-api.com/app/data-zxdhn/endpoint/data/v1/action/aggregate"

    # API ��û ���
    headers = {
        'Content-type': 'application/json',
        'Accept': 'text/plain',
        'apiKey': 'ipfQPuBNhC2wMB9VKM4ikgCQeoXJMtl4e06bf4WsMbXvGZbyTwKtd4IHInes8z1E'
    }

    # Thing, Property ����Ʈ
    data_list = [
        ["DG1Thing", "BrgDE_Temp1"]
        , ["DG1Thing", "BrgDE_Temp2"]
        , ["DG1Thing", "BrgDE_Temp3"]
        , ["DG1Thing", "BrgDE_Temp4"]
        , ["DG1Thing", "BrgDE_Temp5"]
        , ["DG1Thing", "BrgDE_Temp6"]
        , ["DG1Thing", "BrgDE_Temp7"]
        , ["DG1Thing", "BrgDE_Temp8"]
        , ["DG1Thing", "BrgDE_Temp9"]
        , ["DG1Thing", "BrgDE_Temp10"]
        , ["DG1Thing", "BrgDE_Temp11"]
        , ["DG1Thing", "BrgDE_Temp12"]
        , ["DE1Thing", "Cy1ExhGasOutletTemp"]
        , ["DE1Thing", "Cy2ExhGasOutletTemp"]
        , ["DE1Thing", "Cy3ExhGasOutletTemp"]
        , ["DE1Thing", "Cy4ExhGasOutletTemp"]
        , ["DE1Thing", "Cy5ExhGasOutletTemp"]
        , ["DE1Thing", "Cy6ExhGasOutletTemp"]
        , ["DE1Thing", "Cy7ExhGasOutletTemp"]
        , ["DE1Thing", "Cy8ExhGasOutletTemp"]
        , ["DE1Thing", "Cy9ExhGasOutletTemp"]
        , ["DE1Thing", "Cyl1_Pmax"]
        , ["DE1Thing", "Cy21_Pmax"]
        , ["DE1Thing", "Cy31_Pmax"]
        , ["DE1Thing", "Cy41_Pmax"]
        , ["DE1Thing", "Cy51_Pmax"]
        , ["DE1Thing", "Cy61_Pmax"]
        , ["DE1Thing", "Cy71_Pmax"]
        , ["DE1Thing", "Cy81_Pmax"]
        , ["DE1Thing", "Cy91_Pmax"]
        , ["DE1Thing", "Load"]
        , ["DE1Thing", "Power"]
    ]



    # �Է� ����
    ship_number = "HDGRC7F_W"
    from_date = datetime.datetime(2024, 1, 1, 12, 0, 0)
    to_date = datetime.datetime(2024, 1, 3, 18, 0, 0)
    _thing0, _property0 = data_list[0]      # ù��° ����
    _thing1, _property1 = data_list[1]      # �ι�° ����

    body = {
        "collection": "tag_datas",
        "database": "dev-hipom",
        "dataSource": "aiins-tv-test",
        "pipeline": [
            {
                "$match": {
                    "$and": [
                        {
                            "metadata.ship_number": {"$eq": ship_number}
                        },
                        {
                            "$or": [
                                {   # ù��° ����
                                    "$and": [
                                        {
                                            "metadata.thing": {"$eq": _thing0}
                                        },
                                        {
                                            "metadata.property": {"$eq": _property0}
                                        }
                                    ]
                                },
                                {   # �ι�° ����
                                    "$and": [
                                        {
                                            "metadata.thing": {"$eq": _thing1}
                                        },
                                        {
                                            "metadata.property": {"$eq": _property1}
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "timestamp": {
                                # "$gte": from_date.isoformat(),
                                # "$lt": to_date.isoformat()
                                "$gte": {"$date": from_date.isoformat() + ".000Z"},
                                "$lt": {"$date": to_date.isoformat() + ".000Z"}
                            }
                        }
                    ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "ship_number": "$metadata.ship_number",
                        "thing": "$metadata.thing",
                        "property": "$metadata.property",
                        "year": {"$year": "$timestamp"},
                        "month": {"$month": "$timestamp"},
                        "day": {"$dayOfMonth": "$timestamp"},
                        "hour": {"$hour": "$timestamp"}
                    },
                    "averageValue": {"$avg": "$value"}
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
            {
                "$group": {
                    "_id": {
                        "ship_number": "$_id.ship_number",
                        "thing": "$_id.thing",
                        "property": "$_id.property"
                    },
                    "items": {
                        "$push": {
                            "year": "$_id.year",
                            "month": "$_id.month",
                            "day": "$_id.day",
                            "hour": "$_id.hour",
                            "minute": "$_id.minute",
                            "second": "$_id.second",
                            "millisecond": "$_id.millisecond",
                            "value": "$averageValue"
                        }
                    }
                }
            }
        ]
    }

    # API ��
    response = requests.post(url, headers=headers, json=body)
    response_dict = response.json()
    response_string = json.dumps(response_dict)
    print("response = ", response_string)

    
    # CSV ����
    csv_file_name = "tag_datas_file.csv"
    with open(csv_file_name, "w") as file_stream:

        # ù��° ����
        date_items = ["ship_number", "thing", "property"]
        cursor_date = from_date
        while cursor_date <= to_date:
            # YYYY-MM-DD HH:MM:SS.MS
            date_items.append(cursor_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
            cursor_date += datetime.timedelta(hours=1)
        line_string = ",".join(date_items) + "\n"
        file_stream.write(line_string)

        documents_list = response_dict["documents"]
        for index, document_object in enumerate(documents_list):
            _id_object = document_object["_id"]
            _ship_number = _id_object["ship_number"]
            _thing = _id_object["thing"]
            _property = _id_object["property"]
            items_list = document_object["items"]

            cursor_date = from_date
            line_items = [_ship_number, _thing, _property]
            for idx, item_object in enumerate(items_list):
                _year = item_object["year"]
                _month = item_object["month"]
                _day = item_object["day"]
                _hour = item_object["hour"]
                _value = item_object["value"]

                # hour ���� �񱳸� ���� datetime���� ��ȯ
                _date = datetime.datetime(
                    _year, _month, _day,
                    _hour, 0, 0, 0,
                    from_date.tzinfo)

                while cursor_date <= _date:
                    # YYYY-MM-DD HH:MM:SS.MS
                    if (cursor_date.year == _date.year) \
                            and (cursor_date.month == _date.month) \
                            and (cursor_date.day == _date.day) \
                            and (cursor_date.hour == _date.hour):
                        # �����Ͱ� ���� ��, value ����
                        line_items.append(str(_value))
                    else:
                        # �����Ͱ� ���� ��, �� ����
                        line_items.append("")
                    cursor_date += datetime.timedelta(hours=1)
            line_string = ",".join(line_items)
            if index != (len(documents_list) - 1):
                line_string += "\n"
            file_stream.write(line_string)
    pass