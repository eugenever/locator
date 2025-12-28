# API локатора

Комментарии к полям:
- `timestamp` округляется до секунд или миллисекунд. Имеет дефолтное значение текущего времени.
- формат `mac` допускает использование твоеточия либо его отсутствие.
- `device_id` и `cell` являются не обязательными.
- `tac/lac`, `eci/ci/cid` имеют десятинчый формат.

## Сохранение отчетов

Эндпоинт для общего доступа (с авторизацией):

```
curl -X POST "http://127.0.0.1:8080/api/v1/report" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer axC7NmKh3NmQt5gvhNHEew2l8rKY9cGuUZsaq3b6WejVNzzk31J70yjlt2gOGYKz" \
-d \
'{
    "items": [
        {
            "timestamp": 1764679692190,
            "device_id": "868172073967398",
            "gnss": {
                "latitude": 56.011208,
                "longitude": 37.476509,
                "accuracy": 2.09
            },
            "wifi": [
                {
                    "mac": "5ca6e669e5ec",
                    "rssi": -81
                },
                {
                    "mac": "50ff20ec90d7",
                    "rssi": -73
                },
                {
                    "mac": "508811e8cc1a",
                    "rssi": -72
                },
                {
                    "mac": "5a8811e8cc1a",
                    "rssi": -71
                }
            ],
            "cell": {
                "lte": [
                    {
                        "mcc": 250,
                        "mnc": 2,
                        "tac": 5016,                
                        "eci": 40944044,
                        "rsrp": -67
                    }
                ]
            }
        }
    ]
}'
```

Эндпоинт для приложения `NeoStumbler` (внутреннего или ограниченного использования):

```
curl -X POST "http://127.0.0.1:8080/v2/geosubmit" \
-H "Content-Type: application/json" \
-d \
'{"items": [...]}'
```

## Локализация

```
curl -X POST "http://127.0.0.1:8080/api/v1/locate" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer axC7NmKh3NmQt5gvhNHEew2l8rKY9cGuUZsaq3b6WejVNzzk31J70yjlt2gOGYKz" \
-d \
'{
    "timestamp": 1764679692190,
    "device_id": "868172073967398",
    "gnss": {
        "latitude": 56.011208,
        "longitude": 37.476509,
        "accuracy": 2.09
    },
    "wifi": [
        {
            "mac": "30169df163d4",
            "rssi": -78
        },
        {
            "mac": "00026faaa397",
            "rssi": -76
        },
        {
            "mac": "bafbe474cc49",
            "rssi": -79
        },
        {
            "mac": "e8377a911fce",
            "rssi": -65
        }
    ],
    "cell": {        
        "lte": [
            {
                "mcc": 250,
                "mnc": 20,
                "tac": 27856,
                "eci": 205781280,
                "rsrp": -78
            }
        ]        
    }
}'
```