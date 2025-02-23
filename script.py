# pylint: disable=missing-module-docstring
# type: ignore[missing-imports]

import csv

import requests

RESOURCE_KEY = "<<INSERT YOUR RESOURCE KEY HERE>>"

payload = {
    "version": "1.0.0",
    "queries": [
        {
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {"Name": "t1", "Entity": "Tarefas", "Type": 0}
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "t1"}
                                            },
                                            "Property": "Progresso",
                                        },
                                        "Name": "Tarefas.Progresso",
                                        "NativeReferenceName": "Progresso",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "t1"}
                                            },
                                            "Property": "Nome da tarefa",
                                        },
                                        "Name": "Tarefas.Nome da tarefa",
                                        "NativeReferenceName": "Nome da tarefa",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "t1"}
                                            },
                                            "Property": "Descrição",
                                        },
                                        "Name": "Tarefas.Descrição",
                                        "NativeReferenceName": "Descrição",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "t1"}
                                            },
                                            "Property": "Rótulos",
                                        },
                                        "Name": "Tarefas.Rótulos",
                                        "NativeReferenceName": "Rótulos",
                                    },
                                ],
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {"Projections": [0, 3, 1, 2], "Subtotal": 1}
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                            "ExecutionMetricsKind": 1,
                        }
                    }
                ]
            },
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "3335a7d0-20f8-4d92-847e-bbd63a204578",
                "Sources": [
                    {
                        "ReportId": "faf52e3f-9332-4c70-bfa8-7204083f3b03",
                        "VisualId": "03343ae8760525f873cd",
                    }
                ],
            },
        }
    ],
    "cancelQueries": [],
    "modelId": 5226838,
}


response = requests.request(
    "POST",
    "https://wabi-brazil-south-b-primary-api.analysis.windows.net/public/reports/querydata",
    timeout=60,
    json=payload,
    headers={
        "Accept": "application/json, text/plain, */*",
        "X-PowerBI-ResourceKey": RESOURCE_KEY,
    },
    params={"synchronous": "true"},
)

response_body = response.json()
data = response_body["results"][0]["result"]["data"]

descriptors = data["descriptor"]["Select"]
ds = data["dsr"]["DS"][0]
dm0 = ds["PH"][0]["DM0"]
value_dicts = ds["ValueDicts"]
d0 = value_dicts["D0"]
d1 = value_dicts["D1"]
d2 = value_dicts["D2"]
d3 = value_dicts["D3"]

previous_row: list[str] = []

output = []
for idx, row in enumerate(dm0):
    row_values = row.get("C", [])
    if len(row_values) == 0 and len(previous_row) == 0:
        print("Skipping index", idx, "due to empty columns list and no previous values")
        continue

    current_row = ["", "", "", ""]
    cidx = 0  # pylint: disable=invalid-name
    use_previous = "0000{0:b}".format(  # pylint: disable=consider-using-f-string
        row.get("R", 0)
    )[-4:][::-1]

    for scidx, sc in enumerate(use_previous):
        if sc == "0":  # new value
            if isinstance(row_values[cidx], str):
                current_row[scidx] = row_values[cidx]
            elif isinstance(row_values[cidx], int):
                if scidx == 0:
                    current_row[scidx] = d0[row_values[cidx]]
                if scidx == 1:
                    current_row[scidx] = d1[row_values[cidx]]
                if scidx == 2:
                    current_row[scidx] = d2[row_values[cidx]]
                if scidx == 3:
                    current_row[scidx] = d3[row_values[cidx]]
            else:
                raise Exception(  # pylint: disable=broad-exception-raised
                    "Invalid data type"
                )

            cidx += 1
        if sc == "1":  # old value
            current_row[scidx] = previous_row[scidx]

    output.append(
        {
            "progresso": current_row[0],
            "rotulos": current_row[1],
            "nome_tarefa": current_row[2],
            "descricao": current_row[3],
        }
    )

    previous_row = current_row

with open("data/output.csv", "w", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file, fieldnames=["progresso", "rotulos", "nome_tarefa", "descricao"]
    )
    writer.writeheader()
    writer.writerows(output)
