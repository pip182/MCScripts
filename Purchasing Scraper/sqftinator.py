import os
import datetime
import csv

root_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(root_dir, 'data')
data = []

data_structure = {
    "MDF Door Style Drawer Front": {
        'q': 13,
        'w': 14,
        'h': 15,
    },
    "MDF Door End Panels": {
        'q': 14,
        'w': 15,
        'h': 16,
    },
    "Cope & Stick Door": {
        'q': 18,
        'w': 19,
        'h': 20,
    },
    "Cope & Stick End Panel": {
        'q': 18,
        'w': 19,
        'h': 20,
    },
    "Cope & Stick Drawer Front": {
        'q': 19,
        'w': 20,
        'h': 21,
    },
    "Solid Slab Drawer Front": {
        'q': 9,
        'w': 10,
        'h': 11,
    },
}

with open('data.csv', 'w', newline='') as write_file:
    filew = csv.writer(write_file)
    filew.writerow(['DATE', 'NAME', 'QTY', 'WIDTH', 'HEIGHT'])

    with os.scandir(root_dir) as file:
        for entry in file:
            if entry.is_file():
                time = os.path.getmtime(entry)
                with open(entry, 'r') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        section = row[0]
                        if section in data_structure:
                            filew.writerow([
                                datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S'),
                                section,
                                row[data_structure[section]['q']],
                                row[data_structure[section]['w']],
                                row[data_structure[section]['h']]
                            ])
                        else:
                            print(f"Missing {section}")
