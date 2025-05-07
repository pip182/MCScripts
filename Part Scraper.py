import datetime
import subprocess
import os
import re
import json
import csv


sheet_name = 'MicrovellumData'
directory = "M:\Homestead_Library\Work Orders"


part_keys = [
    'Length', 'Width', 'Thickness', 'MaterialName', 'Name', 'Comments', 'MaterialThickness',
    'EdgeNameBottom', 'EdgeNameLeft', 'EdgeNameRight', 'EdgeNameTop', 'Quantity',
    'LinkID', 'LinkIDMaterial', 'LinkIDProduct',
]

subassembly_keys = [
    'Name', 'Width', 'Height', 'Depth', 'Quantity',
]

product_keys = [
    'Name', 'Depth', 'Height', 'Width', 'Comments1', 'ItemNumber', 'Quantity',
    'LinkID',
]

prompt_keys = [
    'Name', 'Value', 'LinkID', 'LinkIDProduct', 'LinkIDSubassembly',
]

prompt_names = [
    'Left_Reveal_Override', 'Right_Reveal_Override', 'Top_Gap_Override', 'Bottom_Gap_Override',
    'MaterialType', 'Bid_Item_ID',
]

sheet_keys = [
    'Name', 'Code', 'Comments', 'Quantity', 'LinkID', 'LinkIDWorkOrder', 'LinkIDMaterial',
    'Width', 'Length', 'LinkIDWorkOrderBatch', 'Type', 'UnitType'
]

hardware_keys = [
    'Name', 'Code', 'Comments', 'Width', 'Depth', 'Height', 'LinkIDWorkOrder', 'TotalQuantity',
    'LinkIDProduct', 'MaterialXData1', 'Type', 'UnitType'
]

edgebanding_keys = [
    'Name', 'Code', 'Comments', 'LinFt', 'LinkIDWorkOrder', 'LinkID', 'LinkIDMaterial',
    'LinkIDProduct', 'Type', 'UnitType'
]


def filter_keys(data, keys):
    return {k: v for k, v in data.items() if k in keys}


def find_sdf_files(directory, limit=None):
    sdf_files = []
    count = 0
    pattern = r"(?i)\(\d+-\d+\)*.*(?!purchasing)"
    for root, dirs, files in os.walk(directory):
        if limit and count >= limit:
            break
        for file in files:
            if file.endswith(".sdf") and file == "MicrovellumWorkOrder.sdf":
                work_order = os.path.split(root)[-1]
                if re.match(pattern, work_order) and "purchasing" not in root.lower():
                    sdf_files.append(os.path.join(root, file))
                    count += 1
    return sdf_files


def extract_code_from_path(path):
    """
    Extracts the code within parentheses from the path.
    For example, it will extract "(01-03)" from "Work Orders\\(01-03) 16710-Keller P2_Purchasing\\MicrovellumWorkOrder.sdf".
    """
    # Use a regular expression to find the pattern in parentheses
    match = re.search(r'\(\d{2}-\d{2}\)', path)
    if match:
        return match.group(0)  # Return the matched text, e.g., "(01-03)"
    return None


class WorkOrder:
    data = {}

    def __init__(self, file, separator='~'):
        p = os.path.split(file)

        # self.client = client
        self.first_batch_id = None
        self.date_created = datetime.datetime.fromtimestamp(os.path.getctime(p[0])).strftime('%Y-%m-%d %H:%M:%S')

        self.year_created = datetime.datetime.fromtimestamp(os.path.getctime(p[0])).strftime('%Y')

        self.date_id = extract_code_from_path(file)
        self.date_id = self.date_id.replace('(', '').replace(')', '') if self.date_id else None
        self.date_id = self.date_id + '-' + self.year_created if self.date_id else None

        self.full_path = file
        self.separator = separator

        regex = r"(\d{5})\s*(.*)"
        print(p)
        match = re.search(regex, p[0])
        if match:
            self.bid_id = int(match.group(1))
            name = match.group(2).strip()
            self.bid_name = re.search(r"(?i)-\s*(.*?)", name).group(1).strip()
        else:
            # If no match, extract the last part of the path
            self.bid_id = 0
            self.bid_name = p[0].split('\\')[-1]

        self.data['BidID'] = self.bid_id
        self.data['BidName'] = self.bid_name
        self.data['DateCreated'] = self.date_created

        batches = self.exec("SELECT * FROM WorkOrderBatches")
        self.batches = self.parse_results(batches, keys=['Name', 'LinkID', 'WorkOrderID'])
        if self.batches:
            self.first_batch_id = self.batches[0]['LinkID']

        self.runQuery("parts", f"SELECT {(',').join(part_keys)} FROM Parts")
        self.runQuery("subassemblies", f"SELECT * FROM Subassemblies WHERE Name LIKE '%Drawer%'")
        self.runQuery("sheets", f"SELECT {(',').join(sheet_keys)} FROM PlacedSheets WHERE LinkIDWorkOrderBatch = '{self.first_batch_id}'")
        # self.runQuery("prompts", f"SELECT {(',').join(prompt_keys)} FROM Prompts", filter_names=prompt_names)
        self.runQuery("hardware", f"SELECT {(',').join(hardware_keys)} FROM Hardware")
        self.runQuery("edgebanding", f"SELECT {(',').join(edgebanding_keys)} FROM Edgebanding")

    def parse_results(self, query_string, keys=None, filter_names=None):
        rows = []
        try:
            query_string = query_string.decode('utf-8')
        except Exception:
            query_string = query_string.decode('latin-1')

        data = query_string.replace('/', '_').replace('\n\n', '').splitlines()
        headers = data[0].split(self.separator)
        del data[:2]
        del data[len(data) - 2:]

        # Converts output to dictionary with headers as keys
        for index, row in enumerate(data):
            d = dict(zip(headers, data[index].split(self.separator)))
            if keys:
                d = filter_keys(d, keys)

            if filter_names:
                try:
                    if d['Name'] in filter_names:
                        rows.append(d)
                except Exception:
                    pass
            else:
                rows.append(d)
        return rows

    def exec(self, query):
        out = subprocess.run(f'SqlCeCmd40.exe -d "Data Source={self.full_path}" -q "{query}" -W -s "{self.separator}"', capture_output=True)
        if out.returncode:
            raise Exception(str(out.stderr))
        return out.stdout

    def runQuery(self, table_name, query, keys=None, filter_names=None):
        out = self.exec(query)
        rows = self.parse_results(out, keys=keys, filter_names=filter_names)

        for row in rows:
            row['BidID'] = self.bid_id
            row['BidName'] = self.bid_name
            row['Date Modified'] = self.date_created
            row['Date Processed'] = self.date_id

        if table_name:
            self.data[table_name] = rows

        return rows


if __name__ == "__main__":
    all_parts = []
    sdf_files = find_sdf_files(directory)

    print(f"Found {len(sdf_files)} work orders...")

    for file in sdf_files:
        # Get the directory of the .sdf file
        sdf_dir = os.path.dirname(file)
        # Check if this work order has already been processed
        json_file = os.path.join(sdf_dir, f'workorder_{os.path.basename(sdf_dir)}.json')
        if os.path.exists(json_file):
            print(f"Skipping {file} - already processed")
            continue

        print(f"Processing {file}...")
        wo = WorkOrder(file)
        # Write work order data to JSON file in the same directory as the .sdf
        with open(json_file, 'w') as f:
            json.dump(wo.data, f, indent=4)
