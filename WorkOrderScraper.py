import gspread
import gspread_formatting
import datetime
import subprocess
import os
import re
import pyodbc
import json
import csv
from pathlib import Path


GOOGLE_SHEETS_CREDENTIALS = {

}

server = 'SERVER2019\\HSSQL'
database = 'MicrovellumData'
username = 'sa'
password = 'H0m35te@d12!'

sheet_name = 'MicrovellumData'
directory = "M:\Homestead_Library\Work Orders"

# station_LinkID = '0816ab46-ad71-4fe2-804b-c804120d3a7f'       # Weeke - Standard
station_LinkID = '5760d3cd-7ef0-40f8-8d7f-ea56e6a19770'         # Purchasing station


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


csv_dumps = []


def filter_keys(data, keys):
    return {k: v for k, v in data.items() if k in keys}


def get_or_create_sheet(client, tab, index=None, **kwargs):
    clear = kwargs.get('clear', False)
    # Check if the sheet already exists
    try:
        sheet = client.open(sheet_name).worksheet(tab)
    except Exception:
        # If the sheet doesn't exist, create a new one
        sheet = client.open(sheet_name).add_worksheet(title=tab, rows=100, cols=20, index=index)

    if clear:
        sheet.clear()
    return sheet


def write_data(client, data, tab, **kwargs):
    append = kwargs.get('append', False)
    index = kwargs.get('index', None)
    include_nested = kwargs.get('include_nested', False)
    exclude_keys = kwargs.get('exclude_keys', [])
    key_tab_map = kwargs.get('key_tab_map', {})
    tab_data = {}

    # Create and clear out tabs
    if key_tab_map:
        for v in key_tab_map.values():
            get_or_create_sheet(client, v, clear=not append)

    sheet = get_or_create_sheet(client, tab, index, clear=not append)

    currency_format = gspread_formatting.cellFormat(
        numberFormat={
            'type': 'CURRENCY',
            'pattern': '"$"#,##0.00'
        }
    )

    number_format = gspread_formatting.cellFormat(
        numberFormat={
            'type': 'NUMBER',
            'pattern': '###0.00'
        }
    )
    date_format = gspread_formatting.cellFormat(
        numberFormat={
            'type': 'DATE',
            'pattern': 'yyyy-mm-dd'
        }
    )
    percentage_format = gspread_formatting.cellFormat(
        numberFormat={
            'type': 'PERCENT',
            'pattern': '0%'
        }
    )

    rows = []

    # If dictionary is passed in, convert to list
    if isinstance(data, dict):
        data = [data]

    # Write Header Row
    headers = []
    first = data[0]
    for key in first.keys():
        if key not in exclude_keys:
            if include_nested:
                if type(first[key]) is dict or type(first[key]) is list:
                    for k in first[key].keys():
                        headers.append(f'{key}_{k}')
            else:
                headers.append(key)

    do_header = not append

    if any(sheet.row_values(1)):
        do_header = False
    else:
        do_header = True

    if do_header:
        rows.append(headers)

    for d in data:
        _d = {}
        for k, v in d.items():
            if k not in exclude_keys:
                if k in key_tab_map.keys():
                    if type(v) is list:
                        if not tab_data.get(key_tab_map[k]):
                            tab_data[key_tab_map[k]] = []
                        for item in v:
                            tab_data[key_tab_map[k]].append(item)
                else:
                    if include_nested:
                        if type(v) is dict or type(v) is list:
                            for k2, v2 in v.items():
                                _d[f'{k}_{k2}'] = v2
                    elif type(v) is datetime.datetime:
                        _d[k] = v.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        if type(v) is not dict and type(v) is not list:
                            _d[k] = v

        rows.append(list(_d.values()))

    sheet.append_rows(rows)

    if tab_data:
        for k, v in tab_data.items():
            write_data(client, v, k, append=append)

    if not append:
        # Apply currency formatting to columns specified in kwargs
        cols = kwargs.get('currency_cols', [])
        for col in cols:
            gspread_formatting.format_cell_range(sheet, f'{col}:{col}', currency_format)

        # Apply date formatting to columns specified in kwargs
        cols = kwargs.get('date_cols', [])
        for col in cols:
            gspread_formatting.format_cell_range(sheet, f'{col}:{col}', date_format)

        # Apply percentage formatting to columns specified in kwargs
        cols = kwargs.get('percentage_cols', [])
        for col in cols:
            gspread_formatting.format_cell_range(sheet, f'{col}:{col}', percentage_format)

        cols = kwargs.get('number_cols', [])
        for col in cols:
            gspread_formatting.format_cell_range(sheet, f'{col}:{col}', number_format)

    return sheet


def find_sdf_files(directory, limit=None):
    sdf_files = []
    count = 0

    print(f"Searching {directory} for SDF files...")
    for file in Path(directory).rglob("*_purchasing/*.sdf"):
        if limit and count >= limit:
            break
        sdf_files.append(file)
        count += 1
        print(file)

    # pattern = r"(?i)\(\d+-\d+\)*.*purchasing"
    # for root, dirs, files in os.walk(directory):
    #     for file in files:
    #         if file.endswith(".sdf") and file == "MicrovellumWorkOrder.sdf":
    #             work_order = os.path.split(root)[-1]
    #             print(work_order)
    #             if re.match(pattern, work_order):
    #                 sdf_files.append(os.path.join(root, file))
    #                 count += 1
    return sdf_files


class WorkOrder:
    data = {}

    def __init__(self, file, client, separator='~'):
        self.conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
        self.cursor = self.conn.cursor()

        p = os.path.split(file)
        self.name = os.path.split(p[0])[-1]

        self.cursor.execute(f"""
            Select Name, LinkID From WorkOrders WHERE Name = '{self.name}'
        """)
        column_names = [column[0] for column in self.cursor.description]
        rows = self.cursor.fetchall()
        if len(rows) > 0:
            self.data = dict(zip(column_names, rows[0]))

        self.client = client
        self.date_created = datetime.datetime.fromtimestamp(os.path.getctime(p[0])).strftime('%Y-%m-%d %H:%M:%S')
        self.full_path = file
        self.separator = separator

        regex = r"(\d{5})\s*(.*)"
        match = re.search(regex, p[0])
        self.bid_id = int(match.group(1))
        name = match.group(2).strip()
        self.bid_name = re.search(r"(?i)-\s*(.*?).purchasing", name).group(1).strip()
        print(self.bid_id, self.bid_name)

        self.data['BidID'] = self.bid_id
        self.data['BidName'] = self.bid_name
        self.data['DateCreated'] = self.date_created

        batches = self.exec("SELECT * FROM WorkOrderBatches")
        self.batches = self.parse_results(batches, keys=['Name', 'LinkID', 'WorkOrderID'])
        if self.batches:
            self.first_batch_id = self.batches[0]['LinkID']
            self.runQuery("sheets", f"SELECT {(',').join(sheet_keys)} FROM PlacedSheets WHERE LinkIDWorkOrderBatch = '{self.first_batch_id}'")

        self.runQuery("subassemblies", f"SELECT {(',').join(subassembly_keys)} FROM Subassemblies")
        self.runQuery("products", f"SELECT {(',').join(product_keys)} FROM Products")
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

        # Write out variable to file
        # with open(f"{table_name}.txt", 'a') as file:
        #     file.write(str(out))

        rows = self.parse_results(out, keys=keys, filter_names=filter_names)

        for row in rows:
            row['BidID'] = self.bid_id
            row['BidName'] = self.name
            row['DateCreated'] = self.date_created

        if table_name:
            self.data[table_name] = rows

        return rows

    def dump_to_sheet(self, table_name, query, **kwargs):
        data = self.runQuery(table_name, query, **kwargs)
        return write_data(self.client, data, table_name, **kwargs)


class PurchaseOrders:
    data = {}

    def __init__(self):
        self.conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
        self.cursor = self.conn.cursor()
        self.query()

    def query(self):
        self.cursor.execute("""
            Select Comments, Name, Type, LinkID, DateCreated, LinkIDProject, LinkIDUpdatingEmployee,
                LinkIDVendor, ExpectedArrivalDate, PurchaseOrderNumber From PurchaseOrders
        """)
        column_names = [column[0] for column in self.cursor.description]
        po_rows = self.cursor.fetchall()
        for row in po_rows:
            _d = dict(zip(column_names, row))
            _d['ProjectID'] = None
            _d['ProjectName'] = None
            _d['Materials'] = []
            # Collect Purchased Items
            self.cursor.execute(f"""
                Select Cost, DateCreated, LinkID, LinkIDMaterial, LinkIDPart, LinkIDProduct,
                    LinkIDProject, LinkIDPurchaseOrder, LinkIDSheet, LinkIDWorkOrder, Name,
                    QuantityOrdered, QuantityReceived, Type, UnitType from PurchasedMaterial
                Where LinkIDPurchaseOrder = '{_d['LinkID']}'
            """)
            material_column_names = [column[0] for column in self.cursor.description]
            material_rows = self.cursor.fetchall()
            for r2 in material_rows:
                _d2 = dict(zip(material_column_names, r2))
                if _d2['LinkIDProject'] and not _d['ProjectID']:
                    self.cursor.execute(f"""
                        Select LinkID, Name, DateCreated from Projects
                        Where LinkID = '{_d2['LinkIDProject']}'
                    """)
                    project_column_names = [column[0] for column in self.cursor.description]
                    project_rows = self.cursor.fetchall()
                    if len(project_rows) == 1:
                        _d3 = dict(zip(project_column_names, project_rows[0]))
                        regex = r"^(\d+)"
                        try:
                            match = int(re.search(regex, _d3['Name']).group(1))
                        except Exception:
                            match = 'No ID Found'
                        _d['ProjectID'] = match
                        _d['ProjectName'] = _d3['Name']

                _d2['BidID'] = _d['ProjectID']
                _d2['BidName'] = _d['ProjectName']
                _d['Materials'].append(_d2)

            self.data[_d['LinkID']] = _d


if __name__ == "__main__":
    work_orders = {}
    interval = 15
    count = 0
    client = gspread.service_account_from_dict(GOOGLE_SHEETS_CREDENTIALS, client_factory=gspread.BackoffClient)

    sdf_files = find_sdf_files(directory)
    for file in sdf_files:
        count += 1
        progress = round(count / len(sdf_files) * 100, 1)
        print(f"{progress}%: Processing {file}...")
        wo = WorkOrder(file, client)
        work_orders[wo.bid_id] = wo.data

        if count % interval == 0:
            wo_count = len(work_orders.keys())
            print(f"Writing {wo_count} work orders to Google Sheets...")
            write_data(
                client,
                list(work_orders.values()),
                'WorkOrders',
                append=count > interval,
                key_tab_map={
                    'products': 'WO_Products',
                    'hardware': 'WO_Hardware',
                    'edgebanding': 'WO_Edgebanding',
                    'sheets': 'WO_Sheets',
                    'subassemblies': 'WO_Subassemblies',
                }
            )
            work_orders = {}

    po = PurchaseOrders()

    with open('purchase_orders_data.json', 'w') as json_file:
        json.dump(po.data, json_file, default=str)

    # for d in po.data.values():
    write_data(client, list(po.data.values()), 'PurchaseOrders',
               key_tab_map={'Materials': 'PurchaseOrderMaterials'},
               )
