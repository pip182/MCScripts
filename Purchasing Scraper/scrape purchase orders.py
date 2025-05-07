import os
import xlrd
import xlwt
import re
import os
import re
import datetime
import re



def parse_xls_files(directory):
    headers = ['bid_id', 'bid_name', 'purchase date']
    all_data = []
    # Output all_data to an .xls file
    output_file_path = os.path.join(directory, "output.xls")

    # Delete existing file at output_file_path so we don't include it in the new file
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    for filename in os.listdir(directory):
        if filename.endswith(".xls"):
            file_path = os.path.join(directory, filename)
            ctime = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(0)

            # Get bid # from filename if possible
            # regex = r"\b\d{5}\b"
            # match = re.search(regex, filename)

            regex = r"(\d{5})\s*(.*)"
            match = re.search(regex, os.path.splitext(filename)[0])

            if match:
                bid_id = int(match.group(1))
                bid_name = match.group(2)
            else:
                bid_id = None
                bid_name = os.path.splitext(filename)[0]

            # Get the header row
            header_row = sheet.row_values(0)
            for i in range(len(header_row)):
                if header_row[i] not in headers:
                    headers.append(header_row[i])

            # Process the data from the sheet
            for row in range(1, sheet.nrows):
                data_row = sheet.row_values(row)
                d_row = [None] * 40  # Create a list of 20 None values

                # Insert additional information at first place in data_row
                data_row.insert(0, ctime.strftime("%Y-%m-%d"))
                data_row.insert(0, bid_name)
                data_row.insert(0, bid_id)

                for i in range(len(data_row)):
                    is_fractional = False
                    value = data_row[i]
                    if i == 4 and type(value) is str:
                        pattern = r"^(\d+)\s*(\w+)"
                        match = re.match(pattern, value)

                        if bool(match):
                            number = float(match.group(1))
                            unit = match.group(2)
                            # print("Number:", number)
                            # print("Unit:", unit)
                            d_row[i] = number
                            d_row[i + 1] = unit
                        else:
                            d_row[i] = value
                    elif i == 5 and type(value) is str:
                        pattern = r"^(\d-?.*'')(\s*x\s*)(\d-?.*'')"
                        is_fractional = bool(re.match(pattern, value))
                        print(is_fractional, value, i)

                        if is_fractional:
                            d_row[6] = value
                        else:
                            d_row[i] = value
                    else:
                        d_row[i] = value

                data = dict(zip(headers, d_row))
                all_data.append(data)

    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("Data")

    for i, header in enumerate(headers):
        sheet.write(0, i, header)

    date_format = xlwt.Style.easyxf(num_format_str='YYYY-MM-DD')

    for row, data in enumerate(all_data):
        for col, value in enumerate(data.values()):
            if col == 1:  # Column B
                sheet.write(row + 1, col, label=value, style=date_format)
            else:
                sheet.write(row + 1, col, label=value)

    workbook.save(output_file_path)





if __name__ == "__main__":
    # Usage example
    directory_path = "O:\\JOBS 2023\\1 Requisition 2023\\Ready to Order\\Microvellum PO"
    # parse_xls_files(directory_path)
    select_purchase_orders_from_database()
