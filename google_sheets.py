import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)


def open_sheet(spreadsheet_name: str, sheet_name: str):
    spreadsheet = client.open(spreadsheet_name)
    return spreadsheet.worksheet(sheet_name)


def read_cell(sheet, cell: str):
    return sheet.acell(cell).value


def write_cell(sheet, cell: str, value):
    sheet.update_acell(cell, value)


def write_row(sheet, start_cell: str, values: list):
    """
    Escribe una lista horizontal desde una celda inicial.
    Ejemplo: write_row(sheet, "C3", [2020, 2021, 2022])
    """
    col_letter = start_cell[0]
    row_number = int(start_cell[1:])
    end_col_index = ord(col_letter.upper()) - 65 + len(values)
    end_col_letter = chr(65 + end_col_index - 1)

    cell_range = f"{start_cell}:{end_col_letter}{row_number}"
    cell_list = sheet.range(cell_range)

    for i, cell in enumerate(cell_list):
        if i < len(values):
            cell.value = values[i]

    sheet.update_cells(cell_list)

