from io import BytesIO
from os import unlink
from tempfile import NamedTemporaryFile
from typing import List

import openpyxl
from openpyxl.styles import Alignment
from openpyxl.styles.cell_style import StyleArray
from openpyxl.worksheet.worksheet import Worksheet


def excel_workbook_to_stream(wb):
    stream = None
    tmp = NamedTemporaryFile(delete=False)  # create tmp file, we'll controll when to delete it
    # close, NamedTemporaryFile() already opended it, we need it closed for wb.save()
    # or we'll get permission denied errors
    tmp.close()
    wb.save(tmp.name)  # save workbook to tmp file
    with open(tmp.name, "rb") as f:
        stream = f.read()  # open for reading
    unlink(tmp.name)  # delete
    return stream


def excel_update_worksheet(ws, rows, align_params=None, fill_empty_rows=False):
    if not rows:
        return
    alignment = Alignment(**align_params) if align_params else None
    empty_fill = openpyxl.styles.PatternFill(start_color="ccffff", end_color="ccffff", fill_type="solid")
    aligned_styles = {}
    for row_num, row_data in enumerate(rows, 2):
        is_empty_row = fill_empty_rows and (row_data[1] == "" or row_data[1] is None)
        for col_num, col_data in enumerate(row_data, 1):
            cell = ws.cell(row=row_num, column=col_num)
            cell.value = col_data
            if is_empty_row:
                cell.fill = empty_fill
            if alignment is not None:
                _align_cell(cell, alignment, aligned_styles)


def _align_cell(cell, alignment: Alignment, aligned_styles: dict) -> None:
    key = tuple(cell._style) if cell.has_style else None
    aligned = aligned_styles.get(key)
    if aligned is None:
        cell.alignment = alignment
        aligned_styles[key] = StyleArray(cell._style)
    else:
        cell._style = StyleArray(aligned)


def excel_open_workbook(template_buffer):
    byte_stream = BytesIO(template_buffer)
    return openpyxl.load_workbook(byte_stream)


def group_rows_by_key(ws: Worksheet, rows: List[List], key_col: int = 0) -> None:
    if len(rows) < 2:
        return
    ws.sheet_properties.outlinePr.summaryBelow = False
    group_start = 0
    for i in range(1, len(rows) + 1):
        same_group = i < len(rows) and rows[i][key_col] == rows[group_start][key_col]
        if same_group:
            continue
        if i - group_start > 1:
            for row_num in range(group_start + 2 + 1, i + 2):
                ws.row_dimensions[row_num].outlineLevel = 1
        group_start = i
