# Backend File Validation for the CSV's
import pandas as pd
from datetime import datetime
import re
import os
from pandera import Column, DataFrameSchema, Check
from flood_prediction.constants import FLOOD_DATA_DIR

filename_regex = re.compile("^60rf_\d{4}$")


def validateDateTime(date):
    try:
        datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
        return True
    except Exception:
        return False


schema = DataFrameSchema(
    {
        "obstime": Column(str, Check(lambda s: validateDateTime(s), element_wise=True)),
        "station_id": Column(int),
        "Past 60-Minutes Rainfall in mm": Column(float),
        "qcscore": Column(str, Check.isin(["S", "G", "M", "F", "X"])),
    }
)


def validateCSV(file):
    if file.filename == "":
        return 400, "No selected file"
    filename, extension = file.filename.rsplit(".")
    extension = extension.lower()
    if extension != "csv":
        return 400, "Please upload a csv file only"
    filename_match = filename_regex.match(filename)
    if filename_match is None:
        return (
            400,
            'The uploaded file name must be in "60rf_YYYY" format where YYYY is the year of data',
        )
    df = pd.read_csv(file)
    columnNames = df.columns.tolist()
    validationColumns = [
        "obstime",
        "station_id",
        "Past 60-Minutes Rainfall in mm",
        "qcscore",
    ]
    if columnNames != validationColumns:
        return 400, "Please ensure the file has correct column names in correct format"
    try:
        schema.validate(df)
        saved_file_dir = os.path.join(FLOOD_DATA_DIR, file.filename)
        if os.path.isfile(saved_file_dir):
            return 400, "File already exists"
        file.save(saved_file_dir)
    except:
        return 400, "Data not in correct format"
    return 200, "File Validated & Added"
