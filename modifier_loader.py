import csv
import datetime
from collections import defaultdict

def parse_date_flexible(date_str: str) -> datetime.date | None:
    """
    Try parsing the date in mm/dd/yyyy or dd/mm/yyyy format.
    Returns a date object or None if parsing fails.
    """
    for fmt in ("%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None

def load_modifier_map(file_path: str) -> dict[str, set[str]]:
    # simple function to read in the csv
    # and populate the service / modifier map
    modifiers_to_codes = defaultdict(set)
    today = datetime.date.today()

    with open(file_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter="|")
        for row in reader:
            code = row["PROCEDURE_CODE"].strip()
            modifier = row["MODIFIER_CODE"].strip()
            expiration = row["EXPIRATION_DATE"].strip()

            if expiration:
                exp_date = parse_date_flexible(expiration)
                if not exp_date:
                    print(f"⚠️ Unrecognized date format: {expiration} — skipping row")
                    continue
                if exp_date < today:
                    continue  # Expired

            modifiers_to_codes[modifier].add(code)

    return dict(modifiers_to_codes)

