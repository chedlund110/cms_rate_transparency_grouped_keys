import csv
import datetime

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

def load_modifier_map(file_path: str) -> set[tuple[str, str]]:
    active_modifiers = set()
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

            active_modifiers.add((code, modifier))

    return active_modifiers

