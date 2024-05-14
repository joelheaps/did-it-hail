import csv
from pathlib import Path
import pprint


def get_icao_values(filename: str) -> list[str]:
    with Path(filename).open() as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        return [row[1].strip() for row in reader]


# Usage
icao_values = get_icao_values("nexrad.csv")

# Print in a nice format
pprint.pprint(icao_values)
