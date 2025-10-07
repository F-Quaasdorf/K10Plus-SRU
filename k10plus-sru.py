import requests
import unicodedata
from lxml import etree
import pandas as pd


def k10plus_sru(query):
    """SRU-Query for K10plus catalogue."""
    base_url = "http://sru.k10plus.de/opac-de-627" # Select database of specific library from https://uri.gbv.de/database/opac
    parameters = {
        "recordSchema": "marcxml",
        "operation": "searchRetrieve",
        "version": "2.0",
        "maximumRecords": "100",
        "query": query
    }

    ns = {"zs": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
        "marc": "http://www.loc.gov/MARC21/slim"}

    session = requests.Session()
    records = []
    start_record = 1
    first_request = True

    while True:
        parameters["startRecord"] = start_record
        response = session.get(base_url, params=parameters)

        if first_request:
            print(f"SRU-Request URL: {response.url}")
            first_request = False

        if response.status_code != 200:
            print(f"Error fetching data: HTTP {response.status_code}")
            break

        xml_root = etree.fromstring(response.content)

        marc_records = xml_root.xpath(".//zs:recordData/marc:record", namespaces=ns)
        records.extend(etree.tostring(r, encoding="unicode") for r in marc_records)

        print(f"Fetched {len(marc_records)} records (startRecord={start_record})")

        if len(marc_records) < 100:
            break

        start_record += 100

    print(f"Total records fetched: {len(records)}")
    return records


def parse_record(record):
    """Parses every MARCXML record element into dictionary."""
    ns = {"marc": "http://www.loc.gov/MARC21/slim"}
    xml = etree.fromstring(unicodedata.normalize("NFC", record))

    def get_text(xpath_expr):
        values = [elem.text for elem in xml.xpath(xpath_expr, namespaces=ns)]
        return ", ".join(values) if values else "N.N."

    meta = {
        "Verfasser": get_text("//marc:datafield[@tag='100']/marc:subfield[@code='a']"),
        "Titel": get_text("//marc:datafield[@tag='245']/marc:subfield[@code='a']"),
        "Erscheinungsort": get_text("//marc:datafield[@tag='264']/marc:subfield[@code='a']"),
        "Erscheinungsjahr": get_text("//marc:datafield[@tag='264']/marc:subfield[@code='c']"),
        "Sprache": get_text("//marc:datafield[@tag='041']/marc:subfield[@code='a']"),        
        "Einrichtung": get_text("//marc:datafield[@tag='924']/marc:subfield[@code='b']")
    }

    return meta


def to_df(records):
    """Converts list of dictionaries into Pandas DataFrame."""
    return pd.DataFrame(records)


if __name__ == "__main__":
    # Example
    query = "pica.ppn=157142477"

    records = k10plus_sru(query)
    parsed_records = [parse_record(record) for record in records]
    df = to_df(parsed_records)

    pd.set_option("display.max_columns", None)
    print(df)
