import xml.etree.ElementTree as et
import requests
from datetime import datetime
# update database with new data from bnz.co.nz
def update_bnz(url, source_id):
    # HTTP headers required as would receive 403 Error from host
    headers = {
        "authority": "www.bnz.co.nz",
        "method": "GET",
        "path": "/XMLFeed/portal/fcs/xml",
        "scheme": "https",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "cookie": "visid_incap_435392=dr1cRgG7SBCZMEWZfJYSff3CUmAAAAAAQUIPAAAAAAASbX+C7NsAnUqo3EcdpeMf; incap_ses_364_435392=338+d87lXnTBIgPPXDANBf3CUmAAAAAAd/OKaCydMu77fhZ3gYW/Yg==; BIGipServer~BNZAK~pool-aklbvs002-ssl-bnz=!eSkco1xoExmMv3ulhs52XRcmp0uHY8gnwaocwVBHQTuTRqtgFaSheJdwzxYfxNekKIR4Ckq6odrzeQ==; nlbi_435392_447780=zzVcORrJfTpvGY6Gf0KPyAAAAAARd1HoWtSVZE5oUhKXLBqp",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36 Edg/89.0.774.54"}

    # Get XML data and store it in python readable format
    response = requests.get(url, headers=headers)
    conn = sqlite3.connect("ExchangeRates.db")
    cursor = conn.cursor()
    if response.status_code != 200:
        cursor.execute("UPDATE SOURCE_DETAILS SET UPDATE_SUCCESS = \"F\" WHERE SOURCE_ID = ?", [source_id])

        conn.commit()
        cursor.close()
        conn.close()
        return "Source " + url + " Unavailable"

    # Set date update of database
    # set_date(conn, cursor, response.headers['last-modified'], source_id)

    # Get root of XML file
    root = et.fromstring(response.content)
    new_root = root.find("standard")

    # Update CURRENT_RATES table with new exchange rates and SOURCE_DETAILS
    # table with update date and time

    xml_parse(conn, cursor, new_root, "indicativedate", "rate", "indicative", "currencycode", source_id)
    conn.close()


# xml_parser for floatrates and bnz
def xml_parse(conn, cursor, root, date_tag, item_tag, rate_tag, key_tag, source_id):
    row_count = 1
    for child in root:
        if (child.tag == date_tag):
            date = child.text
        if child.tag == item_tag:
            items_dict = {}
            for item in child:
                if item.tag == key_tag or item.tag == rate_tag:
                    items_dict[item.tag.strip()] = item.text.replace(",", "").strip()
            if len(items_dict) == 2:
                cursor.execute("UPDATE CURRENT_RATES SET VALUE = ? WHERE SOURCE_ID = ? AND TARGET = ?",
                               [items_dict[rate_tag], source_id, items_dict[key_tag]])
                if cursor.rowcount != row_count:
                    row_count = 0
                    conn.rollback()
                    cursor.execute("UPDATE SOURCE_DETAILS SET UPDATE_SUCCESS = \"F\" WHERE SOURCE_ID = ?;", [source_id])
                    conn.commit()
    if row_count == 1 and source_id == 1:
        set_date(conn, cursor, date, source_id)

    elif row_count == 1 and source_id == 3:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        date += " " + current_time
        set_date(conn, cursor, date, source_id)
    else:
        # # # # Should return the error as text/html and a header with * so CORS does not break
        return "Error: XML File has changed format. Cannot update data."


def set_date(conn, cursor, date, source_id):
    cursor.execute("UPDATE SOURCE_DETAILS SET UPDATE_SUCCESS = \"T\" WHERE SOURCE_ID = ?;", [source_id])
    dt_obj = datetime.strptime(date, '%d %b %Y %H:%M:%S')
    date_string = dt_obj.strftime("%d/%m/%Y %H:%M").strip()
    dt_ret = datetime.now()
    date_ret_string = dt_ret.strftime("%d/%m/%Y %H:%M")
    cursor.execute("UPDATE SOURCE_DETAILS SET DATE_MODIFIED = ?, USER_RETRIEVED = ? WHERE SOURCE_ID = ?;",
                   [date_string, date_ret_string, source_id])
    conn.commit()


# update database with new data from floatrates.com


def update_floatrates(url, source_id):
    response = requests.get(url)
    conn = sqlite3.connect("ExchangeRates.db")
    cursor = conn.cursor()
    if response.status_code != 200:
        cursor.execute('UPDATE SOURCE_DETAILS SET UPDATE_SUCCESS = ? WHERE SOURCE_ID = ?;', ("F", 1))
        conn.commit()
        conn.close()
        # # # # Should return the error as text/html and a header with * so CORS does not break --- same for other returns in the rest of this program
        return "Source " + url + " Unavailable"
    root = et.fromstring(response.content)
    xml_parse(conn, cursor, root, "lastBuildDate", "item", "exchangeRate", "targetCurrency", source_id)
    conn.close()


def manual_update(source_id):
    conn = sqlite3.connect("ExchangeRates.db")
    cursor = conn.cursor()
    cursor.execute("SELECT SOURCE FROM SOURCE_DETAILS WHERE SOURCE_ID = ?", [source_id])
    url = cursor.fetchone()[0]
    conn.close()
    source_id = int(source_id)
    status = ""
    if source_id == 1:
        status = update_floatrates(url, source_id) + " "
    elif source_id == 2:
        pass
    elif source_id == 3:
        status = status + update_bnz(url, source_id) + " "
    return status


# Update all the sources with latest exchange rates
def main_update():
    conn = sqlite3.connect("ExchangeRates.db")
    cursor = conn.cursor()
    query = "SELECT SOURCE, SOURCE_ID FROM SOURCE_DETAILS"
    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    for row in results:
        url = row[0]
        source_id = row[1]
        if source_id == 1:
            status = update_floatrates(url, source_id)
        elif source_id == 2:
            pass
        elif source_id == 3:
            status = update_bnz(url, source_id)
    return status

def build_xml(currency_results, columns, source_base, user_base, new_base, multiplier):
    data = et.Element("rates", {"Base":user_base, "Base_Value":"1"})
    for row in currency_results:
        attributes = {}
        for i in range(len(row)):
            # Calculates new exchange rate if there is a change in base currency from the source
            # row[2] is the row exchange rate
            # row[0] is the row currency code
            if source_base != user_base:
                if i == 2 and row[0] != new_base:
                    row[2] = row[2] * multiplier
                # rate for exchanging to same rate is 1. Hardcode in case rounding issues occur.l
                elif i == 2 and row[0] == new_base:
                    row[2] = 1
            attributes[columns[i]] = str(row[i])
        item = et.SubElement(data, "rate_item", attributes)

    string_data = et.tostring(data)
    return string_data
