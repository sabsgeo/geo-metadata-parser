from Bio import Entrez

from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import gzip
import urllib.request
import io
import json
import traceback
import time


def get_recently_modified_gse_ids(n_days):
    Entrez.email = "your_email@example.com"
    end_date = datetime.today().strftime('%Y/%m/%d')
    start_date = (datetime.today() - timedelta(days=n_days)).strftime('%Y/%m/%d')

    query = f"GSE[ETYP] AND {start_date}[UDAT] : {end_date}[UDAT]"
    handle = Entrez.esearch(db="gds", term=query, retmax=0)
    record = Entrez.read(handle)
    total_count = int(record["Count"])
    handle.close()

    handle = Entrez.esearch(db="gds", term=query, retmax=total_count)
    record = Entrez.read(handle)
    handle.close()

    geo_ids = record["IdList"]
    gse_ids = []

    for geo_id in geo_ids:
        gse_ids.append("GSE{}".format(geo_id[3:]))

    return gse_ids


def get_gse_ids_from_pattern(gse_pattern):
    url = "https://ftp.ncbi.nlm.nih.gov/geo/series/" + gse_pattern + "/"

    soup = None
    number_of_retry = 3
    retry_num = 0
    while retry_num <= number_of_retry:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            retry_num = number_of_retry + 1
        except Exception as err:
            retry_num = retry_num + 1
            print("Not able to get patten " + gse_pattern + " going to retry")
            time.sleep(5)

    if soup == None:
        return []
    # Getting the date from the list
    links = soup.find_all("pre")[0]
    datetime_values = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', str(links))
    gse_ids = re.findall(r'href="([^"]*)"', str(links))
    gse_ids.pop(0)
    final_result = []
    if len(datetime_values) == len(gse_ids):
        for gse_id, datetime_value in zip(gse_ids, datetime_values):
            final_result.append(
                {'gse_id': gse_id[:-1], 'last_modified': datetime_value})
    else:
        print("Parse not fine")
    return final_result


def gse_pattern_from_gse_id(gse_id):
    id = int(int(re.findall(r'\d+', gse_id)[0]) / 1000)
    if id == 0:
        id = ""

    return "GSE{}nnn".format(str(id))


def has_soft_file(gse_id):
    id = int(int(re.findall(r'\d+', gse_id)[0]) / 1000)
    if id == 0:
        id = ""

    url = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE" + \
        str(id) + "nnn/" + gse_id + "/"
    links = ""
    try:
        response = requests.get(url)
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")
        # Getting the date from the list
        links = str(soup.find_all("pre")[0])
    except:
        pass

    if re.search("soft", links):
        return True
    else:        
        url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=all&form=text&view=full"
        search_txt = ""
        try:
            response = requests.head(url)
            search_txt = json.dumps(dict(response.headers))
        except:
            pass
        
        if re.search(gse_id + ".txt", search_txt):
            return True
        else:
            return False

def parse_soft_file(decompressed_data):
    if decompressed_data == None:
        return {}
    # Load the decompressed data into an IO stream
    stream = ""
    try:
        stream = io.StringIO(decompressed_data.decode())
    except:
        stream = io.StringIO(decompressed_data.decode('latin1'))
    # Read the lines from the stream
    start = False
    what_is_parsed = ''
    what_is_parsed_value = ''
    final_parse = {}
    for line in stream:
        if re.search("\^", line.strip()) and line.strip().startswith("^"):
            start_info = line.split("=")
            try:
                what_is_parsed = start_info[0].strip()[1:]
                what_is_parsed_value = "=".join(start_info[1:]).strip()
            except:
                continue
            if not (what_is_parsed in final_parse):
                final_parse[what_is_parsed] = {}
            if not (what_is_parsed_value in final_parse[what_is_parsed]):
                final_parse[what_is_parsed][what_is_parsed_value] = {}
            start = True
            continue
        if re.search("\!", line.strip()) and line.strip().startswith("!"):
            if (start):
                line_info = line.split("=")
                if len(line_info) > 1:
                    data_key = line_info[0].strip()[1:]
                    data_value = "=".join(line_info[1:]).strip()

                    if "Sample_supplementary_file" in data_key:
                        data_key = "Sample_supplementary_file"
                        
                    if data_key in final_parse[what_is_parsed][what_is_parsed_value]:
                        if not (isinstance(final_parse[what_is_parsed][what_is_parsed_value][data_key], list)):
                            save_single_value = final_parse[what_is_parsed][what_is_parsed_value][data_key]
                            final_parse[what_is_parsed][what_is_parsed_value][data_key] = []
                            final_parse[what_is_parsed][what_is_parsed_value][data_key].append(save_single_value)

                        final_parse[what_is_parsed][what_is_parsed_value][data_key].append(data_value)
                    else:
                        final_parse[what_is_parsed][what_is_parsed_value][data_key] = data_value
    return final_parse

def get_series_metadata_from_soft_file(gse_id):
    decompressed_data = None
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=self&form=text&view=brief"
    number_of_retry = 3
    retry_num = 0
    while retry_num <= number_of_retry:
        try:
            with urllib.request.urlopen(url) as response:
                decompressed_data = response.read()
            retry_num = number_of_retry + 1
        except Exception as err:
            retry_num = retry_num + 1
            print("Not able to parse " + gse_id + " going to retry")
            time.sleep(5)
    
    return parse_soft_file(decompressed_data)

def read_full_soft_file(gse_id):
    compressed_data = None
    try:
        url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=all&form=text&view=brief"
        with urllib.request.urlopen(url) as response:
            decompressed_data = response.read()
    except Exception as err:
        print(traceback.format_exc())
        print("Error in "+ gse_id + " tring to add again by another method")
        try:
            id = int(int(re.findall(r'\d+', gse_id)[0]) / 1000)
            if id == 0:
                id = ""
            url = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE" + \
                str(id) + "nnn/" + gse_id + "/soft/" + gse_id + "_family.soft.gz"
            with urllib.request.urlopen(url) as response:
                compressed_data = response.read()
        # Decompress the data
            decompressed_data = gzip.decompress(compressed_data)
        except Exception as err:
            print(traceback.format_exc())
            print("Not able to parse " + gse_id)
            return {}

    return parse_soft_file(decompressed_data)
