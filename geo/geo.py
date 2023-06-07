import requests
from bs4 import BeautifulSoup
import re
import gzip
import urllib.request
import io
import json
import traceback


def get_series_parrerns_for_geo():
    # URL to scrape
    url = "https://ftp.ncbi.nlm.nih.gov/geo/series/"

    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all the links (directories) on the page

    # Getting the date from the list
    links = soup.find_all("pre")[0]
    datetime_values = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', str(links))

    # Getting all the href and removing the headinf by poping
    hrefs = re.findall(r'href="([^"]*)"', str(links))
    gse_patterns = sorted(set(hrefs), key=hrefs.index)
    gse_patterns.pop(0)
    final_result = []
    if len(datetime_values) == len(gse_patterns):
        for gse_patten, datetime_value in zip(gse_patterns, datetime_values):
            gse_patten_index = 0 if len(re.findall(
                r'\d+', gse_patten)) == 0 else int(re.findall(r'\d+', gse_patten)[0])
            # Removing the exttra backslash
            final_result.append(
                {'gse_patten': gse_patten[:-1], 'last_modified': datetime_value, 'index': gse_patten_index})
    else:
        print("Parse not fine")

    return sorted(final_result, key=lambda d: d['index'], reverse=True)


def get_gse_ids_from_pattern(gse_pattern):
    url = "https://ftp.ncbi.nlm.nih.gov/geo/series/" + gse_pattern + "/"

    response = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all the links (directories) on the page

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

def get_gse_with_soft(gse_ids):
    # final_result = []
    for gse_id in gse_ids:
        id = int(int(re.findall(r'\d+', gse_id['gse_id'])[0]) / 1000)
        url = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE" + \
            str(id) + "nnn/" + gse_id['gse_id'] + "/"
        response = requests.get(url)

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all the links (directories) on the page

        # Getting the date from the list
        links = str(soup.find_all("pre")[0])
        if re.search("soft", links):
            # final_result.append(gse_id)
            yield gse_id

def parse_soft_file(decompressed_data):
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

def read_series_metadata_from_soft_file(gse_id):
    decompressed_data = ""
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse_id + "&targ=self&form=text&view=brief"

    try:
        with urllib.request.urlopen(url) as response:
            decompressed_data = response.read()
    except Exception as err:
        print(traceback.format_exc())
        print("Not able to parse " + gse_id)
    
    return parse_soft_file(decompressed_data)

def get_samples_ids(gse_id):
   return read_series_metadata_from_soft_file(gse_id).get("SERIES").get(gse_id).get("Series_sample_id")

def read_full_soft_file(gse_id):
    compressed_data = ""
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
