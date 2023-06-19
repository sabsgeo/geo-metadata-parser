from Bio import Medline
import requests
import pandas as pd
import json
import csv


def parse_medline(pmid):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&rettype=medline&retmode=text"

    number_of_retry = 3
    retry_num = 0
    while retry_num <= number_of_retry:
        records = {}
        try:
            response = requests.get(url)
            records = Medline.parse(response.text.split("\n"))
            for record in records:
                if "TI" in record:
                    return dict(record)
                else:
                    print("Some issue in parsing the PUBMED ID {}".format(pmid))
                    return {}
        except Exception as err:
            retry_num = retry_num + 1
            print("Retrying {} time for {}".format(retry_num, pmid))
    return {}

import os
# json_data = open(
#     '/Users/sabugeorge-elucidata/Personal/GEO-parsed/series_metadata.json')

json_data = open('/home/ubuntu/series_metadataMonday_Jun_19_2023_04:22:06.json')

# returns JSON object as
# a dictionary
json_data = json_data.read()
json_data.split("\n")
pubmed_id_added = set()
pro_num = int(os.path.basename(__file__).split("_")[1].split(".")[0])
headers = ['pmid', 'pmc_id', 'title', 'transliterated_title', 'journal_title', 'journal_title_abbreviation',
           'publication_type', 'abstract', 'medical_subject_headings', 'source', 'article_identifier',
           'general_note', 'substance_name', 'registry_number']
with open('pmbmed_id_info_{}.csv'.format(str(pro_num)), 'w') as write_file:
    writer = csv.DictWriter(write_file, fieldnames=headers)
    each_json = json_data.split("\n")
    each_json_len = len(each_json)
    pro_num = int(os.path.basename(__file__).split("_")[1].split(".")[0])
    proc_num = 10
    start = pro_num * int(each_json_len / proc_num)
    end = (pro_num + 1) * int(each_json_len / proc_num) - 1
    final_each_json = each_json[start:end]
    for each_data in final_each_json:
        pubmed_ids = json.loads(each_data).get("Series_pubmed_id")
        if len(pubmed_ids) > 0:
            for pubmed_id in pubmed_ids:
                if not(pubmed_id in pubmed_id_added):
                    pub_json = parse_medline(pubmed_id)
                    writer.writerow({
                        "pmid": pubmed_id,
                        "pmc_id": pub_json.get("PMC", ""),
                        "title": pub_json.get("TI", ""),
                        "transliterated_title": pub_json.get("TT", ""),
                        "journal_title": pub_json.get("JT", ""),
                        "journal_title_abbreviation": pub_json.get("TA", ""),
                        "publication_type": pub_json.get("PT", []),
                        "abstract": pub_json.get("AB", ""),
                        "medical_subject_headings": pub_json.get("MH", []),
                        "source": pub_json.get("SO", ""),
                        "article_identifier": pub_json.get("AID", []),
                        "general_note": pub_json.get("GN", ""),
                        "substance_name": pub_json.get("NM", ""),
                        "registry_number": pub_json.get("RN", [])
                    })
                    pubmed_id_added.add(pubmed_id)
        else:
            print("Not present")
