from Bio import Medline
import requests
import pandas as pd
import json


def parse_medline(pmid):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&rettype=medline&retmode=text"

    number_of_retry = 3
    retry_num = 0
    while retry_num <= number_of_retry:
        response = requests.get(url)

        statusCode = response.status_code
        if statusCode == 200:
            records = Medline.parse(response.text.split("\n"))
            for record in records:
                if "TI" in record:
                    return dict(record)
                else:
                    print("Some issue in parsing the PUBMED ID {}".format(pmid))
                    return {}
        else:
            retry_num = retry_num + 1
            print("Retrying {} time for {}".format(retry_num, pmid))
    return {}


# Example usage
pmid = "23347031"  # Replace with the PMID of the article you want to parse
pmid_json = parse_medline(pmid)


json_data = open(
    '/Users/sabugeorge-elucidata/Personal/GEO-parsed/series_metadata.json')

# returns JSON object as
# a dictionary
json_data = json_data.read()
json_data.split("\n")

count = 0
json_coll = set()
for each_data in json_data.split("\n"):
    pubmed_ids = json.loads(each_data).get("Series_pubmed_id")
    if len(pubmed_ids) > 0:
        for pubmed_id in pubmed_ids:
            pub_json = parse_medline(pubmed_id)
            d = json.dumps({
                        "PMID": pmid,
                        "PMC ID": pmid_json.get("PMC", ""),
                        "Title": pmid_json.get("TI", ""),
                        "Transliterated Title": pmid_json.get("TT", ""),
                        "Journal Title": pmid_json.get("JT", ""),
                        "Journal Title Abbreviation": pmid_json.get("TA", ""),
                        "Publication Type": pmid_json.get("PT", []),
                        "Abstract": pmid_json.get("AB", ""),
                        "Medical Subject Headings": pmid_json.get("MH", []),
                        "Source": pmid_json.get("SO", ""),
                        "Artile Identifier": pmid_json.get("AID", []),
                        "General Note": pmid_json.get("GN", ""),
                        "Substance Name": pmid_json.get("NM", ""),
                        "Registry Number": pmid_json.get("RN", [])
                    }
                )
            if d not in json_coll:
                json_coll.add(d)
    else:
        print("Not present")
    count = count + 1
    if count > 10000:
        break
json_coll_list = list(json_coll)
final_list = []
for ss in json_coll_list:
    final_list.append(json.loads(ss))
df = pd.DataFrame(final_list)
df.to_csv("info.csv")