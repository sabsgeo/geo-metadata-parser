from Bio import Medline
import requests

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
        except Exception as err:
            retry_num = retry_num + 1
            print("Retrying {} time for {}".format(retry_num, pmid))
    return {}
