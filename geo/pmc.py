import os
from urllib.request import urlopen
import sqlite3
import urllib.request
import tarfile
from helpers import general_helper, pubmed_oa_helper

def get_supplementary_info(path):
    supplementary_data = []
    tree = general_helper.read_xml(path, nxml=True)
    tree_title = tree.findall(".//supplementary-material/media")
    for element in tree_title:
        supplementary_data.append({
            "href": [element.attrib[element.attrib.keys()[0]]], 
            "text": [k for k in element.itertext()][0]
        })

    return supplementary_data


def parse_pmc_info(pmc_id):
    latest_tar_time = get_latest_pmc_updated_tar_time()
    pmc_file_name = "oa_file_list_{}.db".format(latest_tar_time)
    pmc_list_path = os.path.join(os.getcwd(), pmc_file_name)

    pmc_parsed_data = {
        "article_metadata": {},
        "supplementary_metadata": [],
        "tables": [],
        "caption": [],
        "references": [],
        "paragraph": []
    }

    if not (os.path.exists(pmc_list_path)):
        raise Exception("db {} not found".format(pmc_list_path))

    con = sqlite3.connect(pmc_list_path)
    cur = con.cursor()
    cur.execute("SELECT * FROM oa_file_list WHERE pmc_id=?", (pmc_id,))
    tar_path = cur.fetchall()[0][0]
    cur.close()

    tar_file = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/{}".format(tar_path)
    print(tar_file)
    ftpstream = urllib.request.urlopen(tar_file)
    with tarfile.open(fileobj=ftpstream, mode="r|gz") as my_tar:
        for member in my_tar:
            if (member.isfile()):
                if member.path.endswith(".nxml"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_parsed_data["article_metadata"] = pubmed_oa_helper.parse_pubmed_xml(content, nxml=True)
                    pmc_parsed_data["supplementary_metadata"] = get_supplementary_info(content)
                    pmc_parsed_data["tables"] = pubmed_oa_helper.parse_pubmed_table(content, return_xml=False)
                    pmc_parsed_data["caption"] = pubmed_oa_helper.parse_pubmed_caption(content)
                    pmc_parsed_data["references"] = pubmed_oa_helper.parse_pubmed_references(content)
                    pmc_parsed_data["paragraph"] = pubmed_oa_helper.parse_pubmed_paragraph(content)
        
        for parsed_keys in pmc_parsed_data:
            if parsed_keys == "article_metadata":
                if len(pmc_parsed_data[parsed_keys].keys()) < 1:
                    pmc_parsed_data[parsed_keys] = {}
            else:
                if pmc_parsed_data[parsed_keys] == None:
                    pmc_parsed_data[parsed_keys] = []
    return pmc_parsed_data

def get_latest_pmc_updated_tar_time():
    url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt"
    with urlopen(url) as file:
        lines = [line.decode().strip() for _, line in zip(range(1), file)]
    return lines[0]
