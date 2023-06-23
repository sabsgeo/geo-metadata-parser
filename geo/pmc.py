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
        link = element.attrib[element.attrib.keys()[0]]
        text = [k for k in element.itertext()]
        supplementary_data.append({
            "href": link, 
            "text": text[0] if len(text) > 0 else "" 
        })

    return supplementary_data


def parse_pmc_info(pmc_id):
    latest_tar_time = get_latest_pmc_updated_tar_time()
    pmc_file_name = "oa_file_list_{}.db".format(latest_tar_time)
    pmc_list_path = os.path.join(os.getcwd(), pmc_file_name)

    pmc_xml_data = {
        "article_metadata": {},
        "supplementary_metadata": [],
        "tables": [],
        "caption": [],
        "references": [],
        "paragraph": []
    }
    pmc_image_data = {}
    pmc_video_data = {}
    pmc_pdf_data = {}
    pmc_zip_data = {}


    if not (os.path.exists(pmc_list_path)):
        raise Exception("db {} not found".format(pmc_list_path))

    con = sqlite3.connect(pmc_list_path)
    cur = con.cursor()
    cur.execute("SELECT * FROM oa_file_list WHERE pmc_id=?", (pmc_id,))
    selected_row = cur.fetchone()
    if not(selected_row == None):
        tar_path = cur.fetchone()[0]
    else:
        print("Not able to get the path for {}".format(pmc_id))
        return {}
    cur.close()

    tar_file = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/{}".format(tar_path)
    ftpstream = urllib.request.urlopen(tar_file)
    with tarfile.open(fileobj=ftpstream, mode="r|gz") as my_tar:
        for member in my_tar:
            if (member.isfile()):
                if member.path.endswith(".nxml"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_xml_data["article_metadata"] = pubmed_oa_helper.parse_pubmed_xml(content, nxml=True)
                    pmc_xml_data["supplementary_metadata"] = get_supplementary_info(content)
                    pmc_xml_data["tables"] = pubmed_oa_helper.parse_pubmed_table(content, return_xml=False)
                    pmc_xml_data["caption"] = pubmed_oa_helper.parse_pubmed_caption(content)
                    pmc_xml_data["references"] = pubmed_oa_helper.parse_pubmed_references(content)
                    pmc_xml_data["paragraph"] = pubmed_oa_helper.parse_pubmed_paragraph(content)
                elif member.path.endswith(".jpg") or  member.path.endswith(".gif"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_image_data[member.name] = content
                elif member.path.endswith(".pdf"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_pdf_data[member.name] = content
                elif member.path.endswith(".zip"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_zip_data[member.name] = content   
                elif member.path.endswith(".avi"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_video_data[member.name] = content                
        
        for parsed_keys in pmc_xml_data:
            if parsed_keys == "article_metadata":
                if len(pmc_xml_data[parsed_keys].keys()) < 1:
                    pmc_xml_data[parsed_keys] = {}
            else:
                if pmc_xml_data[parsed_keys] == None:
                    pmc_xml_data[parsed_keys] = []
    return { "xml":pmc_xml_data, "image": pmc_image_data, "pdf": pmc_pdf_data, "compressed": pmc_zip_data, "video": pmc_video_data }

def get_latest_pmc_updated_tar_time():
    url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt"
    with urlopen(url) as file:
        lines = [line.decode().strip() for _, line in zip(range(1), file)]
    return lines[0]
