import urllib.request
import tarfile
from helpers import general_helper, pubmed_oa_helper
from lxml import etree
import time


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

def get_tar_link(pmc_id):
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={}".format(pmc_id)
    number_of_retry = 3
    retry_num = 0
    while retry_num <= number_of_retry:
        try:
            with urllib.request.urlopen(url) as response:
                xml_data = response.read()
                elements = etree.fromstring(xml_data)
                for link in elements.findall("./records//link"):
                    if link.attrib["format"] == "tgz":
                        url = link.attrib["href"]
                        return url.replace("ftp://", "https://")
            retry_num = number_of_retry + 1
        except Exception as err:
            retry_num = retry_num + 1
            print("Not able to get the tar file for " + pmc_id + " going to retry")
            time.sleep(5)
    return None

def parse_pmc_info(pmc_id):
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
    pmc_doc_data = {}
    pmc_compressed_data = {}

    tar_file = get_tar_link(pmc_id)
    if tar_file == None:
        print("Not able to find the tar file for {}".format(pmc_id))
        return {}

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
                elif member.path.endswith(tuple([".jpg", ".gif"])):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_image_data[member.name] = content
                elif member.path.endswith(tuple([".pdf", ".docx", ".doc", ".xlsx", ".xls"])):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_doc_data[member.name] = content
                elif member.path.endswith(".zip"):
                    current_file_contents = my_tar.extractfile(member)
                    content = current_file_contents.read()
                    pmc_compressed_data[member.name] = content   
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
    return { "xml":pmc_xml_data, "image": pmc_image_data, "pdf": pmc_doc_data, "compressed": pmc_compressed_data, "video": pmc_video_data }
