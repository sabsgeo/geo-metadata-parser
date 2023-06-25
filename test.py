from lxml import etree
import urllib.request
import time

pmc_id = "PMC5334499"

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

k = get_tar_link(pmc_id)
print(k)

