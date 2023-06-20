from geo import pubmed
import json

f = open('pub_med_diff.txt')
k = f.read()
k = k.strip("[")
pubmed_ids = k.strip("]")
final_json = {}
for pubmed_id in pubmed_ids.split(","):
    new_id = str(pubmed_id.strip().strip('\''))
    print("Trying to add pubmed id {}".format(new_id))
    pub_json = pubmed.parse_medline(pubmed_id)
    final_json[new_id] = {
        "pmid": new_id,
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
    }

json_object = json.dumps(final_json, indent=4)
 
# Writing to sample.json
with open("pubmed_info1.json", "w") as outfile:
    outfile.write(json_object)
