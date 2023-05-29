from geo import geo
from geo import geo_mongo

from Bio import Entrez
from geo import google_sheet_helper
import pandas as pd
import json
import traceback
import itertools


def get_journal_name(pubmed_id):
    # Set your email address (required by PubMed API
    Entrez.email = "sabu.george@elucidata.io"
    try:
        # Search for a publication by PubMed ID
        handle = Entrez.efetch(db="pubmed", id=pubmed_id, rettype="xml")
        record = Entrez.read(handle)
    except:
        return ""

    journal_name = ""

    try:
        # Extract the journal name from the publication record
        journal_name = record['PubmedArticle'][0]['MedlineCitation']['Article']['Journal']['Title']
    except:
        pass

    return journal_name


def get_pubmed_contributors(pmid):
    """
    Given a PubMed ID (PMID), retrieve the contributors of the publication using the Entrez module from Biopython.
    """
    # Provide your own email address to use with Entrez
    Entrez.email = "sabu.george@elucidata.io"
    try:
        # Use Entrez to retrieve the summary of the PubMed record
        handle = Entrez.esummary(db="pubmed", id=pmid)
        record = Entrez.read(handle)[0]
        handle.close()
    except:
        return []

    # Extract the list of contributors from the summary and return it
    if "AuthorList" in record:
        return record["AuthorList"]
    else:
        return []
    

def get_sample_level_info():

    all_finished = False

    while not (all_finished):
        try:
            sheet_instance = google_sheet_helper.GoogleSheetUpdater()
            gse_id_sample_info_sheet = sheet_instance.get_gse_id_sample_info_worksheet()
            gse_id_sheet = sheet_instance.get_gse_id_worksheet()
            gse_id_sample_info_df = pd.DataFrame(
                gse_id_sample_info_sheet.get_all_records())
            last_added_gse_id = None
            last_added_sample = None

            try:
                last_added_gse_id = str(gse_id_sample_info_df.iloc[len(
                    gse_id_sample_info_df.index) - 1, gse_id_sample_info_df.columns.get_loc("gse_id")])
                last_added_sample = str(gse_id_sample_info_df.iloc[len(
                    gse_id_sample_info_df.index) - 1, gse_id_sample_info_df.columns.get_loc("gsm_id")])
            except:
                pass

            last_gse_id_found = False
            last_gsm_id_found = False
            gse_id_df = pd.DataFrame(gse_id_sheet.get_all_records())

            for index, gse_id in gse_id_df.iterrows():
                if last_added_gse_id == gse_id['gse_id']:
                    last_gse_id_found = True

                if last_gse_id_found or last_added_gse_id == None:
                    if geo.has_soft_file(gse_id['gse_id']):
                        soft_file = {}
                        try:
                            soft_file = geo.read_full_soft_file(gse_id['gse_id'])
                        except:
                            pass
                        Sample_library_selection = ""
                        Sample_supplementary_file = []
                        Sample_description = []
                        Sample_type = ""
                        Sample_title = ""
                        Sample_scan_protocol = []
                        Sample_library_source = ""
                        Sample_hyb_protocol = ""
                        Sample_relation = ""
                        Sample_instrument_model = ""
                        Sample_contact_web_link = ""
                        Sample_data_processing = []
                        Sample_library_strategy = ""
                        channel_data = {}
                        if "SAMPLE" in soft_file:
                            for sample_id in soft_file["SAMPLE"].keys():
                                if last_gsm_id_found or last_added_sample == None:
                                    Sample_library_selection = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_library_selection", "")
                                    Sample_supplementary_file = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_supplementary_file", [])
                                    Sample_description = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_description", "")
                                    Sample_type = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_type", "")
                                    Sample_title = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_title", "")
                                    Sample_scan_protocol = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_scan_protocol", [])
                                    Sample_library_source = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_library_source", "")
                                    Sample_hyb_protocol = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_hyb_protocol", "")
                                    Sample_relation = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_relation", "")
                                    Sample_instrument_model = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_instrument_model", "")
                                    Sample_contact_web_link = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_contact_web_link", "")
                                    Sample_data_processing = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_data_processing", [])
                                    Sample_library_strategy = soft_file["SAMPLE"][sample_id].get(
                                        "Sample_library_strategy", "")

                                    # getting all the characteristic data
                                    if "Sample_channel_count" in soft_file["SAMPLE"][sample_id]:
                                        for channel_count in range(int(soft_file["SAMPLE"][sample_id]["Sample_channel_count"])):
                                            updated_channel_count = channel_count + 1
                                            channel_key = "ch" + \
                                                str(updated_channel_count)
                                            if not (channel_key in channel_data.keys()):
                                                channel_data[channel_key] = {}

                                        for sample_keys in soft_file["SAMPLE"][sample_id].keys():
                                            last_key = sample_keys.split(
                                                "_")[-1]
                                            if "ch" in last_key:
                                                channel_data[last_key][sample_keys] = soft_file["SAMPLE"][sample_id][sample_keys]
                                    final_array_channel_data = []
                                    for channel_sh_keys in channel_data.keys():
                                        channel_data[channel_sh_keys]["char_name"] = channel_sh_keys
                                        final_array_channel_data.append(
                                            channel_data[channel_sh_keys])

                                    final_array_channel_data = sorted(
                                        final_array_channel_data, key=lambda x: x["char_name"])
                                    temp_array = []
                                    for obj_data in final_array_channel_data:
                                        temp_array.append(json.dumps(obj_data))

                                    final_array_channel_data = temp_array
                                    data_tobe_added = [
                                        str(gse_id['gse_id']),
                                        str(sample_id),
                                        str(Sample_library_selection),
                                        str(Sample_supplementary_file),
                                        str(Sample_description),
                                        str(Sample_type),
                                        str(Sample_title),
                                        str(Sample_scan_protocol),
                                        str(Sample_library_source),
                                        str(Sample_hyb_protocol),
                                        str(Sample_relation),
                                        str(Sample_instrument_model),
                                        str(Sample_contact_web_link),
                                        str(Sample_data_processing),
                                        str(Sample_library_strategy)
                                    ]

                                    data_tobe_added = data_tobe_added + final_array_channel_data
                                    sheet_instance.write_to_worksheet(
                                        gse_id_sample_info_sheet, data_tobe_added)

                                if last_added_sample == sample_id:
                                    last_gsm_id_found = True
            all_finished = True
        except Exception as err:
            print(traceback.format_exc())


def get_sample_level_possible_fields():
    series_pattern = geo.get_series_parrerns_for_geo()
    final_sample_cols = set()
    for each_pattern in reversed(series_pattern):
        gse_ids = geo.get_gse_ids_from_pattern(each_pattern['gse_patten'])
        for gse_id in gse_ids:
            f = open("sample_cols.txt", "r+")
            f.truncate(0)
            f.write(str(final_sample_cols))
            f.close()
            if geo.has_soft_file(gse_id['gse_id']):
                soft_file = {}
                try:
                    print("A")
                    soft_file = geo.read_full_soft_file(gse_id['gse_id'])
                    print("B")
                except:
                    pass
                if "SAMPLE" in soft_file:
                    for sample_id in soft_file["SAMPLE"].keys():
                        new_value = set(
                            list(soft_file["SAMPLE"][sample_id].keys()))
                        final_sample_cols.update(new_value)


def polulate_gse_id():
    # Code for state management
    sheet_instance = google_sheet_helper.GoogleSheetUpdater()
    gse_id_info_sheet = sheet_instance.get_gse_id_worksheet()
    gse_id_info_df = pd.DataFrame(gse_id_info_sheet.get_all_records())

    last_pattern_added = None
    last_gse_id_added = None
    try:
        last_pattern_added = str(gse_id_info_df.iloc[len(
            gse_id_info_df.index) - 1, gse_id_info_df.columns.get_loc("gse_patten")])
        last_gse_id_added = str(gse_id_info_df.iloc[len(
            gse_id_info_df.index) - 1, gse_id_info_df.columns.get_loc("gse_id")])
    except:
        pass
    all_finished = False

    while not (all_finished):
        last_pattern_found = False
        last_gse_id_found = False

        try:
            sheet_instance = google_sheet_helper.GoogleSheetUpdater()
            gse_id_sheet = sheet_instance.get_gse_id_worksheet()
            series_pattern = geo.get_series_parrerns_for_geo()
            for each_pattern in reversed(series_pattern):
                if each_pattern['gse_patten'] == last_pattern_added:
                    last_pattern_found = True
                if not (last_pattern_added == None) and last_pattern_found:
                    gse_ids = geo.get_gse_ids_from_pattern(
                        each_pattern['gse_patten'])
                    for gse_id in gse_ids:
                        if not (last_gse_id_added == None) and last_gse_id_found:
                            if geo.has_soft_file(gse_id['gse_id']):
                                sheet_instance.write_to_worksheet(gse_id_sheet, [
                                                                  each_pattern['gse_patten'], gse_id['gse_id'], gse_id['last_modified']])
                                last_pattern_added = each_pattern['gse_patten']
                                last_gse_id_added = gse_id['gse_id']
                            else:
                                print(
                                    "gse id" + gse_id['gse_id'] + " does not have soft file")
                        else:
                            if gse_id['gse_id'] == last_gse_id_added:
                                last_gse_id_found = True
                            print("Skip gse id " + gse_id['gse_id'])
                else:
                    print("Skip patten " + each_pattern['gse_patten'])
            all_finished = True
        except Exception as err:
            print(traceback.format_exc())


def get_all_gse_id_information():
    sheet_instance = google_sheet_helper.GoogleSheetUpdater()
    gse_id_info_sheet = sheet_instance.get_gse_id_info_worksheet()
    gse_id_info_df = pd.DataFrame(gse_id_info_sheet.get_all_records())
    last_added_gse_id = None
    try:
        last_added_gse_id = str(gse_id_info_df.iloc[len(
            gse_id_info_df.index) - 1, gse_id_info_df.columns.get_loc("gse_id")])
    except:
        pass
    all_finished = False

    while not (all_finished):
        last_gse_id_found = False
        try:
            sheet_instance = google_sheet_helper.GoogleSheetUpdater()
            gse_id_sheet = sheet_instance.get_gse_id_worksheet()
            gse_id_info_sheet = sheet_instance.get_gse_id_info_worksheet()

            gse_id_df = pd.DataFrame(gse_id_sheet.get_all_records())

            for index, gse_id in gse_id_df.iterrows():
                if last_gse_id_found or last_added_gse_id == None:
                    soft_file = {}
                    try:
                        soft_file = geo.read_full_soft_file(gse_id['gse_id'])
                    except:
                        pass
                    Series_title = ""
                    Series_status = ""
                    Series_pubmed_id = []
                    Series_web_link = ""
                    Series_summary = ""
                    Series_overall_design = ""
                    Series_type = ""
                    Series_contributor = []
                    Series_sample_id = []
                    Series_contact_institute = ""
                    Series_supplementary_file = []
                    Series_platform_id = []
                    Series_relation = []
                    Platform_title = []
                    Platform_technology = []
                    Platform_organism = []
                    if "PLATFORM" in soft_file:
                        platform_ids = list(soft_file["PLATFORM"].keys())

                        for platform_id in platform_ids:
                            # Getting platform organism
                            Platform_organism.append(
                                soft_file["PLATFORM"][platform_id].get("Platform_organism"))
                            Platform_organism = [
                                x for x in Platform_organism if x is not None]
                            try:
                                Platform_organism = list(
                                    set(Platform_organism))
                            except:
                                Platform_organism = list(
                                    itertools.chain(*Platform_organism))
                                Platform_organism = list(
                                    set(Platform_organism))
                            # Getting platform technology
                            Platform_technology.append(
                                soft_file["PLATFORM"][platform_id].get("Platform_technology"))
                            Platform_technology = [
                                x for x in Platform_technology if x is not None]
                            try:
                                Platform_technology = list(
                                    set(Platform_technology))
                            except:
                                Platform_technology = list(
                                    itertools.chain(*Platform_technology))
                                Platform_technology = list(
                                    set(Platform_technology))

                            # Getting platform title
                            Platform_title.append(
                                soft_file["PLATFORM"][platform_id].get("Platform_title"))
                            Platform_title = [
                                x for x in Platform_title if x is not None]
                            try:
                                Platform_title = list(
                                    set(Platform_title))
                            except:
                                Platform_title = list(
                                    itertools.chain(*Platform_title))
                                Platform_title = list(
                                    set(Platform_title))

                    if "SERIES" in soft_file:
                        Series_title = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_title", "")
                        Series_status = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_status", "")
                        Series_pubmed_id = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_pubmed_id", [])
                        Series_web_link = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_web_link", "")
                        Series_summary = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_summary", "")
                        Series_overall_design = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_overall_design", "")
                        Series_type = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_type", "")
                        Series_contributor = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_contributor", [])
                        Series_sample_id = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_sample_id", [])
                        Series_contact_institute = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_contact_institute", "")
                        Series_supplementary_file = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_supplementary_file", [])
                        Series_platform_id = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_platform_id", [])
                        Series_relation = soft_file["SERIES"][gse_id['gse_id']].get(
                            "Series_relation", [])
                    if len(str(Series_supplementary_file)) > 50000:
                        Series_supplementary_file = "Manually extract"

                    data_to_add = [
                        str(gse_id['gse_id']),
                        str(Series_title),
                        str(Series_status),
                        str(Series_pubmed_id),
                        str(Series_web_link),
                        str(Series_summary),
                        str(Series_overall_design),
                        str(Series_type),
                        str(Series_contributor),
                        str(Series_sample_id),
                        str(Series_contact_institute),
                        str(Series_supplementary_file),
                        str(Series_platform_id),
                        str(Series_relation),
                        str(Platform_title),
                        str(Platform_technology),
                        str(Platform_organism)
                    ]
                    if len(Series_sample_id) > 350:
                        for index in range(int(len(Series_sample_id) / 350) + 1):
                            data_to_add = [
                                str(gse_id['gse_id'] + "-" + str(index)),
                                str(Series_title),
                                str(Series_status),
                                str(Series_pubmed_id),
                                str(Series_web_link),
                                str(Series_summary),
                                str(Series_overall_design),
                                str(Series_type),
                                str(Series_contributor),
                                str(Series_sample_id[index *
                                    350: index * 350 + 350]),
                                str(Series_contact_institute),
                                str(Series_supplementary_file),
                                str(Series_platform_id),
                                str(Series_relation),
                                str(Platform_title),
                                str(Platform_technology),
                                str(Platform_organism)
                            ]
                            sheet_instance.write_to_worksheet(
                                gse_id_info_sheet, data_to_add)
                    else:
                        sheet_instance.write_to_worksheet(
                            gse_id_info_sheet, data_to_add)
                    last_added_gse_id = gse_id['gse_id']
                else:
                    print("Skippig GSE ID " + gse_id['gse_id'])

                if last_added_gse_id == gse_id['gse_id']:
                    last_gse_id_found = True
            all_finished = True
        except Exception as err:
            print(traceback.format_exc())


def add_series_to_mongo():
    geo_mongo_instance = geo_mongo.GeoMongo()
    geo_mongo_instance.first_time_add_geo_series()


def add_series_metadata_to_mongo():
    geo_mongo_instance = geo_mongo.GeoMongo()
    geo_mongo_instance.first_time_add_series_metadata()



def update_or_update_series():

    start_from_begining = False
    while True:
        try:
            geo_mongo_instance = geo_mongo.GeoMongo()

            loop_state = geo_mongo_instance.state_management_system_collection.find_one(
                {"db_asso": "all_geo_series"})
            if start_from_begining:
                last_pattern = None
                last_gse_id = None
            else:
                last_pattern = loop_state.get("state").get("pattern")
                last_gse_id = loop_state.get("state").get("gse_id")
                start_from_begining = False

            pattern_found = False
            gse_id_found = False

            series_pattern = geo.get_series_parrerns_for_geo()
            for each_pattern in reversed(series_pattern):

                if last_pattern == None:
                    pattern_found = True
                if each_pattern['gse_patten'] == last_pattern:
                    pattern_found = True

                if pattern_found:
                    gse_ids = geo.get_gse_ids_from_pattern(
                        each_pattern['gse_patten'])
                    for gse_id in gse_ids:

                        if last_gse_id == None:
                            gse_id_found = True

                        if gse_id_found and geo.has_soft_file(gse_id['gse_id']):
                            selected_one = geo_mongo_instance.all_geo_series_collection.find_one(
                                {"_id": gse_id.get('gse_id')})
                            if selected_one == None:
                                data_to_add = {
                                    "_id": gse_id.get('gse_id'),
                                    "gse_patten": each_pattern['gse_patten'],
                                    "gse_id": gse_id.get('gse_id'),
                                    "last_updated": gse_id['last_modified'],
                                    "status": "not_up_to_date"
                                }
                                geo_mongo_instance.all_geo_series_collection.insert_one(
                                    data_to_add)
                            else:
                                if (selected_one.get("status") == "not_up_to_date"):
                                    print("No db update")
                                    continue

                                if (selected_one.get("last_updated") == gse_id['last_modified']):
                                    geo_mongo_instance.all_geo_series_collection.update_one(
                                        {"_id": gse_id.get('gse_id')}, {"$set": {"status": "up_to_date"}}, upsert=True)
                                else:
                                    geo_mongo_instance.all_geo_series_collection.update_one(
                                        {"_id": gse_id.get('gse_id')}, {"$set": {"status": "not_up_to_date"}}, upsert=True)
                        else:
                            print("Skip GSE ID " + gse_id.get('gse_id'))

                        if gse_id['gse_id'] == last_gse_id:
                            gse_id_found = True

                        geo_mongo_instance.state_management_system_collection.update_one({"db_asso": "all_geo_series"}, {
                            "$set": {"state": {"pattern": each_pattern['gse_patten'], "gse_id": gse_id['gse_id']}}}, upsert=True)
                else:
                    print("Skip Patten " + each_pattern['gse_patten'])

                if each_pattern['gse_patten'] == series_pattern[0]:
                    start_from_begining = True

        except Exception as err:
            print(traceback.format_exc())


