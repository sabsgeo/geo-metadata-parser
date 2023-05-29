from geo import geo

import itertools
import ast
import traceback


class ModelData():

    def __init__(self) -> None:
        pass


    def extract_series_metadata_info_from_softfile(self, gse_id):
        soft_file = {}
        try:
            soft_file = geo.read_full_soft_file(gse_id)
        except Exception as err:
            print("Error in parsing " + gse_id)
            print(traceback.format_exc())

        if not(bool(soft_file)):
            return {}

        Series_title = ""
        Series_status = ""
        Series_pubmed_id = []
        Series_web_link = ""
        Series_summary = ""
        Series_overall_design = ""
        Series_type = []
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
            Series_title = soft_file["SERIES"][gse_id].get(
                "Series_title", "")
            Series_status = soft_file["SERIES"][gse_id].get(
                "Series_status", "")
            Series_pubmed_id = soft_file["SERIES"][gse_id].get(
                "Series_pubmed_id", [])
            Series_web_link = soft_file["SERIES"][gse_id].get(
                "Series_web_link", "")
            Series_summary = soft_file["SERIES"][gse_id].get(
                "Series_summary", "")
            Series_overall_design = soft_file["SERIES"][gse_id].get(
                "Series_overall_design", "")
            Series_type = soft_file["SERIES"][gse_id].get(
                "Series_type", [])
            Series_contributor = soft_file["SERIES"][gse_id].get(
                "Series_contributor", [])
            Series_sample_id = soft_file["SERIES"][gse_id].get(
                "Series_sample_id", [])
            Series_contact_institute = soft_file["SERIES"][gse_id].get(
                "Series_contact_institute", "")
            Series_supplementary_file = soft_file["SERIES"][gse_id].get(
                "Series_supplementary_file", [])
            Series_platform_id = soft_file["SERIES"][gse_id].get(
                "Series_platform_id", [])
            Series_relation = soft_file["SERIES"][gse_id].get(
                "Series_relation", [])

        each_geo_series = {
            "gse_id": str(gse_id),
            "Series_title": str(Series_title),
            "Series_status": str(Series_status),
            "Series_pubmed_id": str(Series_pubmed_id),
            "Series_web_link": str(Series_web_link),
            "Series_summary": str(Series_summary),
            "Series_overall_design": str(Series_overall_design),
            "Series_type": str(Series_type),
            "Series_contributor": str(Series_contributor),
            "Series_sample_id": str(Series_sample_id),
            "Series_contact_institute": str(Series_contact_institute),
            "Series_supplementary_file": str(Series_supplementary_file),
            "Series_platform_id": str(Series_platform_id),
            "Series_relation": str(Series_relation),
            "Platform_title": str(Platform_title),
            "Platform_technology": str(Platform_technology),
            "Platform_organism": str(Platform_organism)
        }

        updated_data = {}

        for metadata in each_geo_series:
            if metadata in ["gse_id", "Series_title", "Series_status", "Series_web_link", "Series_contact_institute"]:
                updated_data[metadata] = each_geo_series.get(metadata)
                if updated_data[metadata] == None:
                    updated_data[metadata] = ""
            if metadata in ["Series_summary", "Series_overall_design"]:
                if (not (each_geo_series[metadata] == None) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                    updated_data[metadata] = ". ".join(
                        filter(None, ast.literal_eval(each_geo_series[metadata]))).replace(".. ", ". ")
                elif each_geo_series[metadata] == None:
                    updated_data[metadata] = ""
                else:
                    updated_data[metadata] = each_geo_series[metadata]
            if metadata in ["Series_type", "Series_pubmed_id", "Series_contributor", "Series_sample_id", "Series_supplementary_file", "Series_platform_id", "Series_relation", "Platform_title", "Platform_technology", "Platform_organism"]:
                if not (each_geo_series[metadata] == None) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]") and len(each_geo_series[metadata]) == 2:
                    updated_data[metadata] = []
                elif (not (each_geo_series[metadata] == None) and each_geo_series[metadata].startswith("[") and each_geo_series[metadata].endswith("]")):
                    updated_data[metadata] = list(
                        filter(None, ast.literal_eval(each_geo_series[metadata])))
                else:
                    updated_data[metadata] = [each_geo_series[metadata]]
        updated_data["_id"] = each_geo_series["gse_id"]
        return updated_data