from geo import geo

import itertools
import ast
import traceback


class ModelData():

    def __init__(self) -> None:
        pass


    def soft_data_type_to_string(self, each_geo_series, metadata):
        if not (metadata in each_geo_series):
            return ""

        if isinstance(each_geo_series[metadata], list):
            return ". ".join(each_geo_series[metadata]).replace(".. ", ". ")
        else:
            return each_geo_series[metadata]

    def soft_data_type_to_list(self, each_geo_series, metadata):
        if not (metadata in each_geo_series):
            return []

        if isinstance(each_geo_series[metadata], list):
            return each_geo_series[metadata]
        else:
            return [each_geo_series[metadata]]

    def extract_all_metadata_info_from_softfile(self, gse_id):
        soft_file = {}
        series_metadata = {}
        all_sample_data = []
        
        try:
            soft_file = geo.read_full_soft_file(gse_id)
        except Exception as err:
            print("Error in parsing " + gse_id)
            print(traceback.format_exc())

        if not (bool(soft_file)):
            return series_metadata, all_sample_data
    

        if "SERIES" in soft_file:
            series_metadata = {
                "_id": gse_id,
                "gse_id": gse_id,
                "Series_title": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_title"),
                "Series_status": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_status"),
                "Series_pubmed_id": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_pubmed_id"),
                "Series_web_link": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_web_link"),
                "Series_summary": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_summary"),
                "Series_overall_design": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_overall_design"),
                "Series_type": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_type"),
                "Series_contributor": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_contributor"),
                "Series_sample_id": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_sample_id"),
                "Series_contact_institute": self.soft_data_type_to_string(soft_file["SERIES"][gse_id], "Series_contact_institute"),
                "Series_supplementary_file": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_supplementary_file"),
                "Series_platform_id": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_platform_id"),
                "Series_relation": self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_relation"),
            }

        if "PLATFORM" in soft_file:
            platform_ids = list(soft_file["PLATFORM"].keys())
            platform_fields = ["Platform_title",
                               "Platform_technology", "Platform_organism"]
            for platform_field in platform_fields:
                series_metadata[platform_field] = []
                for platform_id in platform_ids:
                    series_metadata[platform_field] = series_metadata[platform_field] + \
                        self.soft_data_type_to_list(
                            soft_file["PLATFORM"][platform_id], platform_field)
        else:
            series_metadata["Platform_organism"] = list(set(self.soft_data_type_to_list(
                soft_file["SERIES"][gse_id], "Series_platform_organism") + self.soft_data_type_to_list(soft_file["SERIES"][gse_id], "Series_sample_organism")))
            series_metadata["Platform_technology"] = []
            series_metadata["Platform_title"] = []

        # Sample data collection
        if "SAMPLE" in soft_file:
            for sample_id in soft_file["SAMPLE"].keys():
                channel_data = {}
                each_geo_sample = {
                    "_id": str(gse_id) + "__" + str(sample_id),
                    "gse_id": str(gse_id),
                    "gsm_id": str(sample_id),
                    "Sample_library_selection": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_library_selection"),
                    "Sample_supplementary_file": self.soft_data_type_to_list(soft_file["SAMPLE"][sample_id], "Sample_supplementary_file"),
                    "Sample_description": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_description"),
                    "Sample_type": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_type"),
                    "Sample_title": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_title"),
                    "Sample_scan_protocol": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_scan_protocol"),
                    "Sample_library_source": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_library_source"),
                    "Sample_hyb_protocol": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_hyb_protocol"),
                    "Sample_relation": self.soft_data_type_to_list(soft_file["SAMPLE"][sample_id], "Sample_relation"),
                    "Sample_instrument_model": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_instrument_model"),
                    "Sample_contact_web_link": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_contact_web_link"),
                    "Sample_data_processing": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_data_processing"),
                    "Sample_library_strategy": self.soft_data_type_to_string(soft_file["SAMPLE"][sample_id], "Sample_library_strategy")
                }

                if "Sample_channel_count" in soft_file["SAMPLE"][sample_id]:
                    channel_array = []
                    for channel_count in range(int(soft_file["SAMPLE"][sample_id]["Sample_channel_count"])):
                        updated_channel_count = channel_count + 1
                        channel_key = "ch" + \
                            str(updated_channel_count)
                        channel_array.append(channel_key)
                        if not (channel_key in channel_data.keys()):
                            channel_data[channel_key] = {}
                    channel_array = list(set(channel_array))
                    for sample_keys in soft_file["SAMPLE"][sample_id].keys():
                        last_key = sample_keys.split(
                            "_")[-1]
                        ch_match = [s for s in channel_array if  s in sample_keys]
                        if len(ch_match) > 0:
                            channel_data[last_key][sample_keys] = self.soft_data_type_to_string(
                                soft_file["SAMPLE"][sample_id], sample_keys)

                for channel_sh_keys in channel_data.keys():
                    channel_data[channel_sh_keys]["char_name"] = channel_sh_keys

                each_geo_sample.update(channel_data)
                all_sample_data.append(each_geo_sample)
        else:
            print("Sample not found for GSE ID " + gse_id)

        return series_metadata, all_sample_data
