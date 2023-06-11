from geo import geo


def get_diff_between_geo_and_all_geo_series_sync_info(gse_pattern_list, get_gse_status):
    for each_pattern in gse_pattern_list:
        gse_ids = geo.get_gse_ids_from_pattern(each_pattern['gse_patten'])
        all_series_data_to_add = []
        all_series_data_to_update = []
        for gse_id in gse_ids:
            print(gse_id['gse_id'])
            if geo.has_soft_file(gse_id['gse_id']):
                selected_one = get_gse_status.get(gse_id['gse_id'])
                last_updated_date = geo.get_series_metadata_from_soft_file(gse_id.get(
                    'gse_id')).get("SERIES").get(gse_id.get('gse_id')).get("Series_last_update_date")
                if selected_one == None:
                    data_to_add = {
                        "_id": gse_id.get('gse_id'),
                        "gse_patten": each_pattern['gse_patten'],
                        "gse_id": gse_id.get('gse_id'),
                        "last_updated": last_updated_date,
                        "status": "not_up_to_date",
                        "access": "public",
                        "sample_status": "invalid"
                    }
                    all_series_data_to_add.append(data_to_add)
                elif not(selected_one.get("last_updated") == last_updated_date):
                    update_to_add =  {"status": "not_up_to_date"}
                    all_series_data_to_update.append(update_to_add)
                    print(gse_id)
                    print("GSE ID has to be updated: " +
                      gse_id.get('gse_id'))
                    exit(0)
            else:
                data_to_add = {
                    "_id": gse_id.get('gse_id'),
                    "gse_patten": each_pattern['gse_patten'],
                    "gse_id": gse_id.get('gse_id'),
                    "last_updated": '-',
                    "status": "up_to_date",
                    "access": "private",
                    "sample_status": "valid"
                }
                all_series_data_to_add.append(update_to_add)
                print("GSE ID is private: " +
                      gse_id.get('gse_id'))
                exit(0)
        yield all_series_data_to_add, all_series_data_to_update
