import gspread
import datetime
import time


class GoogleSheetUpdater():

    def __init__(self):
        self.client = gspread.service_account(
            filename="/Users/sabugeorge-elucidata/Personal/GEO-parsed/geo-parse-94bb7bec29f4.json")
        self.number_writes = 0
        self.start_time = None
        self.end_time = None

    def get_gse_id_worksheet(self):

        sheet_name = 'geo_rna_seq_data'
        worksheet_name = 'GSE_IDS'
        worksheet = None

        try:
            sheet = self.client.open(sheet_name)
            worksheet = sheet.worksheet(worksheet_name)
        except:
            raise Exception("Create a sheet with name " + sheet_name + " and with worksheet name " +
                            worksheet_name + " with headers gse_patten, GSE_ID, last_updated")

        return worksheet

    def get_gse_id_info_worksheet(self):

        sheet_name = 'geo_rna_seq_data'
        worksheet_name = 'GSE_ID_info'
        worksheet = None

        try:
            sheet = self.client.open(sheet_name)
            worksheet = sheet.worksheet(worksheet_name)
        except:
            raise Exception("Create a sheet with name " + sheet_name + " and with worksheet name " + worksheet_name +
                            " with headers GSE_ID, platform_ids, sample_ids, organism, pubmed_id, contributors, summary, title, type, platform_technology, overall_design, journal_name, institut, pubmed_contributors")

        return worksheet

    def get_gse_id_sample_info_worksheet(self):

        sheet_name = 'geo_rna_seq_data'
        worksheet_name = 'GSE_sample_info'
        worksheet = None

        try:
            sheet = self.client.open(sheet_name)
            worksheet = sheet.worksheet(worksheet_name)
        except:
            raise Exception("Create a sheet with name " + sheet_name + " and with worksheet name " + worksheet_name +
                            " with headers GSE_ID, platform_ids, sample_ids, organism, pubmed_id, contributors, summary, title, type, platform_technology, overall_design, journal_name, institut, pubmed_contributors")

        return worksheet

    def write_to_worksheet(self, worksheet, data_to_write):
        number_of_writes = 20
        # Logic for timeout issue
        if self.number_writes == 0:
            self.start_time = datetime.datetime.now()

        if self.number_writes == number_of_writes:
            self.number_writes = 0
            self.end_time = datetime.datetime.now()
            time_taken = self.end_time - self.start_time
            time_taken_in_sec = time_taken.seconds
            if time_taken_in_sec < number_of_writes:
                time.sleep(60 - time_taken_in_sec)

        worksheet.append_row(data_to_write)

        self.number_writes = self.number_writes + 1
