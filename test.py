from geo import model_data
from helpers import general_helper

general_helper.save_pmc_tar_path()

x = model_data.ModelData()
a, b = x.extract_pmc_metadata("PMC6478402")
