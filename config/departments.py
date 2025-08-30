"""Centralized department configuration"""

DEPARTMENTS = {
    "MV Resolvers": {"tableau_view": "MV Department", "skills": ["GPT_MV_RESOLVERS"], "clean_name": "mv_resolvers"},
    "CC Resolvers": {"tableau_view": "CC Department", "skills": ["GPT_CC_RESOLVERS"], "clean_name": "cc_resolvers"},
    "Filipina": {"tableau_view": "Applicants", "skills": ["MAIDSAT_FILIPINA_GPT", "GPT_MAIDSAT_FILIPINA_UAE", "GPT_MAIDSAT_FILIPINA_OUTSIDE", "GPT_MAIDSAT_FILIPINA_PHILIPPINES", "filipina_outside_pending_facephoto", "filipina_outside_pending_passport", "filipina_outside_uae_pending_joining_date", "filipina_in_phl_pending_valid_visa", "filipina_in_phl_pending_passport", "filipina_in_phl_pending_facephoto", "filipina_in_phl_pending_oec_from_maid", "filipina_in_phl_pending_oec_from_company"], "clean_name": "filipina"},
    "African": {"tableau_view": "Applicants", "skills": ["GPT_MAIDSAT_AFRICAN_KENYA", "GPT_MAIDSAT_AFRICAN_OUTSIDE", "GPT_MAIDSAT_AFRICAN_UAE"], "clean_name": "african"},
    "CC Sales": {"tableau_view": "Sales CC", "skills": ["GPT_CC_PROSPECT"], "clean_name": "cc_sales"},
    "MV Sales": {"tableau_view": "Sales MV", "skills": ["GPT_MV_PROSPECT"], "clean_name": "mv_sales"},
    "Delighters": {"tableau_view": "Delighters", "skills": ["GPT_Delighters"], "clean_name": "delighters"},
    "Doctors": {"tableau_view": "Doctors", "skills": ["GPT_Doctors"], "clean_name": "doctors"},
    "Ethiopian": {"tableau_view": "Applicants", "skills": ["MAIDSAT_ETHIOPIAN_GPT", "GPT_MAIDSAT_ETHIOPIA_ETHIOPIA", "GPT_MAIDSAT_ETHIOPIA_OUTSIDE", "GPT_MAIDSAT_ETHIOPIA_UAE", "Ethiopian Assessment", "Ethiopian Invalid Passport", "Ethiopian Failed Question Assessment", "Ethiopian Passed Question Assessment"], "clean_name": "ethiopian"}
}

def get_all_department_names():
    return list(DEPARTMENTS.keys())

def get_department_config(dept_name):
    return DEPARTMENTS.get(dept_name)
