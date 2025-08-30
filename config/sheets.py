"""Google Sheets configuration"""

SHEETS_CONFIG = {
    "main_sheets": {
        'Doctors': '1STHimb0IJ077iuBtTOwsa-GD8jStjU3SiBW7yBWom-E',
        'Delighters': '1PV0ZmobUYKHGZvHC7IfJ1t6HrJMTFi6YRbpISCouIfQ',
        'CC Sales': '1te1fbAXhURIUO0EzQ2Mrorv3a6GDtEVM_5np9TO775o',
        'CC Resolvers': '1QdmaTc5F2VUJ0Yu0kNF9d6ETnkMOlOgi18P7XlBSyHg',
        'Filipina': '1E5wHZKSDXQZlHIb3sV4ZWqIxvboLduzUEU0eupK7tys',
        'African': '1__KlrVjcpR8RoYfTYMYZ_EgddUSXMhK3bJO0fTGwDig',
        'Ethiopian': '1ENzdgiwUEtBSb5sHZJWs5aG8g2H62Low8doaDZf8s90',
        'MV Resolvers': '1XkVcHlkh8fEp7mmBD1Zkavdp2blBLwSABT1dE_sOf74',
        'MV Sales': '1agrl9hlBhemXkiojuWKbqiMHKUzxGgos4JSkXxw7NAk'
    },
    "sa_sheets": {
        'African': '1ygyak-GQINyUMnUf828KLBZX_U0pTvVAOmsSPZdH0dA',
        'CC Resolvers': '1qL3qWGNfIJZek6ZDr5g6yauegdt5Y8zDXDNxRroGK84',
        'CC Sales': '13YEU9kJEX7LFp8KnbY6XXnGIvW5H574qIljy_GN4Yxs',
        'Delighters': '1FmA1MfDQGQP0BGVJF0aW8d7IPd1L9Hd7RUa98CsUXOM',
        'Doctors': '1JIQCPsMn8fMw1UwcNUSoLjgBtfShbDIxjJUtWOLBIxw',
        'Ethiopian': '19ZK3agSB_R8cGbk-0bxITB0DeTN1NetOjFzhxaj90oo',
        'Filipina': '1Shz1_H7ifpZIT9jzhxDy4zIA_Qth2n8aiclsUusZgdQ',
        'MV Resolvers': '1fkF4xglbJaMOOFr7wdOpvaGmrL5IOJF19NXwOV6YOg0',
        'MV Sales': '1PvWLMDV6hMGtQfcVFyXAjzOPNAo_kaV9D9n0BFpKw9Q',
    },
    "rule_breaking_sheets": {
        'Doctors': '1V2d9vw_VFcAdTlLtkXRTpi4xJupLHQhsMvrTRtKSjgI',
        'MV Sales': '1vLLhy31Mu28aWOXtRoWMwpGHwIvalcReUO4C-zIGm7Q',
        'MV Resolvers': '1jNZeBGOjz6MUevrbadTBxRUb3OJ8PN1Kh1NHNeotNsM',
        'CC Sales': '1iqVKp4O6Tp4C4_Humy88FAQPlCJgQncS59Foxa1fiSo',
        'Ethiopian': '1AkPaP_Z6qtlHYzXCScUmMxuf9Jxm0nXvYYaTJtB7lcQ',
        'African': '1I4pglnJ9HFEsWXi_IDXc4I8-L55WZgLtn-LXf01IAwU',
        'Filipina': '1ADrFeuqrq9O6quOCcjdkseiWU1H2q5a13_Gxri1mUao',
        'Delighters': '1Zjvd2tGbs7ibmOv8V62Y6sYlL_INu5IgvVRR-Q5Nnq4',
        'CC Resolvers': '19GiEzoFz81sZ1rRYHkvabh_yNvtEndhnjtFDvmKt_bM'
    },
    "loss_of_interest_sheets": {
        'Filipina': '1GJXyFqiM1gn-jbWVEVI3TfIrFU3Ld8BQOPQFb-gMDaE'
    }
}

class SheetsManager:
    def __init__(self, credentials_path='credentials.json'):
        self.credentials_path = credentials_path
        self.config = SHEETS_CONFIG
    
    def get_sheet_id(self, department, sheet_type="main"):
        sheet_type_key = f"{sheet_type}_sheets"
        if sheet_type_key not in self.config:
            raise ValueError(f"Unknown sheet type: {sheet_type}")
        sheets = self.config[sheet_type_key]
        if department not in sheets:
            raise ValueError(f"No {sheet_type} sheet found for department: {department}")
        return sheets[department]
    
    def get_all_departments_for_type(self, sheet_type="main"):
        sheet_type_key = f"{sheet_type}_sheets"
        if sheet_type_key not in self.config:
            return []
        return list(self.config[sheet_type_key].keys())

SHEET_MANAGER = SheetsManager()
