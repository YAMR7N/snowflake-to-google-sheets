# Configuration package
from .departments import DEPARTMENTS, get_all_department_names
from .sheets import SHEET_MANAGER  
from .settings import SETTINGS

__all__ = ['DEPARTMENTS', 'get_all_department_names', 'SHEET_MANAGER', 'SETTINGS']
