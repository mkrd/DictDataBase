from . writing import  multisession, subsession
from . models import SubModel, DDBMethodChooser


def at(*path):
    return DDBMethodChooser(*path)
