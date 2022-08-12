"""
This module contains DB models related
to dictionaries like WhoDrug, MedDra etc.
"""


from .dictionary_types import DictionaryTypes
from .terms_factory_interface import TermsFactoryInterface

__all__ = [
    "DictionaryTypes",
    "TermsFactoryInterface",
]
