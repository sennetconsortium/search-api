import logging

from atlas_consortia_commons.object import build_enum_class
from atlas_consortia_commons.ubkg import get_from_node
from atlas_consortia_commons.string import to_snake_case_upper

from flask import current_app

logger = logging.getLogger(__name__)


ubkg_instance = None

def _set_instance(_ubkg):
    global ubkg_instance
    ubkg_instance = _ubkg

def _get_instance():
    return ubkg_instance if ubkg_instance is not None else current_app.ubkg

def _get_obj_type(in_enum, as_data_dict: bool = False):
    if as_data_dict:
        return 'dict'
    else:
        return 'enum' if in_enum else 'class'


def _get_response(obj):
    if type(obj) is not str and get_from_node(obj, 'endpoint'):
        return _get_instance().get_ubkg_by_endpoint(obj)
    else:
        return _get_instance().get_ubkg_valueset(obj)


def _build_enum_class(name: str, obj, key: str = 'term', val_key: str = None, prop_callback=to_snake_case_upper, data_as_val=False, obj_type: str = 'class'):
    response = _get_response(obj)
    return build_enum_class(name, response, key, val_key=val_key, prop_callback=prop_callback, obj_type=obj_type, data_as_val=data_as_val)


def entities(in_enum: bool = False, as_data_dict: bool = False):
    return _build_enum_class('Entities', _get_instance().entities, obj_type=_get_obj_type(in_enum, as_data_dict))


def specimen_categories(in_enum: bool = False, as_data_dict: bool = False,
                        prop_callback=to_snake_case_upper, data_as_val=False):
    return _build_enum_class('SpecimenCategories', _get_instance().specimen_categories,
                             obj_type=_get_obj_type(in_enum, as_data_dict), prop_callback=prop_callback, data_as_val=data_as_val)


def organ_types(in_enum: bool = False, as_data_dict: bool = False,
                prop_callback=to_snake_case_upper, data_as_val=False):
    return _build_enum_class('OrganTypes', _get_instance().organ_types, key='rui_code', val_key='term',
                             obj_type=_get_obj_type(in_enum, as_data_dict), prop_callback=prop_callback, data_as_val=data_as_val)


def assay_types(in_enum: bool = False, as_data_dict: bool = False,
                prop_callback=to_snake_case_upper, data_as_val=False):
    return _build_enum_class('AssayTypes', _get_instance().assay_types, key='data_type',
                             obj_type=_get_obj_type(in_enum, as_data_dict), prop_callback=prop_callback, data_as_val=data_as_val)


def source_types(in_enum: bool = False, as_data_dict: bool = False):
    return _build_enum_class('SourceTypes', _get_instance().source_types,
                             obj_type=_get_obj_type(in_enum, as_data_dict))


def init_ontology():
    specimen_categories()
    organ_types()
    entities()
    assay_types()
    source_types()


def enum_val(val):
    return val.value


class Ontology:
    @staticmethod
    def entities(as_arr: bool = False, cb=str, as_data_dict: bool = False):
        return Ontology._as_list_or_class(entities(as_arr, as_data_dict), as_arr, cb)

    @staticmethod
    def assay_types(as_arr: bool = False, cb=str, as_data_dict: bool = False,
                    prop_callback=to_snake_case_upper, data_as_val=False):
        return Ontology._as_list_or_class(assay_types(as_arr, as_data_dict, prop_callback,
                                                      data_as_val=data_as_val), as_arr, cb)

    @staticmethod
    def specimen_categories(as_arr: bool = False, cb=str, as_data_dict: bool = False,
                            prop_callback=to_snake_case_upper, data_as_val=False):
        return Ontology._as_list_or_class(specimen_categories(as_arr, as_data_dict, prop_callback,
                                                              data_as_val=data_as_val), as_arr, cb)

    @staticmethod
    def organ_types(as_arr: bool = False, cb=str, as_data_dict: bool = False,
                    prop_callback=to_snake_case_upper, data_as_val=False):
        return Ontology._as_list_or_class(organ_types(as_arr, as_data_dict, prop_callback,
                                                      data_as_val=data_as_val), as_arr, cb)

    @staticmethod
    def source_types(as_arr: bool = False, cb=str, as_data_dict: bool = False):
        return Ontology._as_list_or_class(source_types(as_arr, as_data_dict), as_arr, cb)

    @staticmethod
    def _as_list_or_class(obj, as_arr: bool = False, cb=str):
        return obj if not as_arr else list(map(cb, obj))

    @staticmethod
    def set_instance(_ubkg):
        _set_instance(_ubkg)