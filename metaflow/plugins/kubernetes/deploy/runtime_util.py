from metaflow.parameters import Parameter,parameters
from metaflow.includefile import IncludeFile

def param_to_dictified_string(param_name):
    return param_name.replace('-','_')


def get_real_param_values():
    # $ todo : Ensure that you send a real file path. 
    input_param_dict = dict()
    input_file_dict = dict()
    for param_obj in parameters:
        if isinstance(param_obj,IncludeFile):
            param_name = param_to_dictified_string(param_obj.name)
            input_file_dict[param_name] = {
                'default_path': param_obj.kwargs.get('default') ,# todo : find a way to set it. 
                'actual_name' : param_obj.name
            }
        elif isinstance(param_obj,Parameter):
            param_name = param_to_dictified_string(param_obj.name)
            input_param_dict[param_name] = {
                'actual_name' : param_obj.name
            }
    return input_file_dict,input_param_dict