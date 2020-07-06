# https://stackoverflow.com/questions/18936554/disable-scientific-notation-in-python-json-dumps-output
# Superset xuất json gây sai số với các số có giá trị lớn, chuyển số sang dạng có e
# File ảnh hưởng:
#   +   models/slice.py

import simplejson as json

def pretty_float_json_dumps(json_obj):
    dumps_str = ""

    if isinstance(json_obj, dict): 
        dumps_str += "{"
        for k,v in json_obj.items():
            dumps_str += json.dumps(k)+":"
            if isinstance(v, float): 
                float_tmp_str = ("%.16f" % v).rstrip("0")
                dumps_str += (float_tmp_str+'0' if float_tmp_str.endswith('.') else float_tmp_str) + ','
            elif isinstance(v, list) or isinstance(v, dict): 
                dumps_str += pretty_float_json_dumps(v)+','
            else:
                dumps_str += pretty_float_json_dumps(v)+','
        if dumps_str.endswith(','):
            dumps_str = dumps_str[:-1]
        dumps_str += "}"
    elif isinstance(json_obj, list): 
        dumps_str += "["
        for v in json_obj:
            if isinstance(v, float): 
                float_tmp_str = ("%.16f" % v).rstrip("0")
                dumps_str += (float_tmp_str+'0' if float_tmp_str.endswith('.') else float_tmp_str) + ','
            elif isinstance(v, list) or isinstance(v, dict): 
                dumps_str += pretty_float_json_dumps(v)+','
            else:
                dumps_str += pretty_float_json_dumps(v)+','
        if dumps_str.endswith(','):
            dumps_str = dumps_str[:-1]
        dumps_str += "]"
    else:
        dumps_str += json.dumps(json_obj)
    return dumps_str