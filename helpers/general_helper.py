import inspect
import re

def parse_arguments_from_docstring(func):
    docstring = inspect.getdoc(func)
    arg_pattern = r'\s+([\w_]+)\s*\(([^)]+)\):\s*([^:\n]+)'
    type_pattern = r'([^:,]+)'
    arg_matches = re.findall(arg_pattern, docstring)
    arguments = {}
    for arg_match in arg_matches:
        arg_name = arg_match[0]
        arg_type = ""

        type_match = re.search(type_pattern, arg_match[1])
        if type_match:
            arg_type = type_match.group(1).strip()

        arg_description = arg_match[2].strip()

        arguments[arg_name] = {"type": arg_type, "description": arg_description}

    return arguments