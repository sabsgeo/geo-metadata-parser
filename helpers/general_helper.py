import inspect
import re
import io
import tarfile

try:
    from collections.abc import Iterable
except:
    from collections import Iterable

from six import string_types

from lxml import etree
from itertools import chain


from geo import pmc


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

        arguments[arg_name] = {"type": arg_type,
                               "description": arg_description}

    return arguments


def remove_namespace(tree):
    for node in tree.iter():
        try:
            has_namespace = node.tag.startswith("{")
        except AttributeError:
            continue  # node.tag is not a string (node is a comment or similar)
        if has_namespace:
            node.tag = node.tag.split("}", 1)[1]


def add_parameters_to_xml(xml_string, tag_name, parameters):

    # Construct the regular expression pattern
    pattern = rf"<{tag_name}\s+(?P<attrs>[^>]*)>"

    # Find the tag in the XML string using regular expressions
    match = re.search(pattern, xml_string)

    if match:
        # Extract the existing attributes
        attrs = match.group("attrs")

        # Add the new parameters
        for key, value in parameters.items():
            attrs = f"{attrs} {key}=\"{value}\""

        # Replace the original tag with the updated attributes
        updated_xml_string = re.sub(
            pattern, rf"<{tag_name} {attrs}>", xml_string)
        return updated_xml_string
    else:
        print(f"Tag '{tag_name}' not found in the XML.")
        return xml_string


def read_xml(path, nxml=False):
    try:
        tree = etree.parse(path)
        if ".nxml" in path or nxml:
            remove_namespace(tree)
    except:
        try:
            tree = etree.fromstring(path)
        except Exception:
            p = {
                "xmlns:mml": "http://www.w3.org/1998/Math/MathML",
                "xmlns:xlink": "http://www.w3.org/1999/xlink"
            }
            try:
                return read_xml(add_parameters_to_xml(path.decode(), "article", p), nxml)
            except:
                print(
                "Error: it was not able to read a path, a file-like object, a string as an XML, or not an link missing issue"
                )
                raise
    return tree


def stringify_affiliation_rec(node):
    parts = _recur_children(node)
    parts_flatten = list(_flatten(parts))
    return " ".join(parts_flatten).strip()


def stringify_children(node):
    parts = (
        [node.text]
        + list(chain(*([c.text, c.tail] for c in node.getchildren())))
        + [node.tail]
    )
    return "".join(filter(None, parts))


def _recur_children(node):
    if len(node.getchildren()) == 0:
        parts = (
            ([node.text or ""] + [node.tail or ""])
            if (node.tag != "label" and node.tag != "sup")
            else ([node.tail or ""])
        )
        return parts
    else:
        parts = (
            [node.text or ""]
            + [_recur_children(c) for c in node.getchildren()]
            + [node.tail or ""]
        )
        return parts


def _flatten(l):
    for el in l:
        if isinstance(el, Iterable) and not isinstance(el, string_types):
            for sub in _flatten(el):
                yield sub
        else:
            yield el


def tar_gz_compress_string(file_name, input_buffer):
    buffer = io.BytesIO()

    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tarinfo = tarfile.TarInfo(file_name)
        tarinfo.size = len(input_buffer)
        tar.addfile(tarinfo, io.BytesIO(input_buffer))

    return buffer.getvalue()
