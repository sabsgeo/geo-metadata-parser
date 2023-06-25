import os
import inspect
import re
import io
import tarfile

try:
    from collections.abc import Iterable
except:
    from collections import Iterable

from six import string_types

import requests
import sqlite3
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

        arguments[arg_name] = {"type": arg_type, "description": arg_description}

    return arguments

def save_pmc_tar_path():
    url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt"
    latest_tar_time = pmc.get_latest_pmc_updated_tar_time()
    pmc_file_name = "oa_file_list_{}.db".format(latest_tar_time)
    pmc_list_path = os.path.join(os.getcwd(), pmc_file_name)
    if not(os.path.exists(pmc_list_path)):
        print("Adding the database")
        con = sqlite3.connect(pmc_list_path)
        cur = con.cursor()
        cur.execute("CREATE TABLE oa_file_list(path TEXT, source TEXT, pmc_id TEXT, pmid TEXT, state TEXT, PRIMARY KEY (pmc_id))")

        response = requests.get(url, stream=True)
        input_data = []
        for line in response.iter_lines():
            if line:
                line_array = line.decode("utf-8").split("\t")
                if len(line_array) == 5:
                    input_data.append(line_array)
        cur.executemany("INSERT INTO oa_file_list (path, source, pmc_id, pmid, state) VALUES (?, ?, ?, ?, ?);", input_data)
        con.commit()
        print("Finished adding database")

def remove_namespace(tree):
    for node in tree.iter():
        try:
            has_namespace = node.tag.startswith("{")
        except AttributeError:
            continue  # node.tag is not a string (node is a comment or similar)
        if has_namespace:
            node.tag = node.tag.split("}", 1)[1]


def read_xml(path, nxml=False):
    try:
        tree = etree.parse(path)
        if ".nxml" in path or nxml:
            remove_namespace(tree)  # strip namespace when reading an XML file
    except:
        try:
            tree = etree.fromstring(path)
        except Exception:
            print(
                "Error: it was not able to read a path, a file-like object, or a string as an XML"
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