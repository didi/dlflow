from pathlib import Path
from collections import OrderedDict
import time
import argparse
import ast
import os
import re
import sys


I18N_NAME = "i18n"

TITLE = """
msgid ""
msgstr ""
"Project-Id-Version: DLFlow {version}\\n"
"POT-Creation-Date: {date}\\n"
"PO-Revision-Date: {date}\\n"
"Content-Type: text/plain; charset=UTF-8\\n"
"Content-Transfer-Encoding: 8bit\\n"\n
"""

NOTE_TMPL = "# {} - lineno: {}"

PO_TMPL = """
{note}
msgid "{id}"
msgstr "{str}"
"""

EXTRACT = re.compile(r"[\'\"](.*)[\'\"]").findall

MSG = OrderedDict()


def extract_msg(s):
    _res = EXTRACT(s)
    res = _res[0] if _res else ""

    return res


def load_msg_map(file):
    po_str = Path(file).read_text()

    m = map(lambda x: x.strip(), po_str.split("\n"))
    msg_strs = [s for s in m if s and not s.startswith("#")]

    id2str = {}
    msgid, msgstr = None, None

    for s in msg_strs:
        _s = extract_msg(s)

        if s.startswith("msgid"):
            id2str[msgid] = msgstr
            msgid, msgstr = _s, None

        elif s.startswith("msgstr"):
            msgstr = _s

        else:
            if msgstr is None:
                msgid += _s
            else:
                msgstr += _s

    id2str[msgid] = msgstr

    return id2str


def parser_i18n_call(node):
    string = node.args[0]
    msg = extract_msg(repr(string.s))

    return string.lineno, msg


def extract_i18n_info(tree):
    info = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if isinstance(node.func, ast.Name):
            if node.func.id == I18N_NAME:
                info.append(parser_i18n_call(node))

    info.sort(key=lambda x: x[0])

    return info


def parser_file(file: Path):
    source = file.read_text()
    tree = ast.parse(source)

    i18n_info = extract_i18n_info(tree)

    for lineno, msgid in i18n_info:
        if msgid in MSG:
            MSG[msgid] = "\n".join([MSG[msgid],
                                    NOTE_TMPL.format(file, lineno)])
        else:
            MSG[msgid] = NOTE_TMPL.format(file, lineno)


def gen_po_str(msg_map):
    res = []

    for msgid, note in MSG.items():
        msgstr = msg_map.get(msgid, "")

        res.append(
            PO_TMPL.format(note=note, id=msgid, str=msgstr))

    return res


def extractor(source: Path, po_file=None):
    sys.path.append("./")
    from dlflow import VERSION

    msg_map = load_msg_map(po_file) if po_file else dict()

    if source.is_dir():
        for cur_dir, _, files in os.walk(source.as_posix()):
            cur_dir = Path(cur_dir)

            for _file in files:
                file = cur_dir.joinpath(_file)

                if file.suffix == ".py":
                    parser_file(file)

    if source.is_file() and source.suffix == ".py":
        parser_file(source)

    _res = gen_po_str(msg_map)
    res = [
        TITLE.format(
            version=VERSION,
            date=time.strftime("%Y-%m-%d %H:%M%z", time.localtime()))]
    res.extend(_res)

    return res


def args_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--output",
                        required=True,
                        help="Output.")
    parser.add_argument("-i", "--input",
                        required=True,
                        help="Input file or directory.")

    parser.add_argument("-m", "--merge",
                        help="Merge old po-file.")

    opt = parser.parse_args()

    return opt


def main():
    args = args_parser()

    source = Path(args.input)
    output = Path(args.output)

    po_list = extractor(source, po_file=args.merge)

    output.write_text("".join(po_list))


if __name__ == "__main__":
    main()
