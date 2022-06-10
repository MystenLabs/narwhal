#!/usr/bin/env python3
"""Create a docker-compose.yaml file to stdout for --num # of nodes
"""

import sys
import argparse

HEADER = """---
version: "3.9"

services:"""

FOOTER = """...
"""

def main():
    "mainly main."
    parser = argparse.ArgumentParser(description="docker compose config generator")

    parser.add_argument("-n", default=4, type=int, help="number of primary+worker instances")
    parser.add_argument("-t", default="node.template", help="node template file")
    args = parser.parse_args()

    templ = open(args.t).read()

    print(HEADER)

    for i in range(args.n):
        tmp = "{:02d}".format(i)
        print(templ.format(counter=tmp, num=args.n))

    print(FOOTER)

if __name__ == '__main__':
    sys.exit(main())
