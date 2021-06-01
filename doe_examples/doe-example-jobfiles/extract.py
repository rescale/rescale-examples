#!/usr/bin/env python

# extract.py

import argparse
import collections
import re
import sys

# parse args
parser = argparse.ArgumentParser(description='Extract the last specified'
                                 ' value of a variable or variables from an '
                                 'OpenFoam output file.')
parser.add_argument('output_file', metavar='output-file',
                    type=argparse.FileType('r'),
                    help='Output file from which to extract variable values.')
parser.add_argument('labels', metavar='variable-label', type=str, nargs='+',
                    help='Variable labels to match for extracting values.')
args = parser.parse_args()
labels = args.labels
output_file = args.output_file

# setup ordered dict to store results
seed = [(label, '*** not found ***') for label in labels]
results = collections.OrderedDict(seed)

# setup regular expressions for label/value matching
# exponential notation
e = '-?(?:\\d+\\.?\\d*|\\.\\d+)[eEdD](?:\\+|-)?\\d+'

# floating point
f = '-?\\d+\\.\\d*|-?\\.\\d+'

# integer
i = '-?\\d+'

# numeric field
value = e + '|' + f + '|' + i

# label
label = '\\w+(?::\\w+)*'

# regular expression for standard output format
regex = re.compile('^\s*(' + label + ')\s{1,}:\s(' + value + ')')

# extract the results from the output file
try:
    for line in output_file:
        m = regex.match(line)
        # store if a match is found and the label is a target
        # overwrite existing values
        if m and m.group(1) in results:
            results[m.group(1)] = m.group(2)
finally:
    output_file.close()

# send to stdout
for k, v in results.items():
    print '%s\t%s' % (k, v)
