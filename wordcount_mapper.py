#!/usr/bin/env python

import re, sys

for line in sys.stdin:						# The script receives as input the content of the file line by line.
	for word in line.strip().split():		# Remove the trailing new-line character from every line and split it into a list of words.
		if re.match('[a-zA-Z]+', word) and len(word) > 1:		# Check if word consits of only alphabetic characters.
			print('{}\t1'.format(word))		# Print every word tab one to the script's output (for every single word occurrence)
