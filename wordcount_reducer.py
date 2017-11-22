#!/usr/bin/env python

import sys

# Initially no word is current_word and current count is zero.
current_word = None
current_count = 0  

# For every line extract word and number of occurrences of that word.
for line in sys.stdin:				
	word, count = line.split('\t')  		# Split every line into word and number of occurrences.
	
	try:
		count = int(count)					# Convert count from string to integer.
	except ValueError:
		continue							# In case the count string does not represent a number, silently ignore it.
	
	if word == current_word:
		current_count += count
	else:	
		if current_word:	# Avoid printing for the default None value of current_value.
			print('{},{}'.format(current_word, current_count))
		
		current_word = word
		current_count = count

# Print the last word and its number of occurrences if any words have been passed to the script's input.
if current_word:											
	print('{},{}'.format(current_word, current_count))		
