#!/usr/local/bin/python3.6
import re
import sys

screen_name_pattern = re.compile(r'@(.*)\[')


def main():
	screen_names = {}

	# 2018-12-16 07:01:53,338 [INFO] tweets_producer: Received 1 tweet from @AMilena8891[453810289]
	for line in sys.stdin:
		if 'tweet from' not in line:
			continue
		line = line.rstrip()

		name = re.search(screen_name_pattern, line)
		if name:
			name = name.group(1)
		else:
			print(f'Screen name was not found in "{line}"')
			continue

		screen_names[name] = screen_names.get(name, 0) + 1

	print(f'\n'.join(
		map(str, reversed(sorted(screen_names.items(), key=lambda item: item[0])))
	))


if __name__ == '__main__':
	main()

