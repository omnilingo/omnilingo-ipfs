#!/usr/bin/env python3
"""Import a Common Voice dump into IPFS generating an index of CIDs."""
import csv
import hashlib
import ipfshttpclient
from itertools import (takewhile,repeat)
import json
import progressbar
import re
import sys

from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3

class Importer:

	def __init__(self):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node')
			sys.exit(-1)


	def line_count(self, input_path):
		"""
		Efficiently count the number of lines in a file
		input_path: path to count the number of lines in
		"""
		f = open(input_path, 'rb')
		bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
		res = sum( buf.count(b'\n') for buf in bufgen )
		f.close()
		return res

	def path_join(self, *args, sep='/'):
		"""
		Join a sequence of arguments on a given delimiter
		*args: any number of strings
		sep: directory separator
		"""
		return sep.join(args)

	def hashify(self, input_path, output_path, dryrun=False):
		"""
		Import a Common Voice dump into IPFS
		input_path: path to a Common Voice dump directory
		output_path: place to put the generated index in JSON
		"""

		clip_index = {}

		print(input_path)
		print(output_path)

		validated_path = self.path_join(input_path, 'validated.tsv')
		clips_path = self.path_join(input_path, 'clips')

		file_length = self.line_count(validated_path)

		with open(validated_path, newline='') as validated_file:
			opts = {}
			if dryrun: 
				opts={'only_hash': True}
			reader = csv.DictReader(validated_file, delimiter='\t')
			bar = progressbar.ProgressBar(max_value=file_length).start()
			for (i, row) in enumerate(reader):
				sentence = {
					'content': row['sentence'],
					'language': row["locale"],
					'copyright': "CC0-1.0"
				}
				sent_hash = self._client.add_json(sentence, opts=opts)
				clip_path = self.path_join(clips_path, row['path'])
				audio = EasyID3(clip_path)
				audio["copyright"] = "CC0-1.0"
				audio["language"] = row["locale"]
				audio["album"] = sent_hash
				audio["author"] = row["client_id"]
				audio.save()
				clip_res = self._client.add(clip_path, opts=opts)

				if sent_hash not in clip_index:
					clip_index[sent_hash] = []
				clip_index[sent_hash].append(clip_res['Hash'])
				bar.update(i)

		# Save the transcript → clip hash as a json file
		with open(output_path, 'w') as output_file:
			json.dump(clip_index, output_file)

	def close(self):
		"""Close the TCP connection to IPFS"""
		self._client.close()

if __name__ == "__main__":
	imp = Importer()
	if len(sys.argv) != 3:
		print('importer.py dataset_dir index_path')
		sys.exit(-1)
	dataset_dir = sys.argv[1]
	index_path = sys.argv[2]
	imp.hashify(dataset_dir, index_path, dryrun=False)
	imp.close()
