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
from mutagen.id3 import ID3

class Importer:

	def __init__(self):

		self.widgets = [' [', progressbar.Timer(format= 'elapsed time: %(elapsed)s'), '] ', 
			progressbar.Bar('#'),' (', progressbar.ETA(), ') ', ]
  
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node')


	def line_count(self, input_path):
		"""
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
	
	def index(self, input_path, output_path):
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
			reader = csv.DictReader(validated_file, delimiter='\t')
			bar = progressbar.ProgressBar(max_value=file_length, widgets=self.widgets).start()
			for (i, row) in enumerate(reader):
				clip_path = self.path_join(clips_path, row['path'])
				clip_res = self._client.add(clip_path, only_hash=True)			
				sent_hash = self._client.add_str(row['sentence'], opts={'only_hash':True})			

		#		print('c:',clip_res)
		#		print('s:',sent_hash)
				if sent_hash not in clip_index:
					clip_index[sent_hash] = []
				clip_index[sent_hash].append(clip_res['Hash'])
				bar.update(i)
				


	def close(self):
		self._client.close()
	
if __name__ == "__main__":
	imp = Importer()
	imp.index(sys.argv[1], sys.argv[2])
	imp.close()
