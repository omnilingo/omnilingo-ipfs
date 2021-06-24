#!/usr/bin/env python3
"""Index a Common Voice from IPFS extracting an indexed-list of CIDs."""

import ipfshttpclient
import io
import json
import progressbar
import re
import sys

from mutagen.mp3 import MP3

from cvutils.tokeniser import Tokeniser
from cvutils.tagger import Tagger

TRANSCRIPT_BLACKLIST = ["Hey", "Hei", "Firefox"]
MAX_TEXT_LENGTH = 100  # in characters
MAX_AUDIO_LENGTH = 10  # in seconds
MAX_PER_BUCKET = 1000  # in clips

class Indexer:
	
	def __init__(self, locale):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node')

		self.locale = locale

	def rebucket(self, b):
		""" """
		# [47, 120, 264, 156, 173, 162, 129, 81, 69, 63]
		# TODO: Rework this, it's pretty terrible
		m = {0:1, 1:1, 2:1, 3:1, 4:1, 5:2, 6:2, 7:3, 8:3, 9:4, 10:5, 11:6, 12:7, 13:8, 14:9, 15:10}
		if b in m:
			return m[b]
		return 10
			
	def index(self, index_path):
		""" """
		with open(index_path, 'r') as index_file:
			clip_index = json.load(index_file)

		tokeniser = Tokeniser(self.locale)
		tagger = Tagger(self.locale)

		skipped = 0
		total = 0

		buckets = {i: [] for i in range(1, 11)}
		seen = {}
		bar = progressbar.ProgressBar(max_value=MAX_PER_BUCKET*10).start()
		for sent_cid in clip_index:
			sent_res = json.loads(self._client.cat(sent_cid))
			if sent_res["content"] in TRANSCRIPT_BLACKLIST:
				skipped += 1
				continue

			num_chars = len(re.sub(r"[^\w ]+", "", sent_res["content"]))

			if num_chars > MAX_TEXT_LENGTH:
				skipped += 1
				continue

			tokens = tokeniser.tokenise(sent_res["content"])
			tags = tagger.tag(tokens)
			meta = {
				'sentence_cid': sent_cid,
				'tokens': tokens,
				'tags': tags
			}
			meta_cid = self._client.add_json(meta)

			for clip_cid in clip_index[sent_cid]:
				clip_fd = io.BytesIO(self._client.cat(clip_cid))
				audio = MP3(clip_fd)
				chars_sec = num_chars / audio.info.length
				bucket = self.rebucket(int((num_chars // audio.info.length)))
				
				if len(buckets[bucket]) >= MAX_PER_BUCKET:
					continue

#				print(bucket, audio.info.length, chars_sec, sent_res)

				entry = {
					'length': audio.info.length,
					'chars_sec': chars_sec,
					'sentence_cid': sent_cid,
					'meta_cid': meta_cid,
					'clip_cid': clip_cid,
				}
				buckets[bucket].append(entry)
				total += 1
				bar.update(total)

		print()
		index_list = []
		for bucket in buckets:
			index_list += buckets[bucket]
			n_clips = len(buckets[bucket])
			print(
				" bucket " + str(bucket).zfill(2) + " -> " + str(n_clips).rjust(5),
				"." * (n_clips // 10),
				file=sys.stderr,
			)

		opts = {'only_hash': False}
		index_hash = self._client.add_json(index_list, opts=opts)

		return index_hash
			
	def close(self):
		"""Close the TCP connection to IPFS"""
		self._client.close()

if __name__ == "__main__":
	ind = Indexer(sys.argv[1])
	index_path = sys.argv[2]
	#output_path = sys.argv[2]
	index = ind.index(index_path)
	print("Index: {index}".format(index = index))
	ind.close()
