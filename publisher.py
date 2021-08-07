#!/usr/bin/env python3
"""Add a new language to OmniLingo IPNS"""
import csv
import hashlib
import ipfshttpclient
import json
import progressbar
import re
import sys

class Publisher:
	
	def __init__(self, lang, cid, nid):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node')

		self.languages = {}

		self.key = self._client.name.resolve()
		print(self.key)
		try:
			x = self._client.cat(self.key['Path'])
			# Populate language list from existing
			print('Found existing list')
			self.languages = json.loads(x)
		except:
			print('No existing list')
			pass

		print('[languages]', self.languages)

		self.lang = lang
		self.cid = cid
		

	def publish(self):
		opts = {}

		self.languages[self.lang] = [self.cid]

		index_hash = self._client.add_json(self.languages, opts=opts)
		
		print(self.languages)
		print(index_hash)

		self._client.name.publish(index_hash, allow_offline=True)

	def close(self):
		"""Close the TCP connection to IPFS"""
		self._client.close()


if __name__ == "__main__":

	# Takes either:
	## Single CID of index
	### -> Generates a new key and adds the language
	## CID of index + NID of existing language list
	### -> Retrieves existing 

	if len(sys.argv) < 3 or len(sys.argv) > 4:
		print('Incorrect number of arguments')
		sys.exit(-1)

	lang = sys.argv[1]
	cid = sys.argv[2]
	nid = ''
	if len(sys.argv) == 4:
		nid = sys.argv[3]	

	pub = Publisher(lang, cid, nid)
	
	pub.publish()
