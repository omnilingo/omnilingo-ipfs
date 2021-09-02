#!/usr/bin/env python3
"""Add a new language to OmniLingo IPNS"""
import csv
import hashlib
import ipfshttpclient
import json
import progressbar
import re
import sys

import languages 
import orthography

class Publisher:
	
	def __init__(self, locale, display, cid, nid):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node')

		self.languages = {}
		self.display = display

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

		self.locale = locale
		self.cid = cid
		

	def publish(self):
		opts = {}

		meta_info = {
			'alternatives': orthography.alternatives(self.locale),
			'display': self.display
		}
		meta_hash = self._client.add_json(meta_info, opts=opts)

		self.languages[self.locale] = {
			'meta': meta_hash, 
			'cids': [self.cid]
		}

		index_hash = self._client.add_json(self.languages, opts=opts)
		
		print(self.languages)
		print('[locale]', self.locale)
		print('[display]', self.display)
		print('[index]', index_hash)
		print('[meta]', meta_hash)

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

	locale = sys.argv[1]
	cid = sys.argv[2]
	nid = ''
	if len(sys.argv) == 4:
		nid = sys.argv[3]	

	display = locale
	if locale in languages.names:
		display = languages.names[locale]
	else:
		print('WARNING:', locale, 'not found in languages.py, display name will be "' + locale + '".')

	pub = Publisher(locale, display, cid, nid)
	
	pub.publish()
