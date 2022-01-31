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
	
	def __init__(self, locale, display, cid, reinitialise=False):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node', file=sys.stderr)

		self.languages = {}
		self.display = display

		self.key = self._client.name.resolve()
		if not reinitialise:
			try:
				x = self._client.cat(self.key['Path'])
				# Populate language list from existing
				print('Found existing list', file=sys.stderr)
				self.languages = json.loads(x)
			except:
				print('No existing list')
				pass

#		self.languages = {}
		print('[languages]', self.languages.keys(), file=sys.stderr)

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
		
		print('[' + self.locale + ']',  self.display, '|', meta_hash, file=sys.stderr)

		self._client.name.publish(index_hash, allow_offline=True)

		return index_hash 

	def close(self):
		"""Close the TCP connection to IPFS"""
		self._client.close()


if __name__ == "__main__":

	# Takes either:
	## Single CID of index
	### -> Generates a new key and adds the languages

	if len(sys.argv) < 3 or len(sys.argv) > 4:
		print('Incorrect number of arguments', file=sys.stderr)
		print('',file=sys.stderr)
		print('publisher.py [-r] locale cid', file=sys.stderr)
		sys.exit(-1)

	locale = ''
	cid = ''
	reinit = False
	if sys.argv[1] == '-r':
		reinit = True
		locale = sys.argv[2]
		cid = sys.argv[3]
	else:
		locale = sys.argv[1]
		cid = sys.argv[2]

	display = locale
	if locale in languages.names:
		display = languages.names[locale]
	else:
		print('WARNING:', locale, 'not found in languages.py, display name will be "' + locale + '".', file=sys.stderr)

	pub = Publisher(locale, display, cid, reinitialise=reinit)
	
	new_hash = pub.publish()

	print('index:', new_hash)


