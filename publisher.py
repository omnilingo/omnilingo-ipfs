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
	
	def __init__(self, locale, display, cid, merge=None):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node', file=sys.stderr)

		self.languages = {}
		self.display = display

		if merge:
			try:
				try:
					k5 = next(k for k in self._client.key.list()['Keys'] if k['Name'] == merge)
					print("Resolved %s to %s" % (k5['Name'], k5['Id']), file=sys.stderr)
					merge = k5['Id']
				except StopIteration:
					pass
				if merge.startswith("k5"):
					merge = self._client.name.resolve(merge)['Path']
				x = self._client.cat(merge)
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

	def usage():
		print('Incorrect number of arguments', file=sys.stderr)
		print('',file=sys.stderr)
		print('publisher.py [--merge cid] locale cid', file=sys.stderr)
		sys.exit(-1)

	locale = ''
	cid = ''
	merge = None
	if sys.argv[1] == '-r':
		print('Warning: -r is deprecated, and is now the default.', file=sys.stderr)
		sys.argv.pop(1)

	if sys.argv[1] == '--merge':
		if len(sys.argv) != 5:
			usage()
		merge = sys.argv[2]
		locale = sys.argv[3]
		cid = sys.argv[4]
	else:
		if len(sys.argv) != 3:
			usage()
		locale = sys.argv[1]
		cid = sys.argv[2]

	display = locale
	if locale in languages.names:
		display = languages.names[locale]
	else:
		print('WARNING:', locale, 'not found in languages.py, display name will be "' + locale + '".', file=sys.stderr)

	pub = Publisher(locale, display, cid, merge=merge)
	
	new_hash = pub.publish()

	print('index:', new_hash)


