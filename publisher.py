#!/usr/bin/env python3
"""Add a new language to OmniLingo IPNS"""
import csv
import hashlib
import ipfshttpclient
import json
import progressbar
import re
import sys
import glob
import argparse

import languages 
import orthography

class Publisher:
	
	def __init__(self, locale, display, models, cid, merge=None):
		"""Set up a connection to the local IPFS node"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node', file=sys.stderr)

		self.languages = {}
		self.display = display
		self.models = models

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
		model_hash = self._client.add(self.models[0][0], opts=opts)
		self.models[0][1]["model"] = model_hash["Hash"]
		print(self.models[0][1])
		model_meta_hash = self._client.add_json(self.models[0][1], opts=opts)

		meta_info = {
			'alternatives': orthography.alternatives(self.locale),
			'display': self.display,
			'models': [model_meta_hash]
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
		print('             [--with-model model.tflite] locale cid', file=sys.stderr)
		sys.exit(-1)

	merge = None
	locale = ''
	cid = ''
	models = []

	parser = argparse.ArgumentParser()
	parser.add_argument('-g', '--merge', dest='merge', action='store')
	parser.add_argument('-m', '--with-model', dest='model', action='store')
	parser.add_argument('locale')
	parser.add_argument('cid')
	args = parser.parse_args()

	if args.model:
		model_fn = args.model
		model_meta_fn = model_fn.replace('.tflite', '.json')
		model_meta = json.loads(open(model_meta_fn).read())	
		models.append((model_fn, model_meta))

	display = args.locale
	if args.locale in languages.names:
		display = languages.names[args.locale]
	else:
		print('WARNING:', args.locale, 'not found in languages.py, display name will be "' + ags.locale + '".', file=sys.stderr)

	pub = Publisher(args.locale, display, models, args.cid, merge=args.merge)
	
	new_hash = pub.publish()

	print('index:', new_hash)


