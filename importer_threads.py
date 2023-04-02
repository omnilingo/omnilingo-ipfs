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
import os

from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3

# MULTIPROCESSING
from io import TextIOWrapper
from typing import (Any, Iterable)
from datetime import datetime, timedelta
import threading
from concurrent.futures import Future, ThreadPoolExecutor
import psutil

# How many threads will we use?
# PROC_COUNT: int = psutil.cpu_count(logical=False) - 1			# Lower usage
PROC_COUNT: int = psutil.cpu_count(logical=True)        # High performance? (to be tested)

# To not hog the RAM, we will limit the data loaded
MAX_CHUNK_SIZE: int = 100


# Multiple Progress Bar Wrapper
output_lock = threading.Lock()

class BarStreamWrapper:
	UP = '\033[F'
	DOWN = '\033[B'

	def __init__(self, lines=0, stream=sys.stderr):
		self.stream = stream
		self.lines = lines

	def write(self, data):
		with output_lock:
			self.stream.write(self.UP * self.lines)
			self.stream.write(data)
			self.stream.write(self.DOWN * self.lines)
			self.stream.flush()

	def __getattr__(self, name):
		return getattr(self.stream, name)

class Importer:
	_input_path: str = ""
	_clips_path: str = ""
	_output_path: str = ""
	_opts: object = {}
	# _bars: Iterable[progressbar.ProgressBar] = []
	_bars: list = []

	def __init__(self):
		"""Set up a connection to the local IPFS node - keeping this for initial connection check and warning"""
		try:
			self._client = ipfshttpclient.connect(session=True)
		except:
			print('Could not connect to IPFS node', file=sys.stderr)
			sys.exit(-1)

	def chunk_reader(self, dict_reader: csv.DictReader, chunk_size: int = 100) -> Iterable[dict]:
		"""
		Generator function to read from a csv.DictReader in chunks of lines (records)
		:param dict_reader: csv.DictReader object
		:param chunk_size: Number of records to yield (default=1000)
		:rtype: Iterable[dict]
		"""
		chunk: list[dict] = []
		for i, dict_object in enumerate(dict_reader):
			if (i % (chunk_size-1) == 0 and i > 0):
					chunk.append(dict_object)
					yield chunk
					chunk = []
			chunk.append(dict_object)
		# yield remaining (last portion of file)
		yield chunk

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

	def path_join(self, *args, sep=os.sep):
		"""
		Join a sequence of arguments on a given delimiter
		*args: any number of strings
		sep: directory separator
		"""
		return sep.join(args)

	def hashify_process(self, lst: list):
		# Each process has its own resource pools
		client = ipfshttpclient.connect(session=True)
		# Thread name & number
		th_name = threading.current_thread().getName()
		th_no = int(th_name.split('_')[-1]) + 1 # inx 0 is global one
		# Progress Bar of process
		self._bars[th_no].init()
		self._bars[th_no].max_value = len(lst)
		# bar = progressbar.ProgressBar(max_value=df.shape[0]).start()
		# print(df.shape)
		# print(df.head())

		# accumulate results here
		results = []
		# Iterate through all records
		for i, row in enumerate(lst):
			# print(row)
			sentence = {
				'content': row["sentence"],
				'language': row["locale"],
				'copyright': "CC0-1.0"
			}
			sent_hash = client.add_json(sentence, opts=self._opts)
			clip_path = self.path_join(self._clips_path, row["path"])
			audio = EasyID3(clip_path)
			audio["copyright"] = "CC0-1.0"
			audio["language"] = row["locale"]
			audio["album"] = sent_hash
			audio["author"] = row["client_id"]
			audio.save()
			clip_res = client.add(clip_path, opts=self._opts)
			self._bars[th_no].update(i)
			results.append([sent_hash, clip_res])   # return list (length=input) of list (length=2)
		
		client.close()
		return results

	def hashify(self, input_path, output_path, dryrun=False):
		"""
		Import a Common Voice dump into IPFS
		input_path: path to a Common Voice dump directory
		output_path: place to put the generated index in JSON
		"""
		start_time: datetime = datetime.now()

		# Handle paths
		validated_path = self.path_join(input_path, 'validated.tsv')
		self._clips_path = self.path_join(input_path, 'clips')
		# Check input path
		if not os.path.isfile(validated_path):
			print(f'FATAL: validated.tsv is not found on {input_path}!')
			sys.exit(-1)
		# Create destination directory if not exists
		dest_dir = os.path.dirname(output_path)
		if (not os.path.isdir(dest_dir)):
			print(f'WARNING: Creating non-existing destination directory "{dest_dir}"...')
			os.makedirs(dest_dir, exist_ok=True)

		# Size calculations
		file_length = self.line_count(validated_path) - 1
		chunk_size = min(MAX_CHUNK_SIZE, int(file_length / PROC_COUNT) + 1)
		num_chunks = int(file_length/chunk_size) + 1 if (file_length % chunk_size > 0) else 0

		print(f'=== Importer processing {file_length} recs on {PROC_COUNT} threads.', input_path, '→', output_path, file=sys.stderr)
		print(f'=== Chunk size: {chunk_size} records/process - Total chunks: {num_chunks}')

		# Handle Multi ProgressBars, first one is chunks, others show threds
		update_interval: int = 2 if chunk_size < 100 else 5
		samples_minutes: int = 1 if num_chunks < 10 else 2 if num_chunks < 100 else 5
		self._bars.append(progressbar.ProgressBar(
			fd=BarStreamWrapper(0),
			prefix='Chunks:   ',
			max_value=num_chunks,
			poll_interval=update_interval,
			min_poll_interval=update_interval,
			widget_kwargs={'samples': timedelta(minutes=samples_minutes)}
		))
		for wcnt in range(PROC_COUNT):
			self._bars.append(progressbar.ProgressBar(
				fd=BarStreamWrapper(wcnt+1),
				prefix=f'Worker-{wcnt}: ',
				max_value=chunk_size,
				poll_interval=update_interval,
				min_poll_interval=update_interval
			))
			print('...')
		print()
		for bar in self._bars:
			bar.start()

		#
		# Actual processing through threads
		#
		chunk: Iterable[dict] = []

		with open(validated_path, newline='') as validated_file:
			if dryrun: 
				self._opts={'only_hash': True}

			reader = csv.DictReader(validated_file, delimiter='\t')
			future_list: list[Future] = []
			cnt_chunks: int = 0

			with ThreadPoolExecutor(max_workers=PROC_COUNT, thread_name_prefix="Worker") as e:
				while (cnt_chunks < num_chunks):
					# handle finished
					cnt_running: int = 0
					for future in future_list:
						if future.running():
							cnt_running += 1
					self._bars[0].update(cnt_chunks - cnt_running)
					if cnt_running < PROC_COUNT:
						# generate new chunks
						chunk = next(self.chunk_reader(dict_reader=reader, chunk_size=chunk_size))
						future_list.append(e.submit(self.hashify_process, chunk))
						cnt_chunks += 1
						# print("CHUNK:", cnt_chunks, "THREADS:", threading.active_count())

		for bar in self._bars:
			bar.finish()

		# combine
		clip_index: dict = {}
		cnt_results: int = 0
		for future in future_list:
			results = future.result()
			cnt_results += len(results)
			for item in results:
				if item[0] not in clip_index:
					clip_index[item[0]] = []
				clip_index[item[0]].append(item[1]['Hash'])

		# Save the transcript → clip hash as a json file
		with open(output_path, 'w') as output_file:
			json.dump(clip_index, output_file)

		total_seconds = (datetime.now() - start_time).total_seconds()
		print(f'\n=== Returned items: {cnt_results} - Required: {file_length}')
		print(f'=== PROCESSED {file_length} records in {total_seconds} sec.')
		print(f'=== SPEED ~{int(1000*total_seconds/file_length)} sec/1000 recs / ~{int(file_length/total_seconds)} recs/sec.')

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
