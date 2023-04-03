#!/usr/bin/env python3
"""Import a Common Voice dump into IPFS generating an index of CIDs using multiprocessing."""
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
from typing import Iterable, TypedDict
from datetime import datetime, timedelta
from concurrent.futures import Future, ProcessPoolExecutor
import psutil

class CommonVoiceRec(TypedDict):
  client_id: str
  path: str
  sentence: str
  up_votes: int
  down_votes:	int
  age: str
  gender: str
  accents: str
  variant: str
  locale: str
  segment: str


class Importer:
  """Imports CommonVoice(like) formatted validated.tsv files, processes them with audio files, sets ID3 tags into audio files, outputs a file containing IPFS CID's for sentences and recordings."""
  __clips_path: str = ''
  __opts: object = {}

  def __init__(self):
    """Set up a connection to the local IPFS node - keeping this for initial connection check and warning"""
    try:
      self._client = ipfshttpclient.connect(session=True)
    except:
      print('Could not connect to IPFS node', file=sys.stderr)
      sys.exit(-1)

  def scheduler(self, rec_cnt: int):
    """
    Decide on number of processes and chunk size to be used

    Arguments:
      rec_cnt: int - number of lines in the dataset

    Returns: num_procs, chunk_size, num_chunks
      num_procs: int - number of processes to be used (size of pool)
      chunk_size: int - chunk size to be used
      num_chunks: int - number of total chunks to run
    """
    # Rudumentary decision logic:
    # If dataset is small (<1000 recs) then do it in one proc/chunk
    # On larger datasets, decide process count & chunk_size so that:
    # - chunk size = 1000
    # - num_procs >= 2 AND num_procs <= all v-cores
    # MIN_CHUNK: int = 500		# lower numbers will cause data transfer costs
    MAX_CHUNK: int = 1000		# larger numbers will cause higher memory usage
    # MAX_PROCS: int = psutil.cpu_count(logical=False) - 1
    MAX_PROCS: int = psutil.cpu_count(logical=True)
    if rec_cnt < MAX_CHUNK:
      # small dataset, run it on a single proc in one chunk
      num_procs = 1
      chunk_size = rec_cnt
    elif (rec_cnt / MAX_CHUNK) < MAX_PROCS:
      # medium sized dataset for this machine, maybe use less v-cores for energy efficiency?
      # e.g. for a 2500 rec dataset on 8 core machine we will use 3 v-cores & 1000 recs/chunk & 3 chunks
      chunk_size = MAX_CHUNK
      num_procs = min(MAX_PROCS, int(rec_cnt / chunk_size) + (1 if (rec_cnt % chunk_size > 0) else 0))
    else: # large dataset, use all v-cores with largest possible chunk_size
      num_procs = MAX_PROCS
      chunk_size = min(MAX_CHUNK, int(rec_cnt / num_procs) + (1 if (rec_cnt % num_procs > 0) else 0))

    num_chunks = int(rec_cnt/chunk_size) + (1 if (rec_cnt % chunk_size > 0) else 0)
    return num_procs, chunk_size, num_chunks

  def chunk_reader(self, dict_reader: csv.DictReader, chunk_size: int = 1000) -> Iterable[CommonVoiceRec]:
    """
    Generator function to read from a csv.DictReader in chunks of lines (records)

    Arguments:
      dict_reader: csv.DictReader object
      chunk_size: Number of records to yield (default=1000)

    Returns:
      list[CommonVoiceRec]
    """
    chunk: list[CommonVoiceRec] = list()
    for i, dict_obj in enumerate(dict_reader):
      rec: CommonVoiceRec = dict_obj
      chunk.append(rec)
      if len(chunk) == chunk_size:
        yield chunk
        chunk = []
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

    # accumulate results here
    results = []
    # Iterate through all records
    for row in lst:
      # print(row) # DEBUG
      sentence = {
        'content': row['sentence'],
        'language': row['locale'],
        'copyright': 'CC0-1.0'
      }
      sent_hash = client.add_json(sentence, opts=self.__opts)
      clip_path = self.path_join(self.__clips_path, row['path'])
      audio = EasyID3(clip_path)
      audio['copyright'] = 'CC0-1.0'
      audio['language'] = row['locale']
      audio['album'] = sent_hash
      audio['author'] = row['client_id']
      audio.save()
      clip_res = client.add(clip_path, opts=self.__opts)
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
    self.__clips_path = self.path_join(input_path, 'clips')
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
    rec_cnt = self.line_count(validated_path) - 1
    num_procs, chunk_size, num_chunks = self.scheduler(rec_cnt)

    print(f'=== Importer processing {rec_cnt} recs.', input_path, '→', output_path, file=sys.stderr)
    print(f'=== Processes: {num_procs} - Chunk size: {chunk_size} recs/proc - Total chunks: {num_chunks}')

    # ProgressBar showing chunks (if there are many chunks, else it is not shown)
    use_bar: bool = (num_chunks > num_procs) # if it finishes in one turn it is not logical to show the progress var
    if use_bar:
      update_interval: int = 2 if chunk_size < 100 else 5
      samples_seconds: int = 10 if num_chunks < 10 else 60 if num_chunks < 100 else 180
      bar = progressbar.ProgressBar(
        prefix='Chunks: ',
        max_value=num_chunks,
        poll_interval=update_interval,
        min_poll_interval=update_interval,
        widget_kwargs={'samples': timedelta(seconds=samples_seconds)}
      )
      bar.start()

    #
    # Actual processing through processes
    #
    chunk: list[CommonVoiceRec] = []

    with open(validated_path, newline='') as validated_file:
      if dryrun: 
        self.__opts={'only_hash': True}

      reader = csv.DictReader(validated_file, delimiter='\t')
      future_list: list[Future] = []
      cnt_chunks: int = 0

      with ProcessPoolExecutor( max_workers=num_procs) as e:
        while (cnt_chunks < num_chunks):
          # handle finished (TODO - needs rework - callbacks?)
          cnt_running: int = 0
          for future in future_list:
            if future.running():
              cnt_running += 1
          if use_bar:
            bar.update(cnt_chunks - cnt_running)
          if cnt_running < num_procs:
            # generate new chunks
            chunk = next(self.chunk_reader(dict_reader=reader, chunk_size=chunk_size))
            # print("CHUNK LEN=",len(chunk),chunk[0]['path'], " - ", chunk[-1]['path']) # DEBUG
            future_list.append(e.submit(self.hashify_process, chunk))
            cnt_chunks += 1
          if cnt_chunks == num_chunks and cnt_running == 0:
            # shutdown executor after all processes finished (no running futures & all chunks started)
            e.shutdown()

    if use_bar:
      bar.finish()

    #
    # combine results
    #
    # One sentence item is composed of CID of a sentence and a list of CID's of audio recordings in this format:
    # {CID_OF_SENTENCE: [
    #    CID_OF_RECORDING_1,
    #    CID_OF_RECORDING_2,
    #    ...
    #   ]
    # }
    sentence_index: dict = {}
    cnt_results: int = 0
    # result_lengths: list[int] = [] # DEBUG
    for future in future_list:
      results = future.result()
      cnt_results += len(results)
      # result_lengths.append(len(results)) # DEBUG
      for item in results:
        # The result item is in format [CID_OF_SENTENCE, CID_OF_RECORDING]
        if item[0] not in sentence_index:                   # if the sentence is not added yet
          sentence_index[item[0]] = []                      # add it with 
        sentence_index[item[0]].append(item[1]['Hash'])     # add the recordings CID to sentence

    # Save the transcript → clip hash as a json file
    with open(output_path, 'w') as output_file:
      json.dump(sentence_index, output_file)

    total_seconds = (datetime.now() - start_time).total_seconds()
    print(f'\n=== Returned items: {cnt_results} - Required: {rec_cnt}', "" if cnt_results == rec_cnt else " (Reason: Unclosed quotes in dataset)")
    print(f'=== PROCESSED {rec_cnt} records in {timedelta(seconds=total_seconds)}')
    print(f'=== SPEED ~{int(1000*total_seconds/rec_cnt)} sec/1000 recs / ~{int(rec_cnt/total_seconds)} recs/sec.')
    # print(result_lengths) # DEBUG

  def close(self):
    """Close the TCP connection to IPFS"""
    self._client.close()

if __name__ == '__main__':
  imp = Importer()
  if len(sys.argv) != 3:
    print('importer.py dataset_dir index_path')
    sys.exit(-1)
  dataset_dir = sys.argv[1]
  index_path = sys.argv[2]
  imp.hashify(dataset_dir, index_path, dryrun=False)
  imp.close()
