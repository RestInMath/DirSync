import os
from os.path import exists, isfile, isdir
from shutil import copy
import logging
import hashlib
import time
import sys

LOG_FILE_NAME = 'dirsync.log'


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class DirectorySyncer:
    def __init__(self, source_directory, replica_directory, interval, log_dir):
        #check arguments
        self.__check_dir(source_directory)
        self.__check_dir(replica_directory)
        self.__check_dir(log_dir)
        self.__check_int(interval)
        
        #set up fields
        self.source = source_directory
        self.replica = replica_directory
        self.interval = interval
        self.__init_fields()

        #logging
        log_file = log_dir + '\\' + LOG_FILE_NAME
        self.logger = logging.getLogger('dirsync')
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=log_file, level=logging.INFO)

    def sync(self):
        interval = self.interval
        syncdef = self.__sync

        while True:
            syncdef()
            time.sleep(interval)

    def __sync(self):
        self.__compare_files()
        self.__delete_files()
        self.__delete_foldes()
        self.__create_folders()
        self.__copy_files()

        self.__init_fields()
        self.__log('synchronized')

    def __get_all_directory_files(self, directory):
        directory_files = set()
        directory_folders = set()

        for path, dirs, files in os.walk(directory):

            #skip first-level folder (source or replica) 
            beg = len(directory)
            if beg != -1:
                common_path = path[beg:] + '\\'
            else:
                common_path = ''

            directory_folders.add(common_path)
            [directory_files.add(common_path+file) for file in files]

        return directory_files, directory_folders

    def __compare_files(self):
        files_source, folders_source = self.__get_all_directory_files(self.source)
        files_replica, folders_replica = self.__get_all_directory_files(self.replica)

        files_to_sync = files_source - files_replica
        files_to_delete = files_replica - files_source
        files_intersection = set(files_source).intersection(files_replica)

        for file in files_intersection:
            checksums = md5(self.source+'\\'+file) == md5(self.replica+'\\'+file)
            if not checksums:
                files_to_delete.add(file)
                files_to_sync.add(file)

        self.folders_to_create = folders_source - folders_replica
        self.folders_to_delete = folders_replica - folders_source

        self.to_sync, self.to_delete = files_to_sync, files_to_delete

    def __copy_files(self):
        for file in self.to_sync:
            directory = self.replica+'\\'+os.path.dirname(file)
            if not exists(directory):
                os.makedirs(directory)

            src = self.source + '\\' + file          
            copy(src, self.replica+'\\'+file)
            self.__log("file copied " + file)

    def __delete_files(self):
        for file in self.to_delete:
            os.remove(self.replica+'\\'+file)
            self.__log("file removed " + file)

    def __create_folders(self):
        root = self.replica
        for directory in self.folders_to_create:
            if not exists(root+'\\'+directory):
                os.makedirs(root+'\\'+directory)
                self.__log("directory created " + directory)

    def __delete_foldes(self):
        root = self.replica
        for directory in sorted(list(self.folders_to_delete), reverse=True):
            os.rmdir(root+'\\'+directory)
            self.__log("directory removed " + directory)

    def __check_file(self, fname):
        if not isfile(fname):
            raise ValueError("No such file " + str(fname))

    def __check_dir(self, dirname):
        if not isdir(dirname):
            raise ValueError("No such directory " + str(dirname))

    def __check_int(self, number):
        if not isinstance(number, int) or number < 0:
            raise ValueError("Wrong time interval " + str(number))

    def __log(self, message):
        self.logger.info(message)

    def __init_fields(self):
        self.to_sync = None
        self.to_delete = None
        self.folders_to_create = None
        self.folders_to_delete = None


def parse_args(args):
    parsed_args = None
    if len(sys.argv) == 5:
        parsed_args = []
        parsed_args.append(args[1])
        parsed_args.append(args[2])

        try:
            if args[3][-1] == 's':
                interval = int(args[3][:-1])
            elif args[3][-1] == 'm':
                interval = int(args[3][:-1])*60
            parsed_args.append(interval)

        except:
            raise ValueError("Wrong time interval")

        parsed_args.append(args[4])

    return parsed_args

if __name__ == '__main__':
    args = parse_args(sys.argv)
    if args:
        syncer = DirectorySyncer(*args)
        syncer.sync()
    else:
        raise Exception("Required arguments: source directory, " +\
            "replica directory, time interval, logging file")
            