from os import name
import shutil
from preprocessor.utils.preprocessor_utils import (
    PreProcessorLogger,
    PreProcessorException,
)
import os
import tempfile
import shutil
import boto3
from typing import Dict

logger = PreProcessorLogger(name="fileUtils")


class TempDir(object):
    def __init__(self, chdr=False, remove_on_exit=True):
        self._dir = None
        self._path = None
        self._chdr = chdr
        self._remove = remove_on_exit

    def __enter__(self):
        self._path = os.path.abspath(tempfile.mkdtemp())
        assert os.path.exists(self._path)
        if self._chdr:
            self._dir = os.path.abspath(os.getcwd())
            os.chdir(self._path)
        return self

    def __exit__(self, tp, val, traceback):
        if self._chdr and self._dir:
            os.chdir(self._dir)
            self._dir = None
        if self._remove and os.path.exists(self._path):
            shutil.rmtree(self._path)

        assert not self._remove or not os.path.exists(self._path)
        assert os.path.exists(os.getcwd())

    def path(self, *path):
        return (
            os.path.join("./", *path) if self._chdr else os.path.join(self._path, *path)
        )


def extractCompressed(file: str, extractLocation: str) -> None:
    try:
        shutil.unpack_archive(file, extract_dir=extractLocation)
    except Exception as e:
        logger.error("Failed to uncompress the file, error : {0}".format(e))
        raise PreProcessorException(
            "Failed to uncompress the file, error : {0}".format(e)
        )


def download_files(session, source: str, key: str, path: str):
    client = session.resource("s3", endpoint_url="http://localhost:4566")
    bucket = client.Bucket(source)
    try:
        bucket.download_file(key, path)
    except Exception as e:
        logger.error("Downloading files from s3 failed,{0}".format(e))
        raise PreProcessorException(e)


def copy_files(session, source: str, destination: str, key: str):
    client = session.resource("s3", endpoint_url="http://localhost:4566")
    bucket = client.Bucket(destination)
    copy_source = {"Bucket": source, "Key": key}
    try:
        bucket.copy(copy_source, key)
    except Exception as e:
        logger.error("Copy between buckets failed,{0}".format(e))
        raise PreProcessorException(e)


def upload_files(session, destination: str, path: str, folderkey: str):
    client = session.resource("s3", endpoint_url="http://localhost:4566")
    bucket = client.Bucket(destination)
    try:
        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, "rb") as data:
                    key = folderkey + "/" + full_path[len(path) + 1 :]
                    logger.info("Uploading file {0}".format(key))
                    bucket.put_object(Key=key, Body=data)
    except Exception as e:
        logger.error("Uploading files to s3 failed,{0}".format(e))
        raise PreProcessorException(e)


def processCompressed(session, source: str, key: str, destination: str):
    try:
        with TempDir() as tmp:
            local_path = tmp.path("compresspath")
            downloadpath = os.path.join(local_path, "downloads")
            os.makedirs(downloadpath)
            downloadfile = os.path.join(downloadpath, "data.zip")
            logger.info("Downloading the file to temp location")
            download_files(session, source, key, downloadfile)
            uncompressedpath = os.path.join(local_path, "uncompressed")
            os.makedirs(uncompressedpath)
            logger.info("Uncompressing the file")
            extractCompressed(downloadfile, uncompressedpath)
            logger.info("Uploading files to s3..")
            upload_files(
                session, destination, uncompressedpath, "/".join(key.split("/")[:-1])
            )
    except Exception as e:
        logger.error("Failed while processing the compressed files,{0}".format(e))
        raise PreProcessorException(e)


def processPassThrough(session, source: str, key: str, destination: str):
    try:
        copy_files(session, source, destination, key)
    except Exception as e:
        logger.error("Failed while processing the pass through files,{0}".format(e))
        raise PreProcessorException(e)


def processFile(source: str, key: str, destination: str, config: Dict):
    session = boto3.Session(
        aws_access_key_id=config.get("s3").get("key"),
        aws_secret_access_key=config.get("s3").get("access"),
    )
    filename = key.split("/")[-1]
    extension = "".join(filename.split(".")[1:])
    logger.info("Checking file extension")
    if extension in ["zip", "tar", "targz", "gztar", "gzip"]:
        logger.info("Compressed file")
        processCompressed(session, source, key, destination)
    elif extension in ["7z"]:
        logger.info("7z file")
        pass
    else:
        logger.info("pass through file")
        processPassThrough(session, source, key, destination)
