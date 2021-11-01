from __future__ import absolute_import

import os
import sys
import argparse

if sys.path[0] in ("", os.getcwd()):
    sys.path.pop(0)

if __package__ == "":
    path = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, path)

from preprocessor.main import processor

parser = argparse.ArgumentParser(description="Falcon Processor")
parser.add_argument("--file_process_id", "-i", help="File Process id")
parser.add_argument("--bucket", "-b", help="S3 Bucket Name")
parser.add_argument("--key", "-k", help="S3 Object key")
parser.add_argument("--size", "-s", help="file size")
parser.add_argument("--reprocess_flag", "-f", help="reprocessing flag", default=False)
parser.add_argument(
    "--config", "-c", help="Preprocessor Config file", default="resources/config.yaml"
)

args = parser.parse_args()

if __name__ == "__main__":
    sys.exit(
        processor(
            args.file_process_id,
            args.bucket,
            args.key,
            args.size,
            args.reprocess_flag,
            args.config,
        )
    )
