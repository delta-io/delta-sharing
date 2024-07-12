from json import loads, dump

import delta_kernel_python
import os
import pandas as pd
import pyarrow as pa
import tempfile
import time
from pyarrow.dataset import dataset

class DeltaSharingReaderKernel:
    @staticmethod
    def to_pandas_kernel(delta_sharing_reader):
        delta_sharing_reader._rest_client.set_delta_format_header()
        start = time.time()
        response = delta_sharing_reader._rest_client.list_files_in_table(
            delta_sharing_reader._table,
            predicateHints=delta_sharing_reader._predicateHints,
            jsonPredicateHints=delta_sharing_reader._jsonPredicateHints,
            limitHint=delta_sharing_reader._limit,
            version=delta_sharing_reader._version,
            timestamp=delta_sharing_reader._timestamp
        )
        end = time.time()
        print("PranavSukumar")
        print("Time to get response from server is " + str(end-start) + " seconds")

        start = time.time()

        lines = response.lines

        # Create a temporary directory using the tempfile module
        temp_dir = tempfile.TemporaryDirectory()
        delta_log_dir_name = temp_dir.name
        table_path = "file:///" + delta_log_dir_name

        # Create a new directory named '_delta_log' within the temporary directory
        log_dir = os.path.join(delta_log_dir_name, '_delta_log')
        os.makedirs(log_dir)

        # Create a new .json file within the '_delta_log' directory
        json_file_name = "0".zfill(20) + ".json"
        json_file_path = os.path.join(log_dir, json_file_name)
        json_file = open(json_file_path, 'w+')

        # Write the protocol action to the log file
        protocol_json = loads(lines.pop(0))
        deltaProtocol = {"protocol": protocol_json["protocol"]["deltaProtocol"]}
        dump(deltaProtocol, json_file)
        json_file.write("\n")

        # Write the metadata action to the log file
        metadata_json = loads(lines.pop(0))
        deltaMetadata = {"metaData": metadata_json["metaData"]["deltaMetadata"]}
        dump(deltaMetadata, json_file)
        json_file.write("\n")

        # Write the add file actions to the log file
        for line in lines:
            line_json = loads(line)
            dump(line_json["file"]["deltaSingleAction"], json_file)
            json_file.write("\n")

        # Close the file
        json_file.close()

        end = time.time()

        json_file = open(json_file_path, 'r')
        print("PranavSukumar")
        print(f"Created a new file at {json_file_path}")
        print("The file read back in and printed back out is")
        print(json_file.read())

        json_file.close()


        print("PranavSukumar")
        print("Time to save log file locally is " + str(end-start) + " seconds")

        file_stats = os.stat(json_file_path)
        print("PranavSukumar")
        print("Size of log file is " + str(file_stats.st_size / (1024 * 1024)) + " MB")

        # Invoke delta-kernel-rust to return the pandas dataframe
        interface = delta_kernel_python.PythonInterface(table_path)
        table = delta_kernel_python.Table(table_path)
        snapshot = table.snapshot(interface)

        scan = delta_kernel_python.ScanBuilder(snapshot).build()
        table = pa.Table.from_batches(scan.execute(interface))

        result = table.to_pandas()

        # Delete the temp folder explicitly and remove the delta format from header
        temp_dir.cleanup()
        delta_sharing_reader._rest_client.remove_delta_format_header()

        return result