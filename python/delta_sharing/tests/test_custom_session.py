import requests
import pytest
import pandas as pd
from unittest import mock
from datetime import date
import os

from delta_sharing.delta_sharing import (
    DeltaSharingProfile,
    SharingClient,
    load_as_pandas,
)
from delta_sharing.rest_client import DataSharingRestClient
from delta_sharing.reader import DeltaSharingReader

def test_load_as_pandas_with_proxy(profile_path: str):
    profile = DeltaSharingProfile.read_from_file(profile_path)
    
    session = requests.Session()
    session.proxies = {
        "http": "http://proxy.example.com:8080",
        "https": "http://proxy.example.com:8080",
    }
    session.headers.update({"X-Proxy-Test": "true"})
    
    sharing_client = SharingClient(profile, session=session)

    expected_df = pd.DataFrame(
        {
            "eventTime": [pd.Timestamp("2021-04-28 06:32:22.421"), pd.Timestamp("2021-04-28 06:32:02.070")],
            "date": [date(2021, 4, 28), date(2021, 4, 28)]
        }
    )

    with mock.patch.object(DataSharingRestClient, 'list_files_in_table', autospec=True) as mock_list_files_in_table:
        mock_response = mock.Mock()
        mock_response.metadata.schema_string = '{"fields":[{"name":"eventTime","type":"timestamp"},{"name":"date","type":"date"}]}'
        mock_response.add_files = [
            mock.Mock(url="http://example.com/file1.parquet"),
            mock.Mock(url="http://example.com/file2.parquet")
        ]
        mock_list_files_in_table.return_value = mock_response

        with mock.patch('fsspec.filesystem', autospec=True) as mock_filesystem:
            mock_fs_instance = mock_filesystem.return_value
            mock_fs_instance.open.return_value.__enter__.return_value.read.return_value = b''

            with mock.patch.object(DeltaSharingReader, 'to_pandas', return_value=expected_df):
                url = f"{profile_path}#share.schema.table"

                actual_df = load_as_pandas(url, sharing_client=sharing_client)
                
                pd.testing.assert_frame_equal(actual_df, expected_df)

                rest_client_instance = sharing_client.rest_client
                session_used = rest_client_instance.get_session()
                
                assert session_used is session
                assert session_used.proxies == session.proxies
                assert session_used.headers["X-Proxy-Test"] == "true"


if __name__ == "__main__":
    pytest.main([__file__])