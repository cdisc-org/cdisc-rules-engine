from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)

from engine.services import logger
from engine.services.dataset_metadata_reader import DatasetMetadataReader
from engine.utilities.utils import extract_file_name_from_path_string
from engine.exceptions.custom_exceptions import DatasetNotFoundError


class BlobStorageService:
    def __init__(self, blob_storage_connection_string, container):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            blob_storage_connection_string
        )
        self.blob_container = ContainerClient.from_connection_string(
            conn_str=blob_storage_connection_string, container_name=container
        )
        self.container = container

    def get_file_info(self, blob_name: str):
        return next(
            (
                blob
                for blob in self.blob_container.list_blobs()
                if blob.name == blob_name
            ),
            None,
        )

    def get_all_file_names(self, prefix: str):
        return [blob.name for blob in self.blob_container.list_blobs(name_starts_with=prefix)]

    def read_data(self, blob_name) -> bytes:
        blob: StorageStreamDownloader = self._download_blob(blob_name)
        return blob.readall()

    def read_metadata(self, blob_name: str) -> dict:
        """
        Gets blob metadata and returns it as dict.
        Return dict example:
        {
            "file_metadata": {
                "path": "CDISC01/test/ae.xpt",
                "name": "ae.xpt",
                "size": 38000, (in bytes)
            },
            "contents_metadata": {
                "variable_labels": ["Study Identifier", ...],
                "variable_names": ["STUDYID", "SUBJECTID", ...],
                "variable_name_to_label_map": {"STUDYID": "Study Identifier", ...},
                "variable_name_to_data_type_map": {"STUDYID": "string", "AESEQ": "double", ...},
                "variable_name_to_size_map": {"STUDYID": 12, "AESEQ": 10, ...},
                "number_of_variables": 30,
                "dataset_label": "Adverse Events",
                "dataset_name": "AE",
                "dataset_modification_date": "2020-10-10T15:20:20",
            },
        }
        """
        blob: StorageStreamDownloader = self._download_blob(blob_name)
        file_metadata: dict = blob.properties
        # file metadata contains path like "CDISC01/test/ae.xpt" in "name" key
        file_path: str = file_metadata["name"]
        file_metadata["path"] = file_path
        file_metadata["name"] = extract_file_name_from_path_string(file_path)
        contents_metadata: dict = DatasetMetadataReader(blob.readall()).read()
        return {
            "file_metadata": file_metadata,
            "contents_metadata": contents_metadata,
        }

    def _download_blob(self, blob_name: str) -> StorageStreamDownloader:
        """
        Downloads blob from storage
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container, blob=blob_name
        )
        try:
            return blob_client.download_blob()
        except ResourceNotFoundError:
            logger.error(
                f"Dataset {blob_name} is not found in container {self.container}"
            )
            # raising our internal exception
            raise DatasetNotFoundError("dataset does not exist")
