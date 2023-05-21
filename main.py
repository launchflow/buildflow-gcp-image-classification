# flake8: noqa
import dataclasses
import os
import pandas as pd
import sys
import tempfile
from typing import List

import buildflow
from buildflow import Flow
from imageai.Classification import ImageClassification

# TODO(developer): fill these in.
GCP_PROJECT = 'TODO'
BUCKET_NAME = 'TODO'


@dataclasses.dataclass
class Classification:
    classification: str
    confidence: float


@dataclasses.dataclass
class ImageClassificationRow:
    image_name: str
    upload: str
    classifications: List[Classification]


flow = Flow()


class ImageClassificationProcessor(buildflow.Processor):

    def source(self):
        return buildflow.GCSFileNotifications(project_id=GCP_PROJECT,
                                              bucket_name=BUCKET_NAME)

    def sink(self):
        return buildflow.BigQuerySink(
            table_id=
            f"{args.gcp_project}.launchflow_walkthrough.image_classification")

    def setup(self):
        self.execution_path = os.path.dirname(os.path.realpath(__file__))
        self.prediction = ImageClassification()
        self.prediction.setModelTypeAsResNet50()
        self.prediction.setModelPath(
            os.path.join(self.execution_path, "resnet50-19c8e357.pth"))
        self.prediction.loadModel()

    def process(self,
                gcs_file_event: buildflow.GCSFileEvent) -> ImageClassificationRow:
        with tempfile.TemporaryDirectory() as td:
            file_path = os.path.join(td, gcs_file_event.metadata['objectId'])
            with open(file_path,'wb') as f:
                f.write(gcs_file_event.blob)
            predictions, probabilities = self.prediction.classifyImage(
                file_path, result_count=5)
        classifications = []
        for predicition, probability in zip(predictions, probabilities):
            classifications.append(Classification(predicition, probability))
        return ImageClassificationRow(
            image_name=gcs_file_event.metadata['objectId'],
            upload=pd.Timestamp(gcs_file_event.metadata['eventTime']),
            classifications=classifications,
        )
            


# Run your flow.
flow.run(ImageClassificationProcessor()).output()


