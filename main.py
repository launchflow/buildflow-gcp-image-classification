# flake8: noqa
import argparse
import dataclasses
import os
import pandas as pd
import sys
import tempfile
from typing import List

import buildflow
from buildflow import Flow
from imageai.Classification import ImageClassification

parser = argparse.ArgumentParser()
parser.add_argument("--gcp_project", type=str, required=True)
parser.add_argument("--bucket_name", type=str, required=True)
parser.add_argument("--table_name", type=str, default="image_classification")
args, _ = parser.parse_known_args(sys.argv)

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
        return buildflow.GCSFileNotifications(project_id=args.gcp_project,
                                              bucket_name=args.bucket_name)

    def sink(self):
        return buildflow.BigQuerySink(
            table_id=
            f"{args.gcp_project}.launchflow_walkthrough.{args.table_name}")

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


