# Walkthrough of using BuildFlow with GCS File Events 

Install deps:

```
pip install -r requirements.txt
```

Needs the following env vars set:
- $GCP_PROJECT = where the bucket / pubsub topic, subscription should exist
- $BUCKET_NAME = the name of the bucket

Run locally:

```
python main.py --gcp_project=$GCP_PROJECT --bucket_name=$BUCKET_NAME
```

Run remotely:

```
launch jobs submit 'main.py --gcp_project=$GCP_PROJECT --bucket_name=$BUCKET_NAME' --requirements-file=requirements.txt --working-dir=.
```