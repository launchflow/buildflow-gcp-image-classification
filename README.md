# Walkthrough of using BuildFlow with GCS File Events 

Install deps:

```
pip install -r requirements.txt
```

Update vars in `main.py`:


Set the following 
```
GCP_PROJECT = "TODO"
BUCKET_NAME = "TODO"
```

Run locally:

```
buildflow run main:app
```
