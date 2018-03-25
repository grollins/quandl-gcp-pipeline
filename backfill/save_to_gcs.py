from google.cloud import storage

GCS_CLIENT = storage.Client()
GCS_BUCKET = GCS_CLIENT.get_bucket('senpai-io.appspot.com')

path = 'quandl-stage/backfill_data_jan2015_mar2018.csv'
blob = GCS_BUCKET.blob(path)
blob.upload_from_filename(filename='data_jan2015_mar2018.csv')
