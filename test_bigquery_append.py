from google.cloud import bigquery

client = bigquery.Client(project='senpai-io')

dataset_ref = client.dataset('stocks')
table_ref = dataset_ref.table('test1')

previous_rows = client.get_table(table_ref).num_rows
print(previous_rows)

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
job_config.skip_leading_rows = 1
job_config.source_format = bigquery.job.SourceFormat.CSV

load_job = client.load_table_from_uri(
    'gs://senpai-io.appspot.com/quandl-stage/data/2018-04-02.csv',
    table_ref,
    job_config=job_config)  # API request

assert load_job.job_type == 'load'

load_job.result()  # Waits for table load to complete.

assert load_job.state == 'DONE'
current_rows = client.get_table(table_ref).num_rows
print(current_rows)
print(current_rows - previous_rows)
