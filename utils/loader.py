from google.cloud import bigquery

class Loader:

    def loadData(self,  df ,dataset ,table):
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )

        job.result()
        return None