from google.cloud import bigquery

class Loader:
    def __init__(self, df, dataset, table):
        self.df = df 
        self.dataset = dataset
        self.table = table
    
    def loadData(self):
        client = bigquery.Client()
        dataset_ref = client.dataset(self.dataset)
        table_ref = dataset_ref.table(self.table)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(
            self.df, table_ref, job_config=job_config
        )

        job.result()
        return None