from utils.getConfig import GetConfig

class GetDFLoc:
    def __init__(self):
        self.parser = GetConfig()
    
    def getDFLocation(self, df):

        project_id = self.parser.getConfig('gcp_bigQuery', 'project_id')
        dataset = self.parser.getConfig('gcp_bigQuery', 'dataset')
        dataframe = self.parser.getConfig('gcp_bigQuery', df)

        return f'{project_id}.{dataset}.{dataframe}'