class Config:
    @property
    def spark_master(self): return "spark-master"
    @property
    def spark_app_name(self): return "spark-app"
    @property
    def spark_port(self): return 7077
    @property
    def hdfs_url(self): return "hdfs://master:54310"
    @property
    def hdfs_dataset_dir_url(self): return f"{self.hdfs_url}/nifi"
    @property
    def hdfs_dataset_preprocessed_dir_url(self): return f"{self.hdfs_url}/dataset"
    @property
    def hdfs_results_dir_url(self): return f"{self.hdfs_url}/results"
