storeez-batch-pipeline-poc/
├── README.md
├── images/                    
├── requirements/                  
│   ├── business_requirements.md    
│   ├── system_requirements.md      
│   └── tools_and_tech.md          
├── data/                          
│   ├── raw/                       
│   ├── processed/                 
│   └── fake_data/                 
│       ├── generate_fake_data.py  
│       └── README.md              
├── docker/                        
│   ├── docker-compose.yml          
│   ├── spark/                     
│   │   ├── Dockerfile              # Dockerfile for Spark service
│   ├── airflow/                   
│   │   ├── Dockerfile              # Dockerfile for Airflow service
│   ├── duckdb/                    
│   ├── dbt/                       
│   └── metabase/ 
├── code/                          
│   ├── spark_jobs/                # Directory for Spark jobs
│   │   ├── job1.py                 # Example Spark job
│   │   ├── job2.py                 # Another Spark job
│   │   └── README.md               # Instructions for Spark jobs
│   ├── airflow_dags/              # Directory for Airflow DAGs
│   │   ├── dag1.py                 # Example Airflow DAG
│   │   ├── dag2.py                 # Another DAG
│   │   └── README.md               # Instructions for setting up and running DAGs                # Documentation for the dbt project
│   └── scripts/                    # Other scripts (e.g., data quality checks)
│       ├── data_quality_checks.py   
│       └── README.md  
├── dbt_project/                # Directory for the dbt project
│   ├── models/                 # dbt models directory
│   ├── seeds/                  # Seed files (if any)
│   ├── snapshots/               # Snapshot files (if any)
│   ├── dbt_project.yml          # dbt project configuration file
│   └── README.md                   
└──               
