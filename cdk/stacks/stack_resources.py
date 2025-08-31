from pathlib import Path
from constructs import Construct
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    aws_athena as athena,
)

class MPSApiToS3StackResources(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Bucket to save user data api
        bucket = s3.Bucket(
            self,'UserDataBucket',
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN,
            bucket_name="s3-mps-group-user-data" 
        )

        # bucket to save athena results
        bucket_athena = s3.Bucket(
            self,'UserDataBucket_Athena',
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN,
            bucket_name="s3-mps-group-user-data-athena-results"  # nombre único global
        )

        
        #Glue Database, Crawler and Athena Workgroup
        # 1) Glue Database
        glue_db = glue.CfnDatabase(
            self, "Glue_database_MPS",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="glue_database_users_mps",
                description="Catálogo para datos de usuarios de api en formato parquet",
            ),
        )

        # 2) Crawler Role
        crawler_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Rol para Glue Crawler que escanea S3 y actualiza el Data Catalog",
            role_name="MPS-Group-ETL-Stack-Resourc-GlueCrawlerRole",
            inline_policies={
                "GlueCrawlerPolicy": iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        actions=[
                            "s3:GetObject","s3:ListBucket","s3:GetBucketLocation"
                        ],
                        resources=[
                            f"arn:aws:s3:::{bucket.bucket_name}",
                            f"arn:aws:s3:::{bucket.bucket_name}/users/*",
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "glue:*Database*", "glue:CreateTable", "glue:UpdateTable",
                            "glue:GetTable*", "glue:BatchCreatePartition",
                            "glue:CreatePartition", "glue:UpdatePartition",
                            "glue:GetPartition*", "glue:BatchGetPartition",
                        ],
                        resources=["*"],  
                    ),
                    iam.PolicyStatement(
                        actions=["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
                        resources=["*"],
                    ),
                ])
            }
        )

        crawler = glue.CfnCrawler(
            self, "ParquetCrawler",
            role=crawler_role.role_arn,
            database_name=glue_db.ref, 
            name="users_MPS_parquet_crawler",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{bucket.bucket_name}/users/"
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG", 
                update_behavior="UPDATE_IN_DATABASE"
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING" 
            ),
            configuration='''{
            "Version":1.0,
            "CrawlerOutput":{
                "Partitions":{"AddOrUpdateBehavior":"InheritFromTable"},
                "Tables":{"AddOrUpdateBehavior":"MergeNewColumns"}
            }
            }''',
        )

        crawler.add_dependency(glue_db)


        # 3) Athena Workgroup
        athena.CfnWorkGroup(
            self, "AthenaWorkGroup",
            name="mps_group_workgroup",
            description="Athena WorkGroup to query data from MPS S3 bucket",
            recursive_delete_option=True, 
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                bytes_scanned_cutoff_per_query=10 * 1024 * 1024 * 1024,  # 10 GB
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket_athena.bucket_name}/query-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )