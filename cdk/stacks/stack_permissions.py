from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lakeformation as lf,
)

class MPSApiToS3StackPermissions(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        #The best practices is to have this variables in a config file or in SSM Parameter Store
        account_id = "916253565974"
        bucket_name = "s3-mps-group-user-data"
        prefix = "users/"
        principal_arn = f"arn:aws:iam::{account_id}:role/MPS-Group-ETL-Stack-Resourc-GlueCrawlerRole"
        db_name = "glue_database_users_mps"
        s3_location_arn = f"arn:aws:s3:::{bucket_name}/{prefix}"

        
        lf_resource = lf.CfnResource(
            self, "LfDataLocation",
            resource_arn=s3_location_arn,
            use_service_linked_role=True
        )

        # 2) DATA_LOCATION Permissions
        data_loc_perms = lf.CfnPermissions(
            self, "LfPermsDataLocation",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=principal_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=s3_location_arn 
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"]
        )
        data_loc_perms.add_dependency(lf_resource)

        # 2) Glue database Permissions
        lf.CfnPermissions(
            self, "LfPermsDatabase",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=principal_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=account_id,
                    name=db_name
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE", "DROP"]
        )

        #Permissions for usr_athena in some columns
        lf.CfnPermissions(
            self, "UsrAthenaSelectColumns",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier="arn:aws:iam::916253565974:user/usr_athena"
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
                    catalog_id=account_id,
                    database_name=db_name,
                    name="users",
                    column_names=["email","gender"]
                )
            ),
            permissions=["SELECT"]
        )

        
     
        