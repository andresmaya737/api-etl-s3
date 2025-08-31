import aws_cdk as cdk
from stacks.stack_resources import MPSApiToS3StackResources
from stacks.stack_etl import MPSApiToS3StackETL
from stacks.stack_permissions import MPSApiToS3StackPermissions

app = cdk.App()
MPSApiToS3StackResources(app, "MPS-Group-ETL-Stack-Resources")
MPSApiToS3StackETL(app, "MPS-Group-ETL-Stack-ETL")
MPSApiToS3StackPermissions(app, "MPS-Group-ETL-Stack-Permissions")
app.synth()

