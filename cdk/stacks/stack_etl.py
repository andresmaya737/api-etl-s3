from pathlib import Path
from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    BundlingOptions,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
)

class MPSApiToS3StackETL(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)


        # the root of the cdk directory
        cdk_dir   = Path(__file__).resolve().parent
        repo_root = cdk_dir.parent.parent

        # Bundling for the lambda using docker
        bundling = BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_11.bundling_image,
            command=[
                "bash","-lc",
                r"""
                set -euo pipefail
                export PIP_DISABLE_PIP_VERSION_CHECK=1
                export PIP_NO_CACHE_DIR=1
                echo '--- ls /asset-input ---' && ls -la /asset-input || true
                echo '--- ls /asset-input/lambda/app_lambda ---' && ls -la /asset-input/lambda/app_lambda || true
                echo '--- ls /asset-input/src ---' && ls -la /asset-input/src || true

                if [ -s /asset-input/lambda/app_lambda/requirements.txt ]; then
                    pip install --no-cache-dir --prefer-binary --only-binary=:all: \
                        -r /asset-input/lambda/app_lambda/requirements.txt -t /asset-output
                else
                    echo 'sin requirements.txt'
                fi

                cp /asset-input/lambda/app_lambda/handler.py /asset-output/handler.py
                mkdir -p /asset-output/app
                cp -r /asset-input/src/app /asset-output/

                echo '=== Contenido final del artifact ==='
                ls -lah /asset-output
                ls -lah /asset-output/app || true
                """
            ],
        )

        code = _lambda.Code.from_asset(
            path=str(repo_root),
            bundling=bundling,
        )

        # AWS wrangler
        AWSWRANGLER_ARN = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:22"

        # Lambda function   
        fn = _lambda.Function(
            self, 'ETLUsersFunction',
            function_name="func-etl-users-to-s3",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler", 
            code=code,
            timeout=Duration.seconds(60),
            layers=[
                _lambda.LayerVersion.from_layer_version_arn(
                    self, "WranglerLayer", AWSWRANGLER_ARN
                )
            ],
            memory_size=512,
            environment={
                "API_BASE_URL": "https://randomuser.me",
                "S3_BUCKET": "s3-mps-group-user-data",
                "S3_PREFIX": "users/",
                "HTTP_TIMEOUT": "15",
                "LOG_LEVEL": "INFO",
            },
        )

        
        data_bucket = s3.Bucket.from_bucket_name(
            self, "ImportedDataBucket", bucket_name="s3-mps-group-user-data"
        )
        data_bucket.grant_write(fn)


        # EventBridge Rule to trigger the Lambda every hour
        rule = events.Rule(
            self, "scheduleRule",
            schedule=events.Schedule.cron(minute="0"),
        )
        rule.add_target(targets.LambdaFunction(fn))
