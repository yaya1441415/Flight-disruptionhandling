from aws_cdk import (
    aws_dynamodb as dynamodb, 
    RemovalPolicy,
    Stack,
    aws_lambda as _lambda,
    CfnOutput,
)
from constructs import Construct

class FlightDisruptionHandlingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = dynamodb.Table(
            self, "BreakerStateTable",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fake_rebooking = _lambda.Function(
            self, "FakeRebookingApi",
            runtime = _lambda.Runtime.PYTHON_3_12,
            handler = "handler.handler",
            code = _lambda.Code.from_asset("lambdas/fake_rebooking_api"),
            environment = {"FAILURE_RATE": "0.5"},
        )

        fake_rebooking_url= fake_rebooking.add_function_url(auth_type=_lambda.FunctionUrlAuthType.NONE)

        rebooking_lambda = _lambda.Function(
            self, "RebookingLambda",
            runtime = _lambda.Runtime.PYTHON_3_12,
            handler = "handler.handler",
            code = _lambda.Code.from_asset("lambdas/rebooking_api"),
            environment={
                "TABLE_NAME": table.table_name,
                "FAKE_API_URL": fake_rebooking_url.url,
                "COOLDOWN_SECONDS": "30",
                "FAILURE_THRESHOLD": "5",
            },
        )
        table.grant_read_write_data(rebooking_lambda)


        CfnOutput(self, "FakeRebookingApiUrl", value=fake_rebooking_url.url)

