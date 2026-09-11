from aws_cdk import (
    aws_dynamodb as dynamodb, 
    RemovalPolicy,
    Stack,
    aws_lambda as _lambda,
    aws_events as events, 
    aws_events_targets as events_targets,
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

        idempotency_table = dynamodb.Table(
            self, "IdempotencyTable",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fake_rebooking = _lambda.Function(
            self, "FakeRebookingApi",
            runtime = _lambda.Runtime.PYTHON_3_12,
            handler = "handler.handler",
            code = _lambda.Code.from_asset("lambdas/fake_rebooking_api"),
            environment = {"FAILURE_RATE": "0"},
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
                "IDEMPOTENCY_TABLE_NAME": idempotency_table.table_name,
                "PROCESSING_TIMEOUT_SECONDS": "60",
            },
        )

        bus = events.EventBus(self, "FlightDisruptionBus")
        producer_lambda = _lambda.Function(
            self, "ProducerLambda",
            runtime = _lambda.Runtime.PYTHON_3_12,
            handler = "handler.handler",
            code = _lambda.Code.from_asset("lambdas/producer"),
            environment={"EVENT_BUS_NAME": bus.event_bus_name},    
        )
        producer_url = producer_lambda.add_function_url(auth_type=_lambda.FunctionUrlAuthType.NONE)

        rule = events.Rule(
            self, "RouteToRebooking",
            event_bus=bus,
            event_pattern=events.EventPattern(detail_type=["FlightDisrupted"]),
        )
        rule.add_target(events_targets.LambdaFunction(rebooking_lambda))

        bus.grant_put_events_to(producer_lambda)
        table.grant_read_write_data(rebooking_lambda)
        idempotency_table.grant_read_write_data(rebooking_lambda)


        CfnOutput(self, "FakeRebookingApiUrl", value=fake_rebooking_url.url)
        CfnOutput(self, "RebookingLambdaName", value=rebooking_lambda.function_name)
        CfnOutput(self, "ProducerLambdaUrl", value=producer_url.url)


