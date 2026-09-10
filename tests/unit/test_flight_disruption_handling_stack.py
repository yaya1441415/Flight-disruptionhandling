import aws_cdk as core
import aws_cdk.assertions as assertions

from flight_disruption_handling.flight_disruption_handling_stack import FlightDisruptionHandlingStack

# example tests. To run these tests, uncomment this file along with the example
# resource in flight_disruption_handling/flight_disruption_handling_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FlightDisruptionHandlingStack(app, "flight-disruption-handling")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
