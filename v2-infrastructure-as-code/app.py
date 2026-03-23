import aws_cdk as cdk
from stack.pipeline_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "DataPipelineStack")
app.synth()
