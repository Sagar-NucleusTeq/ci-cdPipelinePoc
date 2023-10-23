# Use the official AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.8
# Copy the requirements file into the container
COPY /requirements/requirements.txt .
# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Copy your Lambda function code into the container
COPY lambda_function_policy_search.py .
# Specify the command to run your Lambda function handler
CMD ["lambda_function_policy_search.lambda_handler"]