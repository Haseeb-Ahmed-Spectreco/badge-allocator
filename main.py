"""
AWS Lambda Handler - Badge Evaluation System
Main entry point for AWS Lambda deployment
"""

import json
import sys
import os

# Add current directory to Python path for Lambda environment
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from badge_system.aws.lambda_processor import lambda_function
except ImportError as e:
    print(f"Import error: {e}")

def lambda_handler(event, context):
    """
    AWS Lambda entry point for badge evaluation system.
    
    Args:
        event: AWS Lambda event object
        context: AWS Lambda context object
        
    Returns:
        dict: Lambda response with statusCode and body
    """
    try:
        # Log the invocation
        print(f"Lambda invoked: {context.function_name}")
        print(f"Request ID: {context.aws_request_id}")
        print(f"Remaining time: {context.get_remaining_time_in_millis()}ms")
        
        # Call the actual badge processing function
        result = lambda_function(event, context)
        
        # Ensure result has proper Lambda response format
        if not isinstance(result, dict) or 'statusCode' not in result:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "success": True,
                    "message": "Badge evaluation completed",
                    "result": result
                })
            }
        
        return result
        
    except Exception as e:
        print(f"Lambda handler error: {str(e)}")
        
        # Return error response in proper Lambda format
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "error": str(e),
                "message": "Badge evaluation failed"
            })
        }
