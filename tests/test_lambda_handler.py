#!/usr/bin/env python3
"""
Lambda Testing Utilities - Standalone script for testing Lambda handler
Run this script directly to test your Lambda handler with various scenarios
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

# Add your project path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def create_sqs_event(records_data):
    """Create a realistic SQS event for testing"""
    records = []
    for i, data in enumerate(records_data):
        record = {
            "messageId": f"test-message-{i+1}",
            "receiptHandle": f"test-receipt-handle-{i+1}",
            "body": json.dumps(data),
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1643723400000",
                "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                "ApproximateFirstReceiveTimestamp": "1643723400000"
            },
            "messageAttributes": {},
            "md5OfBody": "test-md5-hash",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:badge-queue",
            "awsRegion": "us-east-1"
        }
        records.append(record)
    
    return {"Records": records}

def test_lambda_handler_manual():
    """
    Manual test of Lambda handler - modify as needed
    """
    print("🧪 Starting Manual Lambda Handler Test...")
    
    try:
        from badge_system.aws.lambda_processor import lambda_handler
        
        # Test environment variables (modify these for your setup)
        test_env = {
            'MONGO_URI': 'mongodb://localhost:27017/?directConnection=true&serverSelectionTimeoutMS=2000',
            'MONGO_DB_NAME': 'ensogove',
            'BASE_URL': 'https://devapi.spectreco.com',
            'REGION_API_URL': 'https://dev-region.spectreco.com',
            'TOKEN': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NjgzOTlhZDUwZjRmNzcwMWE0MWU0NzgiLCJmaXJzdF9uYW1lIjoiQWZhcSIsImxhc3RfbmFtZSI6IkFobWVkIiwiZW1haWwiOiJhZmFxMS4wMi45OEBnbWFpbC5jb20iLCJjb21wYW55Ijoic3BlY3RyZWNvIiwiY29tcGFueV9pZCI6IjM4MiIsInBhY2thZ2VfaWQiOiI2NThkNjMyNjZiNTVmZmM2YmEwMDlmODIiLCJwYXJlbnRfaWQiOm51bGwsImlzX2NoaWxkIjpmYWxzZSwicGFja2FnZV9leHBpcnkiOiJGcmkgQXVnIDAyIDIwMjQgMTE6Mjg6MTAgR01UKzA1MDAgKFBha2lzdGFuIFN0YW5kYXJkIFRpbWUpIiwiZm9yY2VDaGFuZ2VQYXNzd29yZCI6ZmFsc2UsInRyaWFsX2V4cCI6IldlZCBBdWcgMTcgMjAyNSAwNjowOTo0OSBHTVQrMDAwMCAoQ29vcmRpbmF0ZWQgVW5pdmVyc2FsIFRpbWUpIiwiZGlzYWJsZWRfdXNlciI6ZmFsc2UsImlhdCI6MTc0OTYyOTYyNiwiZXhwIjo0MzQxNjI5NjI2fQ.g53JWe6YCw8lsLfItl-dg0hllsuvwPr9jZImgSgpc9c'  # Replace with actual token
        }
        
        # Test scenarios
        test_scenarios = [
            {
                "name": "Single Valid Record",
                "records": [{"company_id": "555", "site_code": "MM-0012", "badge_id": "badge_bronze_001"}]
            },
            {
                "name": "Invalid Record (Missing badge_id)",
                "records": [{"company_id": "555", "site_code": "MM-0012"}]
            },
            {
                "name": "Empty Records",
                "records": []
            }
        ]
        
        for scenario in test_scenarios:
            print(f"\n🎯 Testing: {scenario['name']}")
            print(f"📋 Records: {len(scenario['records'])}")
            
            # Create event
            if scenario['records']:
                event = create_sqs_event(scenario['records'])
            else:
                event = {"Records": []}
            
            context = {}
            
            try:
                with patch.dict(os.environ, test_env):
                    # Reset global processor for clean test
                    import badge_system.aws.lambda_processor as lambda_module
                    lambda_module.processor = None
                    
                    result = lambda_handler(event, context)
                    
                    print(f"✅ Status Code: {result['statusCode']}")
                    body = json.loads(result['body'])
                    
                    if result['statusCode'] == 200:
                        print(f"📊 Message: {body.get('message', 'No message')}")
                        if 'results' in body:
                            print(f"📈 Results: {len(body['results'])} items")
                            for i, res in enumerate(body['results']):
                                success = res.get('success', False)
                                eligible = res.get('eligible', 'N/A')
                                print(f"   Result {i+1}: Success={success}, Eligible={eligible}")
                    else:
                        print(f"❌ Error: {body.get('error', 'Unknown error')}")
                        
            except Exception as e:
                print(f"❌ Test failed with exception: {e}")
        
        print("\n🎉 Manual testing completed!")
        
    except ImportError as e:
        print(f"❌ Could not import lambda_handler: {e}")
        print("Make sure your badge_system module is properly installed/configured")

def test_with_mocked_processor():
    """
    Test Lambda handler with mocked processor - always works
    """
    print("🧪 Testing Lambda Handler with Mocked Processor...")
    
    try:
        from badge_system.aws.lambda_processor import lambda_handler, LambdaBadgeProcessor
        from unittest.mock import AsyncMock, MagicMock, patch
        
        # Create test event
        event = create_sqs_event([
            {"company_id": "555", "site_code": "MM-0012", "badge_id": "badge_bronze_001"}
        ])
        context = {}
        
        # Mock all processor methods
        with patch.object(LambdaBadgeProcessor, 'connect_to_db', new_callable=AsyncMock) as mock_connect:
            with patch.object(LambdaBadgeProcessor, 'disconnect_from_db') as mock_disconnect:
                with patch.object(LambdaBadgeProcessor, 'process_badge_evaluation', new_callable=AsyncMock) as mock_process:
                    with patch.object(LambdaBadgeProcessor, 'update_status', new_callable=AsyncMock) as mock_update:
                        
                        # Setup mock responses
                        mock_connect.return_value = None
                        mock_disconnect.return_value = None
                        mock_update.return_value = True
                        mock_process.return_value = {
                            "success": True,
                            "eligible": True,
                            "badge_id": "badge_bronze_001",
                            "criteria_id": "safety_criteria_001"
                        }
                        
                        # Reset global processor
                        import badge_system.aws.lambda_processor as lambda_module
                        lambda_module.processor = None
                        
                        # Execute test
                        result = lambda_handler(event, context)
                        
                        print(f"✅ Status Code: {result['statusCode']}")
                        if result['statusCode'] == 200:
                            body = json.loads(result['body'])
                            print(f"📊 Processed: {body['message']}")
                            print(f"📈 Results: {len(body['results'])} items")
                            
                            result_item = body['results'][0]
                            print(f"   Success: {result_item['success']}")
                            print(f"   Eligible: {result_item['eligible']}")
                            print(f"   Company: {result_item['company_id']}")
                            print(f"   Site: {result_item['site_code']}")
                            print(f"   Badge: {result_item['badge_id']}")
                            
                            # Verify mocks were called
                            print(f"\n🔍 Mock Call Verification:")
                            print(f"   connect_to_db called: {mock_connect.called}")
                            print(f"   process_badge_evaluation called: {mock_process.called}")
                            print(f"   update_status called: {mock_update.call_count} times")
                            print(f"   disconnect_from_db called: {mock_disconnect.called}")
                            
                            print("✅ Mocked processor test PASSED!")
                        else:
                            body = json.loads(result['body'])
                            print(f"❌ Test failed: {body.get('error')}")
                            
    except Exception as e:
        print(f"❌ Mocked test failed: {e}")

def create_test_event_json():
    """Create a JSON file with test SQS events"""
    test_events = {
        "single_record": create_sqs_event([
            {"company_id": "555", "site_code": "MM-0012", "badge_id": "badge_bronze_001"}
        ]),
        "multiple_records": create_sqs_event([
            {"company_id": "555", "site_code": "MM-0012", "badge_id": "badge_bronze_001"},
            {"company_id": "777", "site_code": "HQ-0001", "badge_id": "badge_gold_003"},
            {"company_id": "888", "site_code": "OF-0201", "badge_id": "badge_silver_002"}
        ]),
        "invalid_record": create_sqs_event([
            {"company_id": "555", "site_code": "MM-0012"}  # Missing badge_id
        ]),
        "empty_event": {"Records": []}
    }
    
    output_file = "lambda_test_events.json"
    with open(output_file, 'w') as f:
        json.dump(test_events, f, indent=2)
    
    print(f"✅ Created test events file: {output_file}")
    return output_file

def run_performance_test():
    """
    Simple performance test with multiple records
    """
    print("⚡ Running Performance Test...")
    
    # Create large event with many records
    records_data = []
    for i in range(50):  # 50 records
        records_data.append({
            "company_id": f"{555 + (i % 3)}",  # Vary company IDs
            "site_code": f"SITE-{i:04d}",
            "badge_id": f"badge_test_{i:03d}"
        })
    
    event = create_sqs_event(records_data)
    
    try:
        from badge_system.aws.lambda_processor import lambda_handler, LambdaBadgeProcessor
        from unittest.mock import AsyncMock, patch
        import time
        
        with patch.object(LambdaBadgeProcessor, 'connect_to_db', new_callable=AsyncMock):
            with patch.object(LambdaBadgeProcessor, 'disconnect_from_db'):
                with patch.object(LambdaBadgeProcessor, 'process_badge_evaluation', new_callable=AsyncMock) as mock_process:
                    with patch.object(LambdaBadgeProcessor, 'update_status', new_callable=AsyncMock):
                        
                        mock_process.return_value = {"success": True, "eligible": True}
                        
                        # Reset global processor
                        import badge_system.aws.lambda_processor as lambda_module
                        lambda_module.processor = None
                        
                        start_time = time.time()
                        result = lambda_handler(event, {})
                        end_time = time.time()
                        
                        duration = end_time - start_time
                        
                        print(f"📊 Performance Results:")
                        print(f"   Records processed: {len(records_data)}")
                        print(f"   Total time: {duration:.2f} seconds")
                        print(f"   Average per record: {(duration/len(records_data)*1000):.2f} ms")
                        print(f"   Status code: {result['statusCode']}")
                        
                        if result['statusCode'] == 200:
                            body = json.loads(result['body'])
                            success_count = sum(1 for r in body['results'] if r.get('success'))
                            print(f"   Successful: {success_count}/{len(body['results'])}")
                            print("✅ Performance test completed!")
                        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")

if __name__ == "__main__":
    print("🚀 Lambda Handler Testing Utilities")
    print("=" * 50)
    
    import argparse
    parser = argparse.ArgumentParser(description="Test Lambda handler")
    parser.add_argument("--test", choices=["manual", "mocked", "performance", "events"], 
                       default="mocked", help="Type of test to run")
    
    args = parser.parse_args()
    
    if args.test == "manual":
        test_lambda_handler_manual()
    elif args.test == "mocked":
        test_with_mocked_processor()
    elif args.test == "performance":
        run_performance_test()
    elif args.test == "events":
        create_test_event_json()
    
    print("\n" + "=" * 50)
    print("✅ Testing utilities completed!")

# Quick test functions you can call directly
def quick_test():
    """Quick test - just call this function"""
    test_with_mocked_processor()

def quick_manual_test():
    """Quick manual test - modify environment variables first"""
    test_lambda_handler_manual()