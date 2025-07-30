#!/usr/bin/env python3
"""
AWS SQS Queue Client

A client script to interact with the AWS SQS badge evaluation queue.
This script allows you to send badge evaluation requests and check status.
"""

import os
import sys
import json
import traceback
import boto3
import asyncio
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

class AWSBadgeQueueClient:
    """
    AWS Badge Queue Client to interact with SQS and MongoDB.
    """
    
    def __init__(self, queue_url: str, region_name: str = "us-east-1", shared_db_client: Optional[AsyncIOMotorClient] = None, 
                 shared_db = None):
        """
        Initialize the AWS Badge Queue Client.
        
        Args:
            queue_url: URL of the SQS queue
            region_name: AWS region name
        """
        self.queue_url = queue_url
        self.region_name = region_name
        self.sqs = boto3.client('sqs', region_name=region_name)
        
        # MongoDB connection for status tracking using motor
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("MONGO_DB_NAME", "ensogove")
       # Use shared connection if provided, otherwise create own
        if shared_db_client and shared_db:
            self.client = shared_db_client
            self.db = shared_db
            self._owns_connection = False  # Don't close shared connection
            print("✅ KPI Validator using shared MongoDB connection")
        else:
            self.client = None
            self.db = None
            self._owns_connection = True  # Close own connection when done
            
        self.queue_collection = "badge_evaluation_queue"
        
        print(f"✅ Connected to SQS queue: {queue_url}")
    
    async def connect_to_db(self):
        """Establish connection to MongoDB using Motor (only if not using shared connection)."""
        if not self._owns_connection:
            # Already using shared connection, no need to connect
            return
            
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            print("✅ KPI Validator connected to MongoDB")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            raise
    
    # Disconnect from MongoDB and close the client connection.
    def disconnect_from_db(self):
        """Close MongoDB connection (only if we own the connection)."""
        if not self._owns_connection:
            # Don't close shared connection
            return
            
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            print("🔌 KPI Validator disconnected from MongoDB")
    
    async def __aenter__(self):
        if self._owns_connection:
            await self.connect_to_db()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print("⚠️ Exception caught in KPI Validator async context manager:")
            print(f"  Type: {exc_type.__name__}")
            print(f"  Message: {exc_val}")
        tb_lines = traceback.format_exception(exc_type, exc_val, exc_tb)
        formatted_traceback = ''.join(tb_lines)
        print("🔍 Traceback:")
        print(formatted_traceback)
        if self._owns_connection:
            self.disconnect_from_db()
    
    def send_badge_evaluation_request(self, company_id: str, site_code: str, badge_id: str, 
                                     priority: int = 1, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Send a badge evaluation request to the SQS queue.
        """
        try:
            now = datetime.now().isoformat()
            
            # Create message body
            message_body = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id,
                "priority": priority,
                "metadata": metadata or {},
                "created_at": now
            }
            
            # Convert to JSON string
            message_json = json.dumps(message_body)
            
            # Send message to SQS
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_json,
                MessageAttributes={
                    'Priority': {
                        'DataType': 'Number',
                        'StringValue': str(priority)
                    },
                    'CompanyId': {
                        'DataType': 'String',
                        'StringValue': str(company_id)
                    },
                    'SiteCode': {
                        'DataType': 'String',
                        'StringValue': site_code
                    },
                    'BadgeId': {
                        'DataType': 'String',
                        'StringValue': badge_id
                    }
                }
            )
            
            print(f"✅ Sent to queue: Company {company_id}, Site {site_code}, Badge {badge_id}")
            
            return {
                "success": True,
                "message_id": response.get('MessageId'),
                "status": "pending"
            }
            
        except Exception as e:
            print(f"❌ Error sending to queue: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_status(self, company_id: str, site_code: str, badge_id: str) -> Dict[str, Any]:
        """
        Get badge evaluation status from MongoDB.
        """
        try:
            # Get document from MongoDB
            document = await self.db[self.queue_collection].find_one({
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id
            })
            
            if not document:
                return {
                    "found": False,
                    "status": "not_found"
                }
            
            return {
                "found": True,
                "company_id": document.get("company_id"),
                "site_code": document.get("site_code"),
                "badge_id": document.get("badge_id"),
                "status": document.get("status"),
                "sqs_message_id": document.get("sqs_message_id"),
                "created_at": document.get("created_at"),
                "updated_at": document.get("updated_at"),
                "completed_at": document.get("completed_at"),
                "processing_history": document.get("processing_history", []),
                "evaluation_result": document.get("evaluation_result")
            }
            
        except Exception as e:
            print(f"❌ Error getting status: {e}")
            return {
                "found": False,
                "error": str(e)
            }
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the queue from both SQS and MongoDB.
        """
        try:
            # Get SQS queue attributes
            sqs_response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['All']
            )
            
            sqs_attributes = sqs_response.get('Attributes', {})
            
            # Get MongoDB statistics
            pending_count = await self.db[self.queue_collection].count_documents({"status": "pending"})
            processing_count = await self.db[self.queue_collection].count_documents({"status": "processing"})
            completed_count = await self.db[self.queue_collection].count_documents({"status": "completed"})
            failed_count = await self.db[self.queue_collection].count_documents({"status": "failed"})
            
            return {
                "sqs_stats": {
                    "approximate_messages": int(sqs_attributes.get('ApproximateNumberOfMessages', 0)),
                    "approximate_messages_not_visible": int(sqs_attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
                    "approximate_messages_delayed": int(sqs_attributes.get('ApproximateNumberOfMessagesDelayed', 0))
                },
                "mongodb_stats": {
                    "pending": pending_count,
                    "processing": processing_count,
                    "completed": completed_count,
                    "failed": failed_count,
                    "total": pending_count + processing_count + completed_count + failed_count
                }
            }
            
        except Exception as e:
            print(f"❌ Error getting queue stats: {e}")
            return {"error": str(e)}
    
    async def list_recent_evaluations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List recent badge evaluations from MongoDB.
        """
        try:
            cursor = self.db[self.queue_collection].find().sort("updated_at", -1).limit(limit)
            
            evaluations = []
            async for doc in cursor:
                # Convert ObjectId to string
                doc["_id"] = str(doc["_id"])
                evaluations.append(doc)
            
            return evaluations
            
        except Exception as e:
            print(f"❌ Error listing recent evaluations: {e}")
            return []


async def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description="AWS SQS Badge Queue Client")
    
    # Command options
    parser.add_argument("--command", choices=["send", "status", "stats", "list"], 
                      required=True, help="Command to execute")
    
    # Send parameters
    parser.add_argument("--company_id", help="Company ID")
    parser.add_argument("--site_code", help="Site code")
    parser.add_argument("--badge_id", help="Badge ID")
    parser.add_argument("--priority", type=int, default=1, help="Priority for send command")
    
    # List parameters
    parser.add_argument("--limit", type=int, default=10, help="Limit for list command")
    
    args = parser.parse_args()
    
    # Load environment variables
    QUEUE_URL = os.getenv("SQS_QUEUE_URL")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    
    if not QUEUE_URL:
        print("❌ Missing SQS_QUEUE_URL environment variable")
        sys.exit(1)
    
    try:
        # Initialize AWS Badge Queue Client
        client = AWSBadgeQueueClient(
            queue_url=QUEUE_URL,
            region_name=AWS_REGION
        )
        
        if args.command == "send":
            # Validate required parameters
            if not all([args.company_id, args.site_code, args.badge_id]):
                print("❌ Missing required parameters for send command")
                sys.exit(1)
            
            # Send to queue
            result = client.send_badge_evaluation_request(
                company_id=args.company_id,
                site_code=args.site_code,
                badge_id=args.badge_id,
                priority=args.priority
            )
            
            print("\n📋 Send Result:")
            print(json.dumps(result, indent=2, default=str))
            
        elif args.command == "status":
            # Validate required parameters
            if not all([args.company_id, args.site_code, args.badge_id]):
                print("❌ Missing required parameters for status command")
                sys.exit(1)
            
            async with client:
                # Get status
                status = await client.get_status(
                    company_id=args.company_id,
                    site_code=args.site_code,
                    badge_id=args.badge_id
                )
                
                print("\n📋 Status Result:")
                print(json.dumps(status, indent=2, default=str))
            
        elif args.command == "stats":
            async with client:
                # Get stats
                stats = await client.get_queue_stats()
                
                print("\n📊 Queue Statistics:")
                print(json.dumps(stats, indent=2, default=str))
            
        elif args.command == "list":
            async with client:
                # List recent evaluations
                evaluations = await client.list_recent_evaluations(limit=args.limit)
                
                print(f"\n📋 Recent {len(evaluations)} Evaluations:")
                print(json.dumps(evaluations, indent=2, default=str))
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
