#!/usr/bin/env python3
"""
AWS Lambda Function for Processing Badge Evaluation Requests

This Lambda function processes badge evaluation requests from SQS using MongoDB
for status tracking and your existing validation scripts.
"""

import json
import os
import sys
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the parent directory to the path to import existing modules
sys.path.append('/opt/python')  # Lambda layer path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from badge_system.core.kpi_validator import KpiValidator
from badge_system.core.criteria_evaluator import CriteriaEvaluator
from badge_system.core.badge_verifier import BadgeVerifier
from badge_system.client.http_client import HttpClient
from badge_system.exceptions import DBError, HTTPRequestError


class LambdaBadgeProcessor:
    """
    Lambda Badge Processor class to handle badge evaluation in AWS Lambda.
    """
    
    def __init__(self, shared_db_client: Optional[AsyncIOMotorClient] = None, 
                 shared_db = None):
        """Initialize the Lambda Badge Processor."""
        # Load environment variables
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("MONGO_DB_NAME", "ensogove")
        self.base_url = os.getenv("BASE_URL")
        self.region_url = os.getenv("REGION_API_URL")
        self.token = os.getenv("TOKEN")
        
        # Validate required environment variables
        if not all([self.mongo_uri, self.base_url, self.token]):
            raise ValueError("Missing required environment variables")
        
        # Shared MongoDB connection
        if shared_db_client and shared_db:
            self.client = shared_db_client
            self.db = shared_db
            self._owns_connection = False  # Don't close shared connection
            print("✅ Lambda Badge processor using shared MongoDB connection")
        else:
            self.client = None
            self.db = None
            self._owns_connection = True  # Close own connection when done
        
        # Components that will share the connection
        self.http_client = None
        self.kpi_validator = None
        self.criteria_evaluator = None
        self.badge_verifier = None
        
        self.queue_collection = "badge_evaluation_queue"
    
    async def connect_to_db(self):
        """Establish connection to MongoDB and initialize components with shared connection."""
        try:
            # Create single MongoDB connection using motor
            if  self._owns_connection:
                self.client = AsyncIOMotorClient(self.mongo_uri)
                self.db = self.client[self.db_name]
                print("✅ Lambda Badge processor connected to MongoDB")
            
            # Initialize HTTP client
            self.http_client = HttpClient(
                base_url=self.base_url,
                region_url=self.region_url,
                token=self.token
            )
            
            # Initialize validators with shared database connection
            self.kpi_validator = KpiValidator(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                base_url=self.base_url,
                region_url=self.region_url,
                http_client=self.http_client,
                shared_db_client=self.client,  # Pass shared connection
                shared_db=self.db
            )
            
            self.criteria_evaluator = CriteriaEvaluator(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                shared_db_client=self.client,  # Pass shared connection
                shared_db=self.db
            )
            
            self.badge_verifier = BadgeVerifier(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                shared_db_client=self.client,  # Pass shared connection
                shared_db=self.db
            )
            
            # Since we're passing shared connections, we don't need to call connect_to_db on each component
            print("✅ Lambda Badge processor connected to MongoDB and initialized components with shared connection")
            
        except Exception as e:
            print(f"❌ Lambda Badge processor Error connecting to database: {e}")
            raise
    
    def disconnect_from_db(self):
        """Close MongoDB connection and cleanup components."""
        try:
            # Components don't need individual disconnection since they share the connection
            if self.client:
                self.client.close()
                self.client = None
                self.db = None
                
            print("🔌Lambda Badge Processor Disconnected from MongoDB and cleaned up components")
        except Exception as e:
            print(f"❌ Error disconnecting: {e}")
    
    async def update_status(self, company_id: str, site_code: str, badge_id: str, 
                           status: str, message_id: str = None, result: Dict[str, Any] = None) -> bool:
        """
        Update badge evaluation status in MongoDB.
        """
        try:
            now = datetime.now(timezone.utc)
            
            # Create filter for the document
            filter_criteria = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id
            }
            
            # Create update document
            update_doc = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id,
                "status": status,
                "updated_at": now
            }
            
            if message_id:
                update_doc["sqs_message_id"] = message_id
                
            if result:
                update_doc["evaluation_result"] = result
                
            if status == "completed" or status == "failed":
                update_doc["completed_at"] = now
            elif status == "processing":
                update_doc["processing_started_at"] = now
            
            # Add to processing history
            history_entry = {
                "status": status,
                "timestamp": now
            }
            
            if result:
                history_entry["result_summary"] = {
                    "success": result.get("success", False),
                    "eligible": result.get("eligible", False)
                }
                
            
            # Upsert the document
            await self.db[self.queue_collection].update_one(
                filter_criteria,
                {
                    "$set": update_doc,
                    "$push": {"processing_history": history_entry}
                },
                upsert=True
            )
            
            print(f"✅ Updated status in MongoDB: {status}")
            return True
            
        except Exception as e:
            print(f"❌ Error updating status: {e}")
            return False
    
    async def process_badge_evaluation(self, company_id: str, site_code: str, badge_id: str, 
                                     message_id: str) -> Dict[str, Any]:
        """
        Process a badge evaluation request.
        """
        try:
            print(f"🔄 Processing badge evaluation: Company {company_id}, Site {site_code}, Badge {badge_id}")
            
            # Update status to processing
            await self.update_status(
                company_id=company_id,
                site_code=site_code,
                badge_id=badge_id,
                status="processing",
                message_id=message_id
            )
            
            # Step 1: Get badge info to find associated criteria
            badge_info = await self.badge_verifier.get_badge_info(badge_id)
            
            if not badge_info["found"]:
                raise DBError(
                    status=404,
                    reason="Badge not found",
                    collection="badges",
                    message=f"Badge {badge_id} not found in database"
                )
            
            criteria_id = badge_info["criteria_id"]
            print(f"📋 Badge {badge_id} is associated with criteria {criteria_id}")
            
            # Step 2: Get criteria info to find associated KPIs
            criteria_doc = await self.db.criterias.find_one({"criteria_id": criteria_id})
            
            if not criteria_doc:
                raise DBError(
                    status=404,
                    reason="Criteria not found",
                    collection="criterias",
                    message=f"Criteria {criteria_id} not found in database"
                )
            
            kpi_ids = criteria_doc.get("kpi_ids", [])
            
            if not kpi_ids:
                raise DBError(
                    status=400,
                    reason="No KPIs found",
                    collection="criterias",
                    message=f"No KPI IDs found for criteria {criteria_id}"
                )
            
            print(f"📋 Found {len(kpi_ids)} KPIs for criteria {criteria_id}")
            
            # Step 3: Validate each KPI
            kpi_results = []
            for kpi_id in kpi_ids:
                print(f"🔍 Validating KPI {kpi_id}")
                
                try:
                    kpi_result = await self.kpi_validator.validate_and_save_kpi(
                        company_id=company_id,
                        site_code=site_code,
                        kpi_id=kpi_id
                    )
                    
                    kpi_results.append({
                        "kpi_id": kpi_id,
                        "result": kpi_result
                    })
                    
                    print(f"✅ KPI {kpi_id} validation completed")
                    
                except Exception as e:
                    print(f"❌ Error validating KPI {kpi_id}: {e}")
                    kpi_results.append({
                        "kpi_id": kpi_id,
                        "error": str(e)
                    })
            
            # Step 4: Evaluate criteria based on KPI results
            print(f"🔍 Evaluating criteria {criteria_id}")
            
            try:
                criteria_result = await self.criteria_evaluator.evaluate_and_save_criteria(
                    criteria_id=criteria_id,
                    company_id=company_id,
                    site_code=site_code
                )
                
                print(f"✅ Criteria {criteria_id} evaluation completed")
                
            except Exception as e:
                print(f"❌ Error evaluating criteria {criteria_id}: {e}")
                return {
                    "success": False,
                    "error": f"Criteria evaluation failed: {str(e)}",
                    "kpi_results": kpi_results
                }
            
            # Step 5: Verify badge eligibility
            print(f"🔍 Verifying badge {badge_id} eligibility")
            
            try:
                badge_result = await self.badge_verifier.verify_and_save_badge(
                    badge_id=badge_id,
                    company_id=company_id,
                    site_code=site_code
                )
                
                verification_result = badge_result.get("verification_result", {})
                eligible = verification_result.get("eligible", False)
                
                print(f"✅ Badge {badge_id} verification completed. Eligible: {eligible}")
                
                return {
                    "success": True,
                    "eligible": eligible,
                    "badge_id": badge_id,
                    "criteria_id": criteria_id,
                    "badge_result": badge_result,
                    "criteria_result": criteria_result,
                    "kpi_results": kpi_results
                }
                
            except Exception as e:
                print(f"❌ Error verifying badge {badge_id}: {e}")
                return {
                    "success": False,
                    "error": f"Badge verification failed: {str(e)}",
                    "criteria_result": criteria_result,
                    "kpi_results": kpi_results
                }
            
        except DBError as e:
            print(f"❌ Database error: {e}")
            return {
                "success": False,
                "error": f"Database error: {str(e)}"
            }
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return {
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            }


# Global processor instance (reused across Lambda invocations)
processor = None

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing badge evaluation requests from SQS.
    """
    global processor
    
    try:
        # Initialize processor if not already done
        if processor is None:
            processor = LambdaBadgeProcessor()
        
        # Extract records from event
        records = event.get('Records', [])
        
        if not records:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No records in event"})
            }
        
        # Process records in async context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(process_records(records))
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f"Processed {len(results)} records",
                    "results": results
                }, default=str)
            }
            
        finally:
            loop.close()
    
    except Exception as e:
        print(f"❌ Unexpected error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

async def process_records(records):
    """Process SQS records asynchronously."""
    global processor
    
    # Connect to database
    await processor.connect_to_db()
    
    try:
        results = []
        
        for record in records:
            try:
                # Parse message body
                body = json.loads(record['body'])
                
                company_id = body.get('company_id')
                site_code = body.get('site_code')
                badge_id = body.get('badge_id')
                message_id = record.get('messageId')
                
                if not all([company_id, site_code, badge_id]):
                    results.append({
                        "success": False,
                        "error": "Missing required fields in message",
                        "message_id": message_id
                    })
                    continue
                
                # Process the badge evaluation
                result = await processor.process_badge_evaluation(
                    company_id=company_id,
                    site_code=site_code,
                    badge_id=badge_id,
                    message_id=message_id
                )
                
                # Update final status
                if result.get("success", False):
                    await processor.update_status(
                        company_id=company_id,
                        site_code=site_code,
                        badge_id=badge_id,
                        status="completed",
                        message_id=message_id,
                        result=result
                    )
                else:
                    await processor.update_status(
                        company_id=company_id,
                        site_code=site_code,
                        badge_id=badge_id,
                        status="failed",
                        message_id=message_id,
                        result=result
                    )
                
                results.append({
                    "success": result.get("success", False),
                    "company_id": company_id,
                    "site_code": site_code,
                    "badge_id": badge_id,
                    "eligible": result.get("eligible", False),
                    "message_id": message_id
                })
                
            except Exception as e:
                print(f"❌ Error processing record: {e}")
                results.append({
                    "success": False,
                    "error": str(e),
                    "message_id": record.get('messageId')
                })
        
        return results
        
    finally:
        # Clean up connections
        processor.disconnect_from_db()
