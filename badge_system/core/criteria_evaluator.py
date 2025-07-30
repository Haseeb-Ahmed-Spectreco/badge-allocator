#!/usr/bin/env python3
"""
Criteria Evaluation Script

A standalone Python script to evaluate criteria based on KPI validation results,
following the same architecture as the KPI validator.

Usage:
    python criteria_evaluator.py --company_id <id> --site_code <code> [--criteria_id <id>] [--framework_id <id>]

Example:
    python criteria_evaluator.py --company_id 555 --site_code MM-0012
    python criteria_evaluator.py --company_id 555 --site_code MM-0012 --criteria_id CRITERIA-001
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
import traceback
import asyncio

from badge_system.exceptions import DBError

load_dotenv()


class CriteriaEvaluator:
    """
    Criteria Evaluator class to evaluate criteria based on KPI validation results
    and save results to criteria_data collection.
    """
    
    # Constructor to initialize the KPI Validator with database and API configuration.

    def __init__(self, mongo_uri: str, db_name: str, shared_db_client: Optional[AsyncIOMotorClient] = None, shared_db = None):
        """
        Initialize the Criteria Evaluator with database configuration.
        
        Args:
            mongo_uri: MongoDB connection URI
            db_name: Database name
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        # Use shared connection if provided, otherwise create own
        if shared_db_client is not None and shared_db is not None:
            self.client = shared_db_client
            self.db = shared_db
            self._owns_connection = False  # Don't close shared connection
            print("✅ KPI Validator using shared MongoDB connection")
        else:
            self.client = None
            self.db = None
            self._owns_connection = True  # Close own connection when done
    
    ############################################################################
             # Methods for connecting and disconnecting from MongoDB
    ############################################################################

    # Connect to MongoDB asynchronously and initialize the database client.    
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

    
    ############################################################################
                        # Context manager for async operations
    ############################################################################
    
    # Async context manager to handle database connection lifecycle.
    async def __aenter__(self):
        if self._owns_connection:
            await self.connect_to_db()
        return self
    
    # Disconnect from MongoDB and close the client connection.
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print("⚠️  Exception caught in async context manager:")
            print(f"  Type: {exc_type.__name__}")
            print(f"  Message: {exc_val}")
             # Extract detailed traceback info
            tb_lines = traceback.format_exception(exc_type, exc_val, exc_tb)
            formatted_traceback = ''.join(tb_lines)
            print("🔍 Traceback:")
            print(formatted_traceback)
            
        if self._owns_connection:
            self.disconnect_from_db()


    ############################################################################
                # Helper Functions for data retrieval and validation
    ############################################################################
    
    def get_nested_value(self, obj: Dict[str, Any], path: str, default_value: Any = None) -> Any:
        """
        Get nested values from dictionary using dot notation.
        
        Args:
            obj: Dictionary to search in
            path: Dot-separated path (e.g., "validation_result.success")
            default_value: Value to return if path not found
            
        Returns:
            Value at the specified path or default_value
        """
        try:
            current = obj
            for part in path.split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return default_value
            return current
        except (KeyError, TypeError):
            return default_value


    ############################################################################
                        # Core KPI Data Analysis Functions
    ############################################################################
    
    async def get_kpi_data_for_criteria(self, criteria_id: str, company_id: str, site_code: str, 
                                       evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Get KPI data for a specific criteria, company, and site.
        
        Args:
            criteria_id: The criteria ID to evaluate
            company_id: Company identifier
            site_code: Site code
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            Dictionary containing KPI statistics and data
        """
        try:
            if evaluation_date is None:
                evaluation_date = datetime.now(timezone.utc)
            
            # Get the criteria document to find associated KPI IDs
            criteria_doc = await self.db.criterias.find_one({
                "criteria_id": criteria_id,
                "is_active": True
            })
            
            if not criteria_doc:
                print(f"⚠️  Criteria {criteria_id} not found or inactive")
                return {
                    "criteria_found": False,
                    "kpi_ids": [],
                    "total_kpis": 0,
                    "submitted_kpis": 0,
                    "incomplete_kpis": 0,
                    "success_kpis": 0,
                    "failed_kpis": 0,
                    "all_success": False,
                    "kpi_data": []
                }
            
            kpi_ids = criteria_doc.get("kpi_ids", [])
            
            if not kpi_ids:
                print(f"⚠️  No KPI IDs found for criteria {criteria_id}")
                return {
                    "criteria_found": True,
                    "kpi_ids": [],
                    "total_kpis": 0,
                    "completed_kpis": 0,
                    "incomplete_kpis": 0,
                    "success_kpis": 0,
                    "failed_kpis": 0,
                    "all_success": False,
                    "kpi_data": []
                }
            
            print(f"🔍 Analyzing {len(kpi_ids)} KPIs for criteria {criteria_id}")
            
            # Query KPI data for the specific criteria KPIs
            # Get the most recent evaluation for each KPI
            kpi_data_results = []
            submitted_kpis = []
            
            for kpi_id in kpi_ids:
                kpi_data = await self.db.kpi_data.find_one(
                    {
                        "kpi_id": kpi_id,
                        "company_id": str(company_id),
                        "site_code": site_code
                    },
                    sort=[("evaluation_date", -1)]  # Get most recent
                )
                
                if kpi_data:
                    kpi_data_results.append(kpi_data)
                    submitted_kpis.append(kpi_id)
            
            # Calculate statistics
            total_kpis = len(kpi_ids)
            submitted_kpis = len(kpi_data_results)
            success_kpis = sum(1 for kpi in kpi_data_results 
                            if self.get_nested_value(kpi, "validation_result.success", False))
            failed_kpis = submitted_kpis - success_kpis
            incomplete_kpis = total_kpis - submitted_kpis
            
            # All KPIs must be successful for criteria to be met
            all_success = (submitted_kpis == total_kpis and success_kpis == total_kpis)
            
            print(f"📊 KPI Statistics for {criteria_id}:")
            print(f"  Total KPIs: {total_kpis}")
            print(f"  Completed KPIs: {submitted_kpis}")
            print(f"  Successful KPIs: {success_kpis}")
            print(f"  Failed KPIs: {failed_kpis}")
            print(f"  All Success: {all_success}")
            
            return {
                "criteria_found": True,
                "kpi_ids": kpi_ids,
                "total_kpis": total_kpis,
                "submitted_kpis": submitted_kpis,
                "incomplete_kpis": incomplete_kpis,
                "success_kpis": success_kpis,
                "failed_kpis": failed_kpis,
                "all_success": all_success,
                "kpi_data": kpi_data_results
            }
            
        except Exception as e:
            print(f"❌ Error getting KPI data for criteria {criteria_id}: {str(e)}")
            return {
                "error": str(e),
                "criteria_found": False,
                "kpi_ids": [],
                "total_kpis": 0,
                "submitted_kpis": 0,
                "incomplete_kpis": 0,
                "success_kpis": 0,
                "failed_kpis": 0,
                "all_success": False,
                "kpi_data": []
            }


    ############################################################################
                        # Criteria Status Determination
    ############################################################################
    
    def determine_criteria_status(self, kpi_stats: Dict[str, Any]) -> str:
        """
        Determine the status of criteria based on KPI completion.
        
        Args:
            kpi_stats: Dictionary containing KPI statistics
            
        Returns:
            Status string: "achieved", "not_met", or "pending"
        """
        if not kpi_stats.get("criteria_found", False):
            return "pending"
        
        total_kpis = kpi_stats["total_kpis"]
        submitted_kpis = kpi_stats["submitted_kpis"]
        success_kpis = kpi_stats["success_kpis"]
        failed_kpis = kpi_stats["failed_kpis"]
        
        if total_kpis == 0:
            return "pending"
        
        # All KPIs completed and successful
        if submitted_kpis == total_kpis and success_kpis == total_kpis:
            return "achieved"
        
        # Some KPIs failed (completed but not successful)
        if failed_kpis > 0:
            return "not_met"
        
        # Not all KPIs completed yet
        return "pending"


    ############################################################################
                    # Main Evaluation and Saving Functions
    ############################################################################
    
    async def evaluate_and_save_criteria(self, criteria_id: str, company_id: str, site_code: str, 
                                        approver_ids: List[str] = None, 
                                        evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Evaluate a criteria and save the results to the database.
        
        Args:
            criteria_id: Criteria identifier
            company_id: Company identifier
            site_code: Site code
            approver_ids: List of approver user IDs (optional)
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            Dictionary containing evaluation and save results
        """
        try:
            print(f"🎯 Evaluating criteria: {criteria_id}")
            
            # Evaluate the criteria
            evaluation_result = await self.evaluate_criteria(criteria_id, company_id, site_code, evaluation_date)
            
            if(evaluation_result.get("success", False) is False):
                print(f"⚠️  Criteria {criteria_id} evaluation failed: {evaluation_result.get('error', 'Unknown error')}")
                # raise DBError(
                #     status=400,
                #     reason="Evaluation failed",
                #     collection="criteria_data",
                #     message=f"Criteria {criteria_id} evaluation failed: {evaluation_result.get('error', 'Unknown error')}"
                # )
                
            
            # Save the results to database
            save_result = await self.save_criteria_data(
                criteria_id=criteria_id,
                company_id=company_id,
                site_code=site_code,
                evaluation_result=evaluation_result,
                approver_ids=approver_ids,
                evaluation_date=evaluation_date
            )
            
            return {
                "evaluation_result": evaluation_result,
                "save_result": save_result
            }
        
        except DBError as e:
            raise e;
            
        except Exception as e:
            print(f"❌ Error in evaluate_and_save_criteria: {e}")
            return {"error": str(e), "status": 500}
    
    async def evaluate_criteria(self, criteria_id: str, company_id: str, site_code: str, 
                               evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Evaluate a criteria based on its KPI results.
        
        Args:
            criteria_id: Criteria identifier
            company_id: Company identifier
            site_code: Site code
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            Dictionary containing evaluation results
        """
        try:
            if evaluation_date is None:
                evaluation_date = datetime.now(timezone.utc)
            
            # Get KPI data for this criteria
            kpi_stats = await self.get_kpi_data_for_criteria(criteria_id, company_id, site_code, evaluation_date)
            
            if not kpi_stats.get("criteria_found", False):
                return {
                    "success": False,
                    "error": "Criteria not found or inactive",
                    "status": 404
                }
            
            # Determine status
            status = self.determine_criteria_status(kpi_stats)
            
            # Create detailed evaluation result
            evaluation_result = {
                "success": status == "achieved",
                "status": status,
                "criteria_id": criteria_id,
                "evaluation_date": evaluation_date.isoformat(),
                "kpi_summary": {
                    "total_kpis": kpi_stats["total_kpis"],
                    "submitted_kpis": kpi_stats["submitted_kpis"],
                    "incomplete_kpis": kpi_stats["incomplete_kpis"],
                    "success_kpis": kpi_stats["success_kpis"],
                    "failed_kpis": kpi_stats["failed_kpis"],
                    "completion_percentage": (kpi_stats["submitted_kpis"] / kpi_stats["total_kpis"] * 100) if kpi_stats["total_kpis"] > 0 else 0,
                    "success_percentage": (kpi_stats["success_kpis"] / kpi_stats["total_kpis"] * 100) if kpi_stats["total_kpis"] > 0 else 0
                },
                "kpi_details": []
            }
            
            # Add individual KPI details
            for kpi_data in kpi_stats["kpi_data"]:
                kpi_detail = {
                    "kpi_id": kpi_data.get("kpi_id"),
                    "complete": kpi_data.get("complete", False),
                    "success": self.get_nested_value(kpi_data, "validation_result.success", False),
                    "evaluation_date": kpi_data.get("evaluation_date"),
                    "validation_type": self.get_nested_value(kpi_data, "validation_result.type", "unknown")
                }
                evaluation_result["kpi_details"].append(kpi_detail)
            
            # Add missing KPIs
            evaluated_kpi_ids = [kpi["kpi_id"] for kpi in evaluation_result["kpi_details"]]
            missing_kpis = [kpi_id for kpi_id in kpi_stats["kpi_ids"] if kpi_id not in evaluated_kpi_ids]
            
            for missing_kpi_id in missing_kpis:
                evaluation_result["kpi_details"].append({
                    "kpi_id": missing_kpi_id,
                    "complete": False,
                    "success": False,
                    "evaluation_date": None,
                    "validation_type": "not_evaluated"
                })
            
            print(f"✅ Criteria {criteria_id} evaluated with status: {status}")
            return evaluation_result
            
        except Exception as e:
            print(f"❌ Error evaluating criteria {criteria_id}: {e}")
            return {"error": str(e), "status": 500}
    
    async def save_criteria_data(self, criteria_id: str, company_id: str, site_code: str, 
                                evaluation_result: Dict[str, Any], approver_ids: List[str] = None,
                                evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Save criteria evaluation results to the criteria_data collection.
        
        Args:
            criteria_id: Criteria identifier
            company_id: Company identifier
            site_code: Site code
            evaluation_result: Results from criteria evaluation
            approver_ids: List of approver user IDs (optional)
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            Dictionary containing save operation result
        """
        try:
            if evaluation_date is None:
                evaluation_date = datetime.now(timezone.utc)
            
            now = datetime.now(timezone.utc)
            
            # Extract data from evaluation result
            kpi_summary = evaluation_result.get("kpi_summary", {})
            status = evaluation_result.get("status", "pending")
            kpi_details = evaluation_result.get("kpi_details", [])
            
            # Prepare the document to save
            criteria_data_doc = {
                "criteria_id": criteria_id,
                "company_id": str(company_id),
                "site_code": site_code,
                "status": status,
                "approval_ready": status == "achieved",  # Only ready for approval if achieved
                "approver_ids": approver_ids or [],
                "evaluation_date": evaluation_date,
                "last_kpi_update": now,
                "kpi_summary": {
                    "total_kpis": kpi_summary.get("total_kpis", 0),
                    "submitted_kpis": kpi_summary.get("submitted_kpis", 0),
                    "incomplete_kpis": kpi_summary.get("incomplete_kpis", 0),
                    "success_kpis": kpi_summary.get("success_kpis", 0),
                    "failed_kpis": kpi_summary.get("failed_kpis", 0),
                    "completion_percentage": kpi_summary.get("completion_percentage", 0),
                    "success_percentage": kpi_summary.get("success_percentage", 0)
                },
                "kpi_details": kpi_details,
                
                "messages": {
                    "summary": f"Criteria evaluation completed. Status: {status}. {kpi_summary.get('success_kpis', 0)}/{kpi_summary.get('total_kpis', 0)} KPIs successful.",
                    "details": {
                        "evaluation_result": evaluation_result,
                        "completion_percentage": kpi_summary.get("completion_percentage", 0),
                        "success_percentage": kpi_summary.get("success_percentage", 0)
                    }
                },
                "updated_at": now
            }
            
            # Define the filter for upsert operation
            filter_criteria = {
                "criteria_id": criteria_id,
                "company_id": str(company_id),
                "site_code": site_code
            }
            
            # Check if document exists
            existing_doc = await self.db.criteria_data.find_one(filter_criteria)
            
            if existing_doc:
                # Update existing document
                result = await self.db.criteria_data.update_one(
                    filter_criteria,
                    {"$set": criteria_data_doc}
                )
                
                operation_result = {
                    "success": True,
                    "operation": "update",
                    "criteria_id": criteria_id,
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "status": status
                }
                
                print(f"📝 Updated existing criteria data for {criteria_id}. Status: {status}")
                
            else:
                # Create new document
                criteria_data_doc["created_at"] = now
                
                result = await self.db.criteria_data.insert_one(criteria_data_doc)
                
                operation_result = {
                    "success": True,
                    "operation": "insert",
                    "criteria_id": criteria_id,
                    "inserted_id": str(result.inserted_id),
                    "status": status
                }
                
                print(f"📝 Created new criteria data for {criteria_id}. Status: {status}")
            
            return operation_result
            
        except Exception as e:
            print(f"❌ Error saving criteria data: {e}")
            return {"success": False, "error": str(e)}


    ############################################################################
                            # Batch Processing Functions
    ############################################################################
    
    async def process_all_criteria_for_company_site(self, company_id: str, site_code: str, 
                                                   approver_ids: List[str] = None,
                                                   evaluation_date: datetime = None) -> List[Dict[str, Any]]:
        """
        Process all active criteria for a specific company and site.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            approver_ids: List of approver user IDs (optional)
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            List of processed criteria results
        """
        try:
            if evaluation_date is None:
                evaluation_date = datetime.now(timezone.utc)
            
            print(f"🔄 Processing all criteria for company {company_id}, site {site_code}")
            
            # Get all active criteria
            active_criteria = await self.db.criterias.find({"is_active": True }).to_list(None)
            
            if not active_criteria:
                print("⚠️  No active criteria found")
                return []
            
            print(f"📋 Found {len(active_criteria)} active criteria to process")
            
            results = []
            
            for criteria in active_criteria:
                criteria_id = criteria["criteria_id"]
                
                try:
                    result = await self.evaluate_and_save_criteria(
                        criteria_id=criteria_id,
                        company_id=company_id,
                        site_code=site_code,
                        approver_ids=approver_ids,
                        evaluation_date=evaluation_date
                    )
                    
                    results.append({
                        "criteria_id": criteria_id,
                        "result": result
                    })
                    
                except Exception as e:
                    print(f"❌ Error processing criteria {criteria_id}: {str(e)}")
                    results.append({
                        "criteria_id": criteria_id,
                        "error": str(e)
                    })
                    continue
            
            successful_results = [r for r in results if "error" not in r]
            failed_results = [r for r in results if "error" in r]
            
            print(f"✅ Successfully processed {len(successful_results)} criteria")
            if failed_results:
                print(f"❌ Failed to process {len(failed_results)} criteria")
            
            return results
            
        except Exception as e:
            print(f"❌ Error processing criteria for company {company_id}, site {site_code}: {str(e)}")
            return [{"error": str(e)}]
    
    async def get_criteria_summary(self, company_id: str, site_code: str, 
                                  evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Get summary of criteria status for a company and site.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            evaluation_date: Date for evaluation (defaults to current date)
            
        Returns:
            Dictionary containing criteria summary
        """
        try:
            if evaluation_date is None:
                evaluation_date = datetime.now(timezone.utc)
            
            # Get criteria data
            criteria_data = await self.db.criteria_data.find({
                "company_id": str(company_id),
                "site_code": site_code
            }).to_list(None)
            
            if not criteria_data:
                return {
                    "company_id": company_id,
                    "site_code": site_code,
                    "total_criteria": 0,
                    "achieved": 0,
                    "not_met": 0,
                    "pending": 0,
                    "completion_percentage": 0,
                    "success_percentage": 0
                }
            
            # Calculate summary statistics
            total_criteria = len(criteria_data)
            achieved_count = sum(1 for c in criteria_data if c["status"] == "achieved")
            not_met_count = sum(1 for c in criteria_data if c["status"] == "not_met")
            pending_count = sum(1 for c in criteria_data if c["status"] == "pending")
            
            completion_percentage = (achieved_count / total_criteria * 100) if total_criteria > 0 else 0
            success_percentage = (achieved_count / total_criteria * 100) if total_criteria > 0 else 0
            
            print(f"📊 Criteria Summary for {company_id}/{site_code}:")
            print(f"  Total: {total_criteria}, Achieved: {achieved_count}, Not Met: {not_met_count}, Pending: {pending_count}")
            print(f"  Success Rate: {success_percentage:.1f}%")
            
            return {
                "company_id": company_id,
                "site_code": site_code,
                "evaluation_date": evaluation_date.isoformat(),
                "total_criteria": total_criteria,
                "achieved": achieved_count,
                "not_met": not_met_count,
                "pending": pending_count,
                "completion_percentage": completion_percentage,
                "success_percentage": success_percentage,
                "criteria_details": [
                    {
                        "criteria_id": c["criteria_id"],
                        "status": c["status"],
                        "kpi_completion": f"{c.get('kpi_completion_count', 0)}/{c.get('kpi_total_count', 0)}",
                        "approval_ready": c.get("approval_ready", False)
                    }
                    for c in criteria_data
                ]
            }
            
        except Exception as e:
            print(f"❌ Error getting criteria summary: {str(e)}")
            return {"error": str(e)}


async def main():
    """Main function to run the script from command line."""
    # Load environment variables
    MONGODB_URI = os.getenv("MONGO_URI")
    DATABASE_NAME = os.getenv("MONGO_DB_NAME", "ensogove")

    if not MONGODB_URI:
        print("❌ Missing MONGO_URI environment variable")
        sys.exit(1)

    try:
        # Initialize Criteria Evaluator
        evaluator = CriteriaEvaluator(
            mongo_uri=MONGODB_URI,
            db_name=DATABASE_NAME
        )

        async with evaluator:
            # Example: Process all criteria for a company and site
            company_id = "555"
            site_code = "MM-0012"
            evaluation_date = datetime.now(timezone.utc)
            approver_ids = ["user1", "user2"]
            
            print(f"🎯 Starting criteria evaluation for company {company_id}, site {site_code}")
            
            # Process all criteria
            # results = await evaluator.process_all_criteria_for_company_site(
            #     company_id=company_id,
            #     site_code=site_code,
            #     approver_ids=approver_ids,
            #     evaluation_date=evaluation_date
            # )
            
            # print(f"\n📊 Batch Processing Results:")
            # for result in results:
            #     if "error" in result:
            #         print(f"❌ {result.get('criteria_id', 'Unknown')}: {result['error']}")
            #     else:
            #         evaluation_result = result.get("result", {}).get("evaluation_result", {})
            #         status = evaluation_result.get("status", "unknown")
            #         print(f"✅ {result['criteria_id']}: {status}")
            
            # # Get summary
            # summary = await evaluator.get_criteria_summary(company_id, site_code, evaluation_date)
            
            # print(f"\n📋 Criteria Summary:")
            # print(json.dumps(summary, indent=2, default=str))
            
            # Example: Process a single criteria
            print(f"\n🎯 Processing single criteria example:")
            single_result = await evaluator.evaluate_and_save_criteria(
                criteria_id="CRITERIA-BUSINESS-PROFILE-001",  # Replace with actual criteria ID
                company_id=company_id,
                site_code=site_code,
                approver_ids=approver_ids,
                evaluation_date=evaluation_date
            )
            
            print(f"Single criteria result:")
            print(json.dumps(single_result, indent=2, default=str))

    except DBError as e:
        e.log_db_error()
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())