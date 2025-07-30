#!/usr/bin/env python3
"""
Badge Verification Script with Approval Support

A standalone Python script to verify badge eligibility based on criteria achievement
and manage badge data for companies and sites.
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
import traceback
import asyncio
from badge_system.exceptions import DBError

load_dotenv()


class BadgeVerifier:
    """
    Badge Verifier class to verify badge eligibility based on criteria achievement
    and manage badge data in the badge_data collection.
    """
    
    def __init__(self, mongo_uri: str, db_name: str, shared_db_client: Optional[AsyncIOMotorClient] = None, shared_db = None):
        """
        Initialize the Badge Verifier with database configuration.
        
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
                # Core Badge Verification Functions
    ############################################################################
    
    async def get_badge_info(self, badge_id: str) -> Dict[str, Any]:
        """
        Get badge information from the badges collection.
        
        Args:
            badge_id: Badge identifier
            
        Returns:
            Dictionary containing badge information
        """
        try:
            badge_doc = await self.db.badges.find_one({"badge_id": badge_id})
            
            if not badge_doc:
                raise DBError(
                    status=404,
                    reason="Badge not found",
                    collection="badges",
                    message=f"Badge {badge_id} not found in database"
                )
             
            return {
                "found": True,
                "badge_id": badge_doc["badge_id"],
                "criteria_id": badge_doc["criteria_id"],
                "name": badge_doc["name"],
                "description": badge_doc.get("description", ""),
                "level": badge_doc["level"],
                "image": badge_doc.get("image", ""),
                "region": badge_doc.get("region", "global")
            }
        except DBError as e:
            raise e
            
        except Exception as e:
            print(f"❌ Error getting badge info for {badge_id}: {str(e)}")
            return {
                "found": False,
                "error": str(e)
            }
    
    async def is_badge_awardable(self, criteria_data: Dict[str, Any]) -> bool:
        """
        Check if a badge is awardable based on approval status from all required approvers.
        
        Args:
            criteria_data: The criteria_data document from MongoDB
            
        Returns:
            Boolean indicating if badge is awardable
        """
        try:
            criteria_data_id = str(criteria_data["_id"])
            approver_ids = criteria_data.get("approver_ids", [])
            
            print(f"🔍 Checking badge awardability for criteria_data_id: {criteria_data_id}")
            print(f"📋 Required approvers: {approver_ids}")

            if not approver_ids:
                print("ℹ️  No approvers required - badge is awardable")
                return True

            # Get all approval data for this criteria_data_id with approved status
            approved_data = await self.db.approval_data.find({
                "criteria_data_id": criteria_data_id,
                "approver_id": {"$in": approver_ids},
                "approval_status": "approved"
            }).to_list(None)
            
            approved_count = len(approved_data)
            required_count = len(approver_ids)
            
            print(f"📊 Approval progress: {approved_count}/{required_count}")
            
            if approved_count == required_count:
                print("✅ All required approvers have approved")
                return True
            else:
                print(f"⏳ Missing {required_count - approved_count} approvals")
                return False

        except Exception as e:
            print(f"❌ Error checking badge awardability: {str(e)}")
            return False
        
    def get_badge_status(self, status: str, awardable: bool) -> str:
        """
        Get the status of a badge based on criteria data.
        
        Args:
            status: Criteria status
            awardable: Whether badge is awardable
            
        Returns:
            Status string ("achieved", "pending", "not_evaluated", etc.)
        """
        if status == "achieved" and not awardable:
            return "pending_approval"
        elif status == "achieved" and awardable:
            return "awarded"
        elif status == "pending":
            return "pending_evaluation"
        elif status == "not_met":
            return "failed_criteria"
        else:
            return "not_evaluated"
        
    async def get_criteria_achievement_status(self, criteria_id: str, company_id: str, site_code: str) -> Dict[str, Any]:
        """
        Get the criteria achievement status for a company and site.
        
        Args:
            criteria_id: Criteria identifier
            company_id: Company identifier
            site_code: Site code
            
        Returns:
            Dictionary containing criteria achievement status
        """
        try:
            criteria_data = await self.db.criteria_data.find_one({
                "criteria_id": criteria_id,
                "company_id": str(company_id),
                "site_code": site_code
            })
            
            if not criteria_data:
                return {
                    "found": False,
                    "achieved": False,
                    "awarded": False,
                    "status": "not_evaluated",
                    "error": f"Criteria {criteria_id} not found for company {company_id}, site {site_code}"
                }
            
            criteria_status = criteria_data.get("status", "pending")
            criteria_achieved = criteria_status == "achieved"
            awardable = await self.is_badge_awardable(criteria_data)
            badge_status = self.get_badge_status(status=criteria_status, awardable=awardable)
            
            return {
                "found": True,
                "achieved": criteria_achieved,
                "awarded": criteria_achieved and awardable,
                "status": badge_status,
                "awardable": awardable,
                "evaluation_date": criteria_data.get("evaluation_date"),
                "kpi_summary": criteria_data.get("kpi_summary", {}),                
                "criteria_data_id": str(criteria_data["_id"])
            }
            
        except Exception as e:
            print(f"❌ Error getting criteria achievement status: {str(e)}")
            return {
                "found": False,
                "achieved": False,
                "awarded": False,
                "status": "error",
                "error": str(e)
            }
    
    async def verify_badge_eligibility(self, badge_id: str, company_id: str, site_code: str) -> Dict[str, Any]:
        """
        Verify if a company and site is eligible for a specific badge.
        
        Args:
            badge_id: Badge identifier
            company_id: Company identifier
            site_code: Site code
            
        Returns:
            Dictionary containing badge eligibility verification results
        """
        try:
            print(f"🏆 Verifying badge eligibility: {badge_id} for company {company_id}, site {site_code}")
            
            # Get badge information
            badge_info = await self.get_badge_info(badge_id)
            
            if not badge_info["found"]:
                raise DBError(
                    status=404,
                    reason="Badge not found",
                    collection="badges",
                    message=f"Error getting Badge {badge_id}: {badge_info['error']} "
                )
            
            # Get criteria achievement status
            criteria_status = await self.get_criteria_achievement_status(
                badge_info["criteria_id"], 
                company_id, 
                site_code
            )
            
            if not criteria_status["found"]:
                return {
                    "success": True,
                    "eligible": False,
                    "badge_info": badge_info,
                    "criteria_status": criteria_status,
                    "reason": "Criteria not evaluated or not found"
                }
            
            # Determine eligibility (now includes approval checking)
            eligible = criteria_status["awarded"]
            
            verification_result = {
                "success": True,
                "eligible": eligible,
                "badge_info": badge_info,
                "criteria_status": criteria_status,
                "verification_date": datetime.now(timezone.utc).isoformat(),
                "reason": "Criteria achieved and approved" if eligible else f"Criteria status: {criteria_status['status']}"
            }
            
            status_emoji = "✅" if eligible else "❌"
            print(f"{status_emoji} Badge {badge_id} eligibility: {eligible}")
            
            return verification_result
            
        except DBError as e:
            raise e
        except Exception as e:
            print(f"❌ Error verifying badge eligibility:")
            raise DBError(
                status=500,
                reason="Verification failed",
                collection="badge_data",
                message=f"Error verifying Badge {badge_id}: {str(e)}"
            )

    async def save_badge_data(self, badge_id: str, company_id: str, site_code: str, 
                             verification_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save badge data to the badge_data collection.
        
        Args:
            badge_id: Badge identifier
            company_id: Company identifier
            site_code: Site code
            verification_result: Results from badge verification
            
        Returns:
            Dictionary containing save operation result
        """
        try:
            now = datetime.now(timezone.utc)
            
            # Extract data from verification result
            eligible = verification_result.get("eligible", False)
            criteria_status = verification_result.get("criteria_status", {})
            criteria_data_id = criteria_status.get("criteria_data_id")
            
            # Prepare the document to save
            badge_data_doc = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id,
                "criteria_data_id": criteria_data_id,
                "status": criteria_status.get("status", "pending"),
                "awarded": criteria_status.get("awarded", False),
                "awarded_date": now if eligible else None,
                "display_permission": eligible,  # Default to true if awarded
                "updated_at": now
            }
            
            # Define the filter for upsert operation
            filter_criteria = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id
            }
            
            # Check if document exists
            existing_doc = await self.db.badge_data.find_one(filter_criteria)
            
            if existing_doc:
                # Only update if status changed or if we're awarding a badge
                existing_status = existing_doc.get("status", "pending")
                new_status = badge_data_doc["status"]
                
                if existing_status != new_status or (eligible and not existing_doc.get("awarded", False)):
                    # Update existing document
                    result = await self.db.badge_data.update_one(
                        filter_criteria,
                        {"$set": badge_data_doc}
                    )
                    
                    operation_result = {
                        "success": True,
                        "operation": "update",
                        "badge_id": badge_id,
                        "matched_count": result.matched_count,
                        "modified_count": result.modified_count,
                        "status": new_status,
                        "awarded": eligible
                    }
                    
                    print(f"📝 Updated badge data for {badge_id}. Status: {new_status}")
                else:
                    operation_result = {
                        "success": True,
                        "operation": "no_change",
                        "badge_id": badge_id,
                        "status": existing_status,
                        "awarded": existing_doc.get("awarded", False),
                        "reason": "Status unchanged"
                    }
                    
                    print(f"📝 No change needed for badge {badge_id}. Status: {existing_status}")
                
            else:
                # Create new document
                badge_data_doc["created_at"] = now
                
                result = await self.db.badge_data.insert_one(badge_data_doc)
                
                operation_result = {
                    "success": True,
                    "operation": "insert",
                    "badge_id": badge_id,
                    "inserted_id": str(result.inserted_id),
                    "status": badge_data_doc["status"],
                    "awarded": eligible
                }
                
                print(f"📝 Created new badge data for {badge_id}. Status: {badge_data_doc['status']}")
            
            return operation_result
            
        except Exception as e:
            print(f"❌ Error saving badge data: {e}")
            return {"success": False, "error": str(e)}
    
    async def verify_and_save_badge(self, badge_id: str, company_id: str, site_code: str) -> Dict[str, Any]:
        """
        Verify badge eligibility and save the results to the database.
        
        Args:
            badge_id: Badge identifier
            company_id: Company identifier
            site_code: Site code
            
        Returns:
            Dictionary containing verification and save results
        """
        try:
            print(f"🎯 Verifying and saving badge: {badge_id}")
            
            # Verify badge eligibility
            verification_result = await self.verify_badge_eligibility(badge_id, company_id, site_code)
            
            if not verification_result.get("success", False):
                print(f"⚠️  Badge {badge_id} verification failed: {verification_result.get('error', 'Unknown error')}")
                return {
                    "verification_result": verification_result,
                    "save_result": {"success": False, "error": "Verification failed"}
                }
            
            # Save the results to database
            save_result = await self.save_badge_data(
                badge_id=badge_id,
                company_id=company_id,
                site_code=site_code,
                verification_result=verification_result
            )
            
            return {
                "verification_result": verification_result,
                "save_result": save_result
            }
            
        except Exception as e:
            print(f"❌ Error in verify_and_save_badge: {e}")
            return {"error": str(e), "status": 500}

    # Alias for backward compatibility
    async def verify_and_save_badge_with_approval(self, badge_id: str, company_id: str, site_code: str) -> Dict[str, Any]:
        """
        Alias for verify_and_save_badge (approval checking is now built-in).
        """
        return await self.verify_and_save_badge(badge_id, company_id, site_code)


    ############################################################################
                # Batch Processing Functions
    ############################################################################
    
    async def get_badges_for_criteria(self, criteria_id: str, region: str = "global") -> List[Dict[str, Any]]:
        """
        Get all badges associated with a specific criteria.
        
        Args:
            criteria_id: Criteria identifier
            region: Region filter (defaults to "global")
            
        Returns:
            List of badge documents
        """
        try:
            badges = await self.db.badges.find({
                "criteria_id": criteria_id,
                "region": region
            }).to_list(None)
            
            return badges
            
        except Exception as e:
            print(f"❌ Error getting badges for criteria {criteria_id}: {str(e)}")
            return []
    
    async def process_all_badges_for_company_site(self, company_id: str, site_code: str, 
                                                 region: str = "global") -> List[Dict[str, Any]]:
        """
        Process all badges for a specific company and site.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            region: Region filter (defaults to "global")
            
        Returns:
            List of processed badge results
        """
        try:
            print(f"🔄 Processing all badges for company {company_id}, site {site_code}, region {region}")
            
            # Get all badges for the region
            all_badges = await self.db.badges.find({"region": region}).to_list(None)
            
            if not all_badges:
                print(f"⚠️  No badges found for region {region}")
                return []
            
            print(f"🏆 Found {len(all_badges)} badges to process")
            
            results = []
            
            for badge in all_badges:
                badge_id = badge["badge_id"]
                
                try:
                    result = await self.verify_and_save_badge(
                        badge_id=badge_id,
                        company_id=company_id,
                        site_code=site_code
                    )
                    
                    results.append({
                        "badge_id": badge_id,
                        "result": result
                    })
                    
                except Exception as e:
                    print(f"❌ Error processing badge {badge_id}: {str(e)}")
                    results.append({
                        "badge_id": badge_id,
                        "error": str(e)
                    })
                    continue
            
            successful_results = [r for r in results if "error" not in r]
            failed_results = [r for r in results if "error" in r]
            awarded_badges = [r for r in successful_results 
                            if r.get("result", {}).get("verification_result", {}).get("eligible", False)]
            
            print(f"✅ Successfully processed {len(successful_results)} badges")
            print(f"🏆 Awarded {len(awarded_badges)} badges")
            if failed_results:
                print(f"❌ Failed to process {len(failed_results)} badges")
            
            return results
            
        except Exception as e:
            print(f"❌ Error processing badges for company {company_id}, site {site_code}: {str(e)}")
            return [{"error": str(e)}]
    
    async def get_badge_summary(self, company_id: str, site_code: str, region: str = "global") -> Dict[str, Any]:
        """
        Get summary of badge status for a company and site.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            region: Region filter (defaults to "global")
            
        Returns:
            Dictionary containing badge summary
        """
        try:
            # Get badge data
            badge_data = await self.db.badge_data.find({
                "company_id": str(company_id),
                "site_code": site_code
            }).to_list(None)
            
            # Get all badges for the region to compare
            all_badges = await self.db.badges.find({"region": region}).to_list(None)
            total_badges = len(all_badges)
            
            if not badge_data:
                return {
                    "company_id": company_id,
                    "site_code": site_code,
                    "region": region,
                    "total_badges": total_badges,
                    "awarded": 0,
                    "pending": 0,
                    "pending_approval": 0,
                    "revoked": 0,
                    "completion_percentage": 0,
                    "badge_details": []
                }
            
            # Calculate summary statistics
            awarded_count = sum(1 for b in badge_data if b.get("awarded", False))
            pending_count = sum(1 for b in badge_data if b.get("status") == "pending")
            pending_approval_count = sum(1 for b in badge_data if b.get("status") == "pending_approval")
            revoked_count = sum(1 for b in badge_data if b.get("status") == "revoked")
            
            completion_percentage = (awarded_count / total_badges * 100) if total_badges > 0 else 0
            
            # Get badge details with levels
            badge_details = []
            for badge_data_item in badge_data:
                badge_id = badge_data_item.get("badge_id")
                badge_info = await self.get_badge_info(badge_id)
                
                badge_details.append({
                    "badge_id": badge_id,
                    "name": badge_info.get("name", "Unknown") if badge_info.get("found") else "Unknown",
                    "level": badge_info.get("level", "unknown") if badge_info.get("found") else "unknown",
                    "status": badge_data_item.get("status", "unknown"),
                    "awarded": badge_data_item.get("awarded", False),
                    "awarded_date": badge_data_item.get("awarded_date"),
                    "display_permission": badge_data_item.get("display_permission", False)
                })
            
            print(f"🏆 Badge Summary for {company_id}/{site_code}:")
            print(f"  Total Badges: {total_badges}, Awarded: {awarded_count}, Pending: {pending_count}")
            print(f"  Pending Approval: {pending_approval_count}, Revoked: {revoked_count}")
            print(f"  Completion Rate: {completion_percentage:.1f}%")
            
            return {
                "company_id": company_id,
                "site_code": site_code,
                "region": region,
                "total_badges": total_badges,
                "awarded": awarded_count,
                "pending": pending_count,
                "pending_approval": pending_approval_count,
                "revoked": revoked_count,
                "completion_percentage": completion_percentage,
                "badge_details": badge_details
            }
            
        except Exception as e:
            print(f"❌ Error getting badge summary: {str(e)}")
            return {"error": str(e)}
    
    async def revoke_badge(self, badge_id: str, company_id: str, site_code: str, reason: str = "") -> Dict[str, Any]:
        """
        Revoke a badge for a company and site.
        
        Args:
            badge_id: Badge identifier
            company_id: Company identifier
            site_code: Site code
            reason: Reason for revocation
            
        Returns:
            Dictionary containing revocation result
        """
        try:
            now = datetime.now(timezone.utc)
            
            filter_criteria = {
                "company_id": str(company_id),
                "site_code": site_code,
                "badge_id": badge_id
            }
            
            update_data = {
                "status": "revoked",
                "awarded": False,
                "display_permission": False,
                "revoked_date": now,
                "revoked_reason": reason,
                "updated_at": now
            }
            
            result = await self.db.badge_data.update_one(
                filter_criteria,
                {"$set": update_data}
            )
            
            if result.matched_count == 0:
                return {
                    "success": False,
                    "error": "Badge data not found"
                }
            
            operation_result = {
                "success": True,
                "operation": "revoke",
                "badge_id": badge_id,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "revoked_date": now,
                "reason": reason
            }
            
            print(f"🚫 Revoked badge {badge_id} for company {company_id}, site {site_code}")
            
            return operation_result
            
        except Exception as e:
            print(f"❌ Error revoking badge: {str(e)}")
            return {"success": False, "error": str(e)}


async def main():
    """Simple example function to demonstrate badge verification."""
    
    # Load environment variables
    MONGODB_URI = os.getenv("MONGO_URI")
    DATABASE_NAME = os.getenv("MONGO_DB_NAME", "ensogove")

    if not MONGODB_URI:
        print("❌ Missing MONGO_URI environment variable")
        sys.exit(1)

    # Example data
    company_id = "555"
    site_code = "MM-0012"
    badge_id = "badge_bronze_001"
    region = "global"

    try:
        # Initialize Badge Verifier
        verifier = BadgeVerifier(
            mongo_uri=MONGODB_URI,
            db_name=DATABASE_NAME
        )

        async with verifier:
            print("🚀 Starting Badge Verification Example")
            print(f"Company ID: {company_id}")
            print(f"Site Code: {site_code}")
            print(f"Badge ID: {badge_id}")
            print("-" * 50)

            # Example 1: Verify specific badge
            print("\n1️⃣ Verifying specific badge...")
            
            result = await verifier.verify_and_save_badge(
                badge_id=badge_id,
                company_id=company_id,
                site_code=site_code
            )
            
            print(f"\n📊 Badge Verification Result:")
            verification_result = result.get("verification_result", {})
            save_result = result.get("save_result", {})
            
            # Print key information
            print(f"✅ Verification Success: {verification_result.get('success', False)}")
            print(f"🏆 Badge Eligible: {verification_result.get('eligible', False)}")
            
            criteria_status = verification_result.get("criteria_status", {})
            print(f"📋 Criteria Achieved: {criteria_status.get('achieved', False)}")
            print(f"🎯 Awardable: {criteria_status.get('awardable', False)}")
            print(f"🏷️  Status: {criteria_status.get('status', 'Unknown')}")
            
            print(f"💾 Save Success: {save_result.get('success', False)}")
            print(f"🏅 Badge Awarded: {save_result.get('awarded', False)}")

            # Example 2: Get badge summary
            print("\n2️⃣ Getting badge summary...")
            
            summary = await verifier.get_badge_summary(company_id, site_code, region)
            
            print(f"\n🏆 Badge Summary for Company {company_id}, Site {site_code}:")
            print(f"📊 Total Badges: {summary.get('total_badges', 0)}")
            print(f"🏅 Awarded: {summary.get('awarded', 0)}")
            print(f"⏳ Pending: {summary.get('pending', 0)}")
            print(f"📋 Pending Approval: {summary.get('pending_approval', 0)}")
            print(f"🚫 Revoked: {summary.get('revoked', 0)}")
            print(f"📈 Completion Rate: {summary.get('completion_percentage', 0):.1f}%")

            # Example 3: Process a few badges
            print("\n3️⃣ Processing example badges...")
            
            # Get all badges for the region
            all_badges = await verifier.db.badges.find({"region": region}).to_list(None)
            print(f"📋 Found {len(all_badges)} badges to process")
            
            # Process first 3 badges as examples
            processed_count = 0
            for badge in all_badges[:3]:
                badge_id_to_process = badge["badge_id"]
                
                try:
                    result = await verifier.verify_and_save_badge(
                        badge_id=badge_id_to_process,
                        company_id=company_id,
                        site_code=site_code
                    )
                    
                    verification_result = result.get("verification_result", {})
                    eligible = verification_result.get("eligible", False)
                    status = "✅ Awarded" if eligible else "❌ Not Eligible"
                    
                    print(f"  {status} {badge_id_to_process}")
                    processed_count += 1
                    
                except Exception as e:
                    print(f"  ❌ Error processing {badge_id_to_process}: {str(e)}")
            
            print(f"✅ Processed {processed_count} badges successfully")

            print("\n🎉 Badge verification example completed!")

    except DBError as e:
        e.log_db_error()
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())