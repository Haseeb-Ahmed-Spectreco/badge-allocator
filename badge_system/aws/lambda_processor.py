#!/usr/bin/env python3
"""
Optimized AWS Lambda Function for Processing Badge Re-evaluation from Queue

This Lambda function processes badge re-evaluation requests from badge_evaluation_queue
with site expansion and efficient batch processing.
"""

import json
import os
import sys
import asyncio
import time
import aiohttp
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


class LambdaBadgeProcessor:
    """
    Optimized Lambda Badge Processor for efficient badge re-evaluation processing.
    """
    
    def __init__(self, shared_db_client: Optional[AsyncIOMotorClient] = None, 
                 shared_db = None):
        """Initialize the processor with configuration."""
        # Load environment variables
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("MONGO_DB_NAME", "ensogove")
        self.base_url = os.getenv("BASE_URL")
        self.region_url = os.getenv("REGION_API_URL")
        self.token = os.getenv("TOKEN")
        
        # Lambda-specific configuration
        self.max_concurrent_groups = int(os.getenv("MAX_CONCURRENT_GROUPS", "3"))
        self.max_badges_per_group = int(os.getenv("MAX_BADGES_PER_GROUP", "10"))
        self.timeout_per_badge = int(os.getenv("TIMEOUT_PER_BADGE", "30"))
        
        # Validate required environment variables
        if not all([self.mongo_uri, self.base_url, self.token]):
            raise ValueError("Missing required environment variables: MONGO_URI, BASE_URL, TOKEN")
        
        # Load API configuration for site expansion
        self._load_api_config()
        
        # Initialize database connections
        if shared_db_client and shared_db:
            self.client = shared_db_client
            self.db = shared_db
            self._owns_connection = False
            print("✅ Using shared MongoDB connection")
        else:
            self.client = None
            self.db = None
            self._owns_connection = True
        
        # Initialize components
        self.http_client = None
        self.kpi_validator = None
        self.criteria_evaluator = None
        self.badge_verifier = None
        
        self.queue_collection = "badge_evaluation_queue"
    
    def _load_api_config(self):
        """Load and validate API configuration from environment variables"""
        self.api_config = {
            'base_url': os.getenv('SITES_API_BASE_URL', self.base_url),
            'endpoint': os.getenv('SITES_API_ENDPOINT', '/api/allcompanies/sites'),
            'token': os.getenv('SITES_API_TOKEN', self.token),
            'timeout': int(os.getenv('SITES_API_TIMEOUT', '30')),
            'expand_enabled': os.getenv('EXPAND_ALL_SITES', 'true').lower() == 'true',
            'max_sites': int(os.getenv('MAX_SITES_PER_COMPANY', '100')),
            'fallback_mode': os.getenv('FALLBACK_WHEN_NO_SITES', 'keep_all')
        }
        
        print("🔧 API Configuration:")
        for key, value in self.api_config.items():
            print(f"   {key}: {value}")
    
    async def connect_to_db(self):
        """Establish connection to MongoDB and initialize components."""
        try:
            print("🔌 Connecting to MongoDB...")
            if self._owns_connection:
                self.client = AsyncIOMotorClient(
                    self.mongo_uri,
                    maxPoolSize=10,
                    minPoolSize=1,
                    maxIdleTimeMS=30000,
                    serverSelectionTimeoutMS=5000
                )
                self.db = self.client[self.db_name]
                # Test connection
                await self.db.command("ping")
                print("✅ Connected to MongoDB")
            
            # Initialize HTTP client
            self.http_client = HttpClient(
                base_url=self.base_url,
                region_url=self.region_url,
                token=self.token
            )
            
            # Initialize components with shared database connection
            self.kpi_validator = KpiValidator(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                base_url=self.base_url,
                region_url=self.region_url,
                http_client=self.http_client,
                shared_db_client=self.client,
                shared_db=self.db
            )
            
            self.criteria_evaluator = CriteriaEvaluator(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                shared_db_client=self.client,
                shared_db=self.db
            )
            
            self.badge_verifier = BadgeVerifier(
                mongo_uri=self.mongo_uri,
                db_name=self.db_name,
                shared_db_client=self.client,
                shared_db=self.db
            )
            
            print("✅ All components initialized")
            
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            raise
    
    def disconnect_from_db(self):
        """Close MongoDB connection and cleanup components."""
        try:
            if self._owns_connection and self.client:
                self.client.close()
                self.client = None
                self.db = None
            print("🔌 Disconnected from MongoDB")
        except Exception as e:
            print(f"❌ Error disconnecting: {e}")
    
    async def fetch_company_sites_api(self, company_id: str) -> List[str]:
        """Fetch all site codes for a company via API"""
        try:
            if not self.api_config['expand_enabled']:
                return []
            
            url = f"{self.api_config['base_url']}{self.api_config['endpoint']}"
            params = {'company_id': company_id}
            headers = {
                "Authorization": f"Bearer {self.api_config['token']}",
                "Content-Type": "application/json"
            }
            
            print(f"🌐 Fetching sites for company {company_id}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, 
                                     timeout=self.api_config['timeout']) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Parse response - handle multiple formats
                        sites = []
                        if isinstance(data, list):
                            sites = [item.get('site_code') for item in data if item.get('site_code')]
                        elif isinstance(data, dict):
                            if 'sites' in data:
                                sites = [site.get('site_code') for site in data['sites'] if site.get('site_code')]
                            elif 'data' in data:
                                sites = [site.get('site_code') for site in data['data'] if site.get('site_code')]
                            elif 'result' in data:
                                if isinstance(data['result'], list):
                                    sites = [site.get('site_code') for site in data['result'] if site.get('site_code')]
                        
                        # Clean and validate sites
                        sites = [str(site).strip() for site in sites if site]
                        
                        # Apply max sites limit
                        if len(sites) > self.api_config['max_sites']:
                            sites = sites[:self.api_config['max_sites']]
                        
                        print(f"✅ Found {len(sites)} sites for company {company_id}")
                        return sites
                        
                    else:
                        print(f"⚠️  API returned {response.status} for company {company_id}")
                        return []
                        
        except Exception as e:
            print(f"❌ Error fetching sites for company {company_id}: {e}")
            return []
    
    async def fetch_pending_evaluations(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch and expand pending evaluations from the queue"""
        try:
            print("🔍 Fetching pending badge evaluations...")
            
            # Fetch pending evaluations
            query = {"status": "pending"}
            cursor = self.db[self.queue_collection].find(query)
            
            if limit:
                cursor = cursor.limit(limit)
            
            raw_evaluations = await cursor.to_list(length=None)
            
            if not raw_evaluations:
                print("📋 No pending evaluations found")
                return []
            
            print(f"📋 Found {len(raw_evaluations)} pending evaluations")
            
            if not self.api_config['expand_enabled']:
                return raw_evaluations
            
            # Expand evaluations with site_code="all" or None
            final_evaluations = []
            
            for evaluation in raw_evaluations:
                site_code = evaluation.get('site_code')
                company_id = evaluation.get('company_id')
                
                if site_code == "all" or site_code is None:
                    print(f"🔄 Expanding sites for company {company_id}")
                    
                    try:
                        company_sites = await self.fetch_company_sites_api(company_id)
                        
                        if company_sites:
                            # Create evaluation for each specific site
                            for site in company_sites:
                                expanded_evaluation = evaluation.copy()
                                expanded_evaluation['site_code'] = site
                                
                                # Add expansion metadata
                                expanded_evaluation['metadata'] = expanded_evaluation.get('metadata', {})
                                expanded_evaluation['metadata'].update({
                                    'expanded_from_all_sites': True,
                                    'original_site_code': site_code,
                                    'expansion_timestamp': time.time(),
                                    'expansion_type': 'specific_site'
                                })
                                
                                # Remove original _id to avoid conflicts
                                if '_id' in expanded_evaluation:
                                    del expanded_evaluation['_id']
                                
                                final_evaluations.append(expanded_evaluation)
                            
                            # Add default evaluation with empty site_code
                            default_evaluation = evaluation.copy()
                            default_evaluation['site_code'] = ""
                            default_evaluation['metadata'] = default_evaluation.get('metadata', {})
                            default_evaluation['metadata'].update({
                                'expanded_from_all_sites': True,
                                'original_site_code': site_code,
                                'expansion_timestamp': time.time(),
                                'expansion_type': 'default_empty_site'
                            })
                            
                            if '_id' in default_evaluation:
                                del default_evaluation['_id']
                            
                            final_evaluations.append(default_evaluation)
                            
                            print(f"✅ Expanded into {len(company_sites)} specific + 1 default evaluation")
                        else:
                            # No sites found, keep as 'all' based on fallback mode
                            if self.api_config['fallback_mode'] != 'skip':
                                evaluation['site_code'] = 'all'
                                final_evaluations.append(evaluation)
                                
                    except Exception as e:
                        print(f"❌ Error expanding evaluation for company {company_id}: {e}")
                        # Fallback: keep original
                        evaluation['site_code'] = 'all'
                        final_evaluations.append(evaluation)
                        
                else:
                    # Specific site code, keep as-is
                    final_evaluations.append(evaluation)
            
            print(f"✅ Final result: {len(final_evaluations)} evaluations ready")
            return final_evaluations
            
        except Exception as e:
            print(f"❌ Error fetching pending evaluations: {e}")
            return []
    
    async def get_badges_for_reevaluation(self, affected_collections: List[str]) -> List[Dict[str, Any]]:
        """Find badges that need re-evaluation based on affected collections"""
        try:
            print(f"🔍 Finding badges for collections: {affected_collections}")
            
            # Find KPIs with affected collections
            kpi_cursor = self.db.kpis.find({
                "evaluation_method.collections.collection": {"$in": affected_collections}
            })
            affected_kpis = await kpi_cursor.to_list(length=None)
            affected_kpi_ids = [kpi["_id"] for kpi in affected_kpis]
            
            if not affected_kpi_ids:
                return []
            
            # Find criteria that use these KPIs
            criteria_cursor = self.db.criterias.find({
                "kpi_ids": {"$in": affected_kpi_ids}
            })
            affected_criteria = await criteria_cursor.to_list(length=None)
            affected_criteria_ids = [criteria["_id"] for criteria in affected_criteria]
            
            if not affected_criteria_ids:
                return []
            
            # Find badges that use these criteria
            badges_cursor = self.db.badges.find({
                "criteria_id": {"$in": affected_criteria_ids}
            })
            badges = await badges_cursor.to_list(length=None)
            
            print(f"🏆 Found {len(badges)} badges for re-evaluation")
            return badges
            
        except Exception as e:
            print(f"❌ Error finding badges: {e}")
            return []
    
    async def process_single_badge_evaluation(self, company_id: str, site_code: str, 
                                            badge_id: str) -> Dict[str, Any]:
        """Process a single badge evaluation"""
        try:
            print(f"🔄 Processing badge {badge_id} for {company_id}/{site_code}")
            
            # Get badge info
            badge_info = await self.badge_verifier.get_badge_info(badge_id)
            if not badge_info["found"]:
                return {"success": False, "error": "Badge not found", "eligible": False}
            
            criteria_id = badge_info["criteria_id"]
            
            # Get criteria and KPIs
            criteria_doc = await self.db.criterias.find_one({"_id": criteria_id})
            if not criteria_doc:
                return {"success": False, "error": "Criteria not found", "eligible": False}
            
            kpi_ids = criteria_doc.get("kpi_ids", [])
            if not kpi_ids:
                return {"success": False, "error": "No KPIs found", "eligible": False}
            
            # Validate KPIs
            kpi_results = []
            for kpi_id in kpi_ids:
                try:
                    kpi_result = await asyncio.wait_for(
                        self.kpi_validator.validate_and_save_kpi(
                            company_id=company_id,
                            site_code=site_code,
                            kpi_id=kpi_id
                        ),
                        timeout=self.timeout_per_badge
                    )
                    kpi_results.append({"kpi_id": kpi_id, "result": kpi_result})
                    
                except asyncio.TimeoutError:
                    kpi_results.append({"kpi_id": kpi_id, "error": "Timeout"})
                    
                except Exception as e:
                    kpi_results.append({"kpi_id": kpi_id, "error": str(e)})
            
            # Evaluate criteria
            try:
                criteria_result = await self.criteria_evaluator.evaluate_and_save_criteria(
                    criteria_id=criteria_id,
                    company_id=company_id,
                    site_code=site_code
                )
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Criteria evaluation failed: {str(e)}",
                    "eligible": False,
                    "kpi_results": kpi_results
                }
            
            # Verify badge
            try:
                badge_result = await self.badge_verifier.verify_and_save_badge(
                    badge_id=badge_id,
                    company_id=company_id,
                    site_code=site_code
                )
                
                verification_result = badge_result.get("verification_result", {})
                eligible = verification_result.get("eligible", False)
                
                return {
                    "success": True,
                    "eligible": eligible,
                    "badge_id": badge_id,
                    "badge_result": badge_result,
                    "criteria_result": criteria_result,
                    "kpi_results": kpi_results
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Badge verification failed: {str(e)}",
                    "eligible": False,
                    "criteria_result": criteria_result,
                    "kpi_results": kpi_results
                }
            
        except Exception as e:
            return {"success": False, "error": str(e), "eligible": False}
    
    def _is_badge_criteria_met(self, evaluation_result) -> bool:
        """Check if badge criteria is met based on evaluation result"""
        if evaluation_result is None:
            return False
        
        if isinstance(evaluation_result, dict):
            if 'success' in evaluation_result:
                return evaluation_result['success'] and evaluation_result.get('eligible', False)
            elif 'eligible' in evaluation_result:
                return evaluation_result['eligible']
            elif 'status' in evaluation_result:
                return evaluation_result['status'] in ['success', 'passed', 'completed']
            return True
        
        return evaluation_result is not None
    
    async def process_company_site_badges(self, group: Dict, badges: List[Dict]) -> Dict:
        """Process all badges for a company/site pair"""
        company_id = group['company_id']
        site_code = group['site_code']
        start_time = time.time()
        
        try:
            print(f"🏆 Processing {len(badges)} badges for {company_id}/{site_code}")
            
            # Sort badges by level_rank
            sorted_badges = sorted(badges, key=lambda b: b.get('level_rank', 0))
            
            # Limit badges for Lambda timeout management
            if len(sorted_badges) > self.max_badges_per_group:
                sorted_badges = sorted_badges[:self.max_badges_per_group]
            
            badges_evaluated = 0
            failed_badge = None
            
            # Process badges sequentially, stop on first failure
            for badge in sorted_badges:
                badge_id = str(badge.get('_id'))
                badge_name = badge.get('name', 'Unknown')
                
                print(f"  🔍 Evaluating: {badge_name}")
                
                try:
                    result = await self.process_single_badge_evaluation(
                        company_id=company_id,
                        site_code=site_code,
                        badge_id=badge_id
                    )
                    
                    badges_evaluated += 1
                    
                    if not self._is_badge_criteria_met(result):
                        failed_badge = badge_name
                        print(f"  ❌ Badge criteria not met for {badge_name}")
                        break
                    else:
                        print(f"  ✅ Badge criteria met for {badge_name}")
                        
                except Exception as badge_error:
                    failed_badge = badge_name
                    print(f"  ❌ Error evaluating {badge_name}: {badge_error}")
                    break
            
            duration = time.time() - start_time
            success = failed_badge is None
            
            return {
                'success': success,
                'company_id': company_id,
                'site_code': site_code,
                'badges_evaluated': badges_evaluated,
                'total_badges_available': len(badges),
                'failed_badge': failed_badge,
                'duration': duration,
                'evaluation_ids': group['evaluation_ids'],
                'affected_collections': group['affected_collections']
            }
            
        except Exception as e:
            duration = time.time() - start_time
            print(f"  ❌ Exception processing {company_id}/{site_code}: {e}")
            
            return {
                'success': False,
                'company_id': company_id,
                'site_code': site_code,
                'badges_evaluated': 0,
                'total_badges_available': len(badges),
                'error': str(e),
                'duration': duration,
                'evaluation_ids': group['evaluation_ids'],
                'affected_collections': group['affected_collections']
            }
    
    async def mark_evaluations_completed(self, evaluation_ids: List[str]):
        """Mark evaluations as completed in the queue"""
        try:
            result = await self.db[self.queue_collection].update_many(
                {"_id": {"$in": evaluation_ids}},
                {"$set": {"status": "completed", "completed_at": time.time()}}
            )
            print(f"📝 Marked {result.modified_count} evaluations as completed")
        except Exception as e:
            print(f"⚠️  Error marking evaluations completed: {e}")
    
    async def process_badge_reevaluation_queue(self, context=None) -> Dict[str, Any]:
        """Main method to process badge re-evaluation queue"""
        start_time = time.time()
        
        try:
            print("🎯 Starting badge re-evaluation from queue...")
            
            # Calculate batch size based on remaining Lambda time
            if context:
                remaining_time = context.get_remaining_time_in_millis()
                max_groups = min(self.max_concurrent_groups, max(1, (remaining_time - 60000) // 30000))
            else:
                max_groups = self.max_concurrent_groups
            
            # Fetch and expand pending evaluations
            pending_evaluations = await self.fetch_pending_evaluations(limit=max_groups * 10)
            
            if not pending_evaluations:
                return {
                    "success": True,
                    "message": "No pending evaluations found",
                    "processed_groups": 0,
                    "execution_time": time.time() - start_time
                }
            
            # Group by company_id and site_code
            company_site_groups = {}
            for evaluation in pending_evaluations:
                company_id = evaluation.get('company_id')
                site_code = evaluation.get('site_code')
                affected_collections = evaluation.get('affected_collections', [])
                
                key = f"{company_id}_{site_code}"
                if key not in company_site_groups:
                    company_site_groups[key] = {
                        'company_id': company_id,
                        'site_code': site_code,
                        'affected_collections': set(),
                        'evaluation_ids': []
                    }
                
                company_site_groups[key]['affected_collections'].update(affected_collections)
                company_site_groups[key]['evaluation_ids'].append(evaluation.get('_id'))
            
            # Convert sets to lists and limit groups
            groups_to_process = []
            for group in list(company_site_groups.values())[:max_groups]:
                group['affected_collections'] = list(group['affected_collections'])
                groups_to_process.append(group)
            
            print(f"🎯 Processing {len(groups_to_process)} company/site groups")
            
            # Process groups sequentially for Lambda reliability
            results = []
            for group in groups_to_process:
                try:
                    # Get badges for this group
                    badges = await self.get_badges_for_reevaluation(group['affected_collections'])
                    
                    if not badges:
                        result = {
                            'success': True,
                            'company_id': group['company_id'],
                            'site_code': group['site_code'],
                            'badges_evaluated': 0,
                            'message': 'No badges found for re-evaluation'
                        }
                    else:
                        result = await self.process_company_site_badges(group, badges)
                        
                        # Mark as completed if successful
                        if result.get('success'):
                            await self.mark_evaluations_completed(group['evaluation_ids'])
                    
                    results.append(result)
                    
                except Exception as e:
                    print(f"❌ Error processing group {group['company_id']}/{group['site_code']}: {e}")
                    results.append({
                        'success': False,
                        'company_id': group['company_id'],
                        'site_code': group['site_code'],
                        'error': str(e)
                    })
            
            # Calculate final metrics
            execution_time = time.time() - start_time
            successful = sum(1 for r in results if r.get('success'))
            failed = len(results) - successful
            total_badges_evaluated = sum(r.get('badges_evaluated', 0) for r in results)
            
            return {
                "success": failed == 0,
                "processed_groups": len(groups_to_process),
                "total_pending_evaluations": len(pending_evaluations),
                "successful_groups": successful,
                "failed_groups": failed,
                "total_badges_evaluated": total_badges_evaluated,
                "execution_time": execution_time,
                "results": results
            }
            
        except Exception as e:
            print(f"❌ Error in badge re-evaluation process: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time": time.time() - start_time
            }


# Global processor instance (reused across Lambda invocations)
processor = None

def lambda_function(event, context):
    """
    AWS Lambda handler for processing badge re-evaluation from queue.
    Fixed function name and streamlined processing.
    """
    global processor
    
    try:
        print(f"🚀 Lambda started with {context.get_remaining_time_in_millis()}ms remaining")
        
        # Initialize processor if needed
        if processor is None:
            
            print("🔌 Initializing LambdaBadgeProcessor...")

            processor = LambdaBadgeProcessor()
            
            print("🔌 Initialized LambdaBadgeProcessor...")
        
        # Process the queue directly (no SQS needed)
        return process_badge_queue(event, context)
    
    except Exception as e:
        print(f"❌ Unexpected error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def process_badge_queue(event, context):
    """Process badge re-evaluation queue efficiently"""
    global processor
    
    print("🔄 Processing badge re-evaluation queue...")
    
    # Process in async context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(process_queue_async(context))
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Badge re-evaluation completed",
                "result": result
            }, default=str)
        }
        
    finally:
        loop.close()

async def process_queue_async(context):
    """Process badge re-evaluation queue asynchronously"""
    global processor
    print("🔄 Starting async badge re-evaluation processing...")
    
    
    # Connect to database
    await processor.connect_to_db()
    
    try:
        # Process the queue with context for timeout management
        result = await processor.process_badge_reevaluation_queue(context=context)
        return result
        
    finally:
        # Clean up connections
        processor.disconnect_from_db()