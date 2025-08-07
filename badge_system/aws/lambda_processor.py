#!/usr/bin/env python3
"""
Enhanced AWS Lambda Function for Processing Badge Re-evaluation from Queue

This Lambda function processes badge re-evaluation requests from badge_evaluation_queue
with advanced site expansion, API integration, and comprehensive error handling.
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


class EnhancedLambdaBadgeProcessor:
    """
    Enhanced Lambda Badge Processor with site expansion and advanced API integration.
    """
    
    def __init__(self, shared_db_client: Optional[AsyncIOMotorClient] = None, 
                 shared_db = None):
        """Initialize the Enhanced Lambda Badge Processor."""
        # Load basic environment variables
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("MONGO_DB_NAME", "ensogove")
        self.base_url = os.getenv("BASE_URL")
        self.region_url = os.getenv("REGION_API_URL")
        self.token = os.getenv("TOKEN")
        
        # Lambda-specific configuration
        self.max_concurrent_groups = int(os.getenv("MAX_CONCURRENT_GROUPS", "5"))
        self.max_badges_per_group = int(os.getenv("MAX_BADGES_PER_GROUP", "10"))
        self.timeout_per_badge = int(os.getenv("TIMEOUT_PER_BADGE", "30"))
        
        # Load and validate API configuration for site expansion
        self._load_api_config()
        
        # Validate required environment variables
        if not all([self.mongo_uri, self.base_url, self.token]):
            raise ValueError("Missing required environment variables")
        
        # Shared MongoDB connection
        if shared_db_client and shared_db:
            self.client = shared_db_client
            self.db = shared_db
            self._owns_connection = False  # Don't close shared connection
            print("✅ Enhanced Lambda Badge processor using shared MongoDB connection")
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
    
    def _load_api_config(self):
        """Load and validate API configuration from environment variables"""
        self.api_config = {
            'base_url': os.getenv('SITES_API_BASE_URL', os.getenv('BASE_URL', 'http://localhost:8000')),
            'endpoint': os.getenv('SITES_API_ENDPOINT', '/api/allcompanies/sites'),
            'token': os.getenv('SITES_API_TOKEN', os.getenv('TOKEN', '')),
            'timeout': int(os.getenv('SITES_API_TIMEOUT', '30')),
            'expand_enabled': os.getenv('EXPAND_ALL_SITES', 'true').lower() == 'true',
            'max_sites': int(os.getenv('MAX_SITES_PER_COMPANY', '100')),
            'fallback_mode': os.getenv('FALLBACK_WHEN_NO_SITES', 'keep_all')
        }
        
        # Log configuration (without sensitive data)
        print(f"🔧 API Configuration loaded:")
        print(f"   Base URL: {self.api_config['base_url']}")
        print(f"   Endpoint: {self.api_config['endpoint']}")
        print(f"   Token: {'***' + self.api_config['token'][-10:] if self.api_config['token'] else 'NOT_SET'}")
        print(f"   Timeout: {self.api_config['timeout']}s")
        print(f"   Site expansion: {'enabled' if self.api_config['expand_enabled'] else 'disabled'}")
        
        if not self.api_config['token']:
            print("⚠️  WARNING: No API token found. Site expansion may fail.")
            print("   Set TOKEN or SITES_API_TOKEN in your environment variables.")
    
    async def test_api_connection(self, test_company_id: str = "382") -> bool:
        """Test API connection with a known company ID"""
        print(f"🧪 Testing API connection with company ID: {test_company_id}")
        
        if not self.api_config['token']:
            print("❌ Cannot test API - no token configured")
            return False
        
        try:
            sites = await self.fetch_company_sites_api(test_company_id)
            if sites:
                print(f"✅ API test successful - found {len(sites)} sites")
                return True
            else:
                print("⚠️  API test completed but no sites returned")
                return True  # API is working, just no sites for this company
        except Exception as e:
            print(f"❌ API test failed: {e}")
            return False
    
    async def connect_to_db(self):
        """Establish connection to MongoDB and initialize components with shared connection."""
        try:
            # Create single MongoDB connection using motor
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
                print("✅ Enhanced Lambda Badge processor connected to MongoDB")
            
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
            
            print("✅ Enhanced Lambda Badge processor initialized all components with shared connection")
            
        except Exception as e:
            print(f"❌ Enhanced Lambda Badge processor Error connecting to database: {e}")
            raise
    
    def disconnect_from_db(self):
        """Close MongoDB connection and cleanup components."""
        try:
            if self._owns_connection and self.client:
                self.client.close()
                self.client = None
                self.db = None
                
            print("🔌 Enhanced Lambda Badge Processor disconnected from MongoDB")
        except Exception as e:
            print(f"❌ Error disconnecting: {e}")
    
    async def fetch_company_sites_api(self, company_id: str) -> List[str]:
        """Fetch all site codes for a company via API using loaded configuration"""
        try:
            # Check if site expansion is enabled
            if not self.api_config['expand_enabled']:
                print(f"🚫 Site expansion disabled - skipping API call for {company_id}")
                return []
            
            # Build URL with query parameter
            url = f"{self.api_config['base_url']}{self.api_config['endpoint']}"
            params = {'company_id': company_id}
            
            headers = {
                "Authorization": f"Bearer {self.api_config['token']}",
                "Content-Type": "application/json"
            }
            
            print(f"🌐 Fetching sites for company {company_id}")
            print(f"   API URL: {url}?company_id={company_id}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, timeout=self.api_config['timeout']) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Parse response - handles multiple API response formats
                        sites = []
                        if isinstance(data, list):
                            # Direct list: [{"site_code": "SITE001"}, ...]
                            sites = [item.get('site_code') for item in data if item.get('site_code')]
                        elif isinstance(data, dict):
                            # Various nested formats
                            if 'sites' in data:
                                sites = [site.get('site_code') for site in data['sites'] if site.get('site_code')]
                            elif 'data' in data:
                                sites = [site.get('site_code') for site in data['data'] if site.get('site_code')]
                            elif 'result' in data:
                                if isinstance(data['result'], list):
                                    sites = [site.get('site_code') for site in data['result'] if site.get('site_code')]
                            elif 'site_codes' in data:
                                sites = data['site_codes'] if isinstance(data['site_codes'], list) else []
                            elif 'sites_list' in data:
                                sites = data['sites_list'] if isinstance(data['sites_list'], list) else []
                        
                        # Clean and validate sites
                        sites = [str(site).strip() for site in sites if site is not None]
                        sites = [site for site in sites if site]  # Remove empty strings
                        
                        # Apply max sites limit
                        if len(sites) > self.api_config['max_sites']:
                            print(f"⚠️  Too many sites ({len(sites)}), limiting to {self.api_config['max_sites']}")
                            sites = sites[:self.api_config['max_sites']]
                        
                        print(f"🏢 Fetched {len(sites)} sites for company {company_id}: {sites}")
                        return sites
                        
                    elif response.status == 404:
                        print(f"⚠️  No sites found for company {company_id} (404 Not Found)")
                        return []
                    elif response.status == 401:
                        print(f"❌ Authentication failed for company {company_id} (401 Unauthorized)")
                        print(f"   Token: {'***' + self.api_config['token'][-10:] if self.api_config['token'] else 'MISSING'}")
                        return []
                    elif response.status == 403:
                        print(f"❌ Access forbidden for company {company_id} (403 Forbidden)")
                        return []
                    else:
                        error_text = await response.text()
                        print(f"⚠️  API error {response.status} for company {company_id}")
                        print(f"   Response: {error_text[:200]}...")  # First 200 chars of error
                        return []
                        
        except asyncio.TimeoutError:
            print(f"⏰ Timeout fetching sites for company {company_id} (after {self.api_config['timeout']}s)")
            return []
        except aiohttp.ClientError as e:
            print(f"🌐 Network error fetching sites for company {company_id}: {e}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error fetching sites for {company_id}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def fetch_pending_evaluations(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch all pending badge evaluations from badge_evaluation_queue
        Expand entries where site_code="all" or null into separate entries for each site
        """
        try:
            print("🔍 Fetching pending badge evaluations...")
            
            # Fetch all pending evaluations
            query = {"status": "pending"}
            cursor = self.db[self.queue_collection].find(query)
            
            if limit:
                cursor = cursor.limit(limit)
            
            raw_evaluations = await cursor.to_list(length=None)
            
            if not raw_evaluations:
                print("📋 No pending evaluations found")
                return []
            
            print(f"📋 Found {len(raw_evaluations)} pending evaluations")
            
            # Check if site expansion is enabled
            if not self.api_config['expand_enabled']:
                print("🚫 Site expansion disabled - returning evaluations as-is")
                return raw_evaluations
            
            final_evaluations = []
            expansions_performed = 0
            expansion_stats = {'expanded': 0, 'kept_as_all': 0, 'specific_sites': 0, 'api_errors': 0}
            
            for evaluation in raw_evaluations:
                site_code = evaluation.get('site_code')
                company_id = evaluation.get('company_id')
                
                # Check if this evaluation needs site expansion
                if site_code == "all" or site_code is None:
                    print(f"🔄 Expanding evaluation for company {company_id} (site_code: {site_code})")
                    
                    try:
                        # Fetch all sites for this company
                        company_sites = await self.fetch_company_sites_api(company_id)
                        
                        if company_sites:
                            # Create one evaluation per site
                            for site in company_sites:
                                expanded_evaluation = evaluation.copy()
                                expanded_evaluation['site_code'] = site
                                
                                # Add expansion metadata
                                if 'metadata' not in expanded_evaluation:
                                    expanded_evaluation['metadata'] = {}
                                expanded_evaluation['metadata'].update({
                                    'expanded_from_all_sites': True,
                                    'original_site_code': site_code,
                                    'expansion_timestamp': time.time(),
                                    'original_evaluation_id': str(evaluation.get('_id')),
                                    'api_base_url': self.api_config['base_url'],
                                    'expansion_type': 'specific_site'
                                })
                                
                                # Remove original _id to avoid conflicts
                                if '_id' in expanded_evaluation:
                                    del expanded_evaluation['_id']
                                
                                final_evaluations.append(expanded_evaluation)
                            
                            # ADDITION: Create an additional default evaluation with site_code=""
                            default_evaluation = evaluation.copy()
                            default_evaluation['site_code'] = ""
                            
                            # Add expansion metadata for default entry
                            if 'metadata' not in default_evaluation:
                                default_evaluation['metadata'] = {}
                            default_evaluation['metadata'].update({
                                'expanded_from_all_sites': True,
                                'original_site_code': site_code,
                                'expansion_timestamp': time.time(),
                                'original_evaluation_id': str(evaluation.get('_id')),
                                'api_base_url': self.api_config['base_url'],
                                'expansion_type': 'default_empty_site'
                            })
                            
                            # Remove original _id to avoid conflicts
                            if '_id' in default_evaluation:
                                del default_evaluation['_id']
                            
                            final_evaluations.append(default_evaluation)
                            
                            expansions_performed += 1
                            expansion_stats['expanded'] += 1
                            total_created = len(company_sites) + 1  # +1 for the default entry
                            print(f"✅ Expanded into {len(company_sites)} site-specific evaluations + 1 default entry (total: {total_created})")
                        else:
                            # Handle fallback based on configuration
                            if self.api_config['fallback_mode'] == 'keep_all':
                                evaluation['site_code'] = 'all'
                                final_evaluations.append(evaluation)
                                expansion_stats['kept_as_all'] += 1
                                print(f"⚠️  No sites found for {company_id}, keeping as 'all'")
                            elif self.api_config['fallback_mode'] == 'skip':
                                expansion_stats['kept_as_all'] += 1
                                print(f"⚠️  No sites found for {company_id}, skipping evaluation")
                                # Don't add to final_evaluations
                            else:
                                # Default: keep as 'all'
                                evaluation['site_code'] = 'all'
                                final_evaluations.append(evaluation)
                                expansion_stats['kept_as_all'] += 1
                                print(f"⚠️  No sites found for {company_id}, keeping as 'all' (default)")
                                
                    except Exception as e:
                        print(f"❌ Error expanding evaluation for {company_id}: {e}")
                        expansion_stats['api_errors'] += 1
                        # Fallback: keep original evaluation
                        evaluation['site_code'] = 'all'
                        final_evaluations.append(evaluation)
                        
                else:
                    # Specific site code, keep as-is
                    final_evaluations.append(evaluation)
                    expansion_stats['specific_sites'] += 1
            
            # Print summary
            print(f"✅ Final result: {len(final_evaluations)} evaluations ready for processing")
            if expansions_performed > 0:
                original_count = len(raw_evaluations)
                expanded_count = len(final_evaluations)
                expansion_factor = expanded_count / original_count if original_count > 0 else 1
                print(f"📈 Expansion Summary:")
                print(f"   Original evaluations: {original_count}")
                print(f"   Final evaluations: {expanded_count}")
                print(f"   Expansion factor: {expansion_factor:.2f}x")
                print(f"   Expanded: {expansion_stats['expanded']}")
                print(f"   Kept as 'all': {expansion_stats['kept_as_all']}")
                print(f"   Specific sites: {expansion_stats['specific_sites']}")
                print(f"   API errors: {expansion_stats['api_errors']}")
            
            return final_evaluations
            
        except Exception as e:
            print(f"❌ Error fetching pending evaluations: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def get_badges_for_reevaluation(self, affected_collections: List[str]) -> List[Dict[str, Any]]:
        """Find badges that need re-evaluation based on affected collections"""
        try:
            print(f"🔍 Finding badges for collections: {affected_collections}")
            
            # Step 1: Find KPIs with affected collections
            kpi_cursor = self.db.kpis.find({
                "evaluation_method.collections.collection": {"$in": affected_collections}
            })
            affected_kpis = await kpi_cursor.to_list(length=None)
            affected_kpi_ids = [kpi["_id"] for kpi in affected_kpis]  # Use _id field
            
            if not affected_kpi_ids:
                print(f"⚠️  No KPIs found for collections: {affected_collections}")
                return []
            
            print(f"📊 Found {len(affected_kpi_ids)} affected KPIs")
            
            # Step 2: Find criteria that use these KPIs
            criteria_cursor = self.db.criterias.find({
                "kpi_ids": {"$in": affected_kpi_ids}
            })
            affected_criteria = await criteria_cursor.to_list(length=None)
            affected_criteria_ids = [criteria["_id"] for criteria in affected_criteria]  # Use _id field
            
            if not affected_criteria_ids:
                print(f"⚠️  No criteria found for KPI IDs: {affected_kpi_ids}")
                return []
            
            print(f"📋 Found {len(affected_criteria_ids)} affected criteria")
            
            # Step 3: Find badges that use these criteria
            badges_cursor = self.db.badges.find({
                "criteria_id": {"$in": affected_criteria_ids}
            })
            badges_to_reevaluate = await badges_cursor.to_list(length=None)
            
            print(f"🏆 Found {len(badges_to_reevaluate)} badges that need re-evaluation")
            return badges_to_reevaluate
            
        except Exception as e:
            print(f"❌ Error finding badges for re-evaluation: {e}")
            return []
    
    async def check_badge_prerequisites(self, company_id: str, site_code: str, badge: Dict) -> bool:
        """Check if user has achieved all prerequisite badges based on level_rank"""
        try:
            level_rank = badge.get('level_rank', 0)
            badge_type = badge.get('type', badge.get('category'))
            
            # If this is the lowest level badge (rank 0 or 1), no prerequisites needed
            if level_rank <= 1:
                return True
            
            print(f"    🔍 Checking prerequisites for level {level_rank} badge...")
            
            # Find all badges of the same type with lower level_rank
            prerequisite_query = {
                "level_rank": {"$lt": level_rank}
            }
            
            # Add type/category filter if available
            if badge_type:
                prerequisite_query["$or"] = [
                    {"type": badge_type},
                    {"category": badge_type}
                ]
            
            prerequisite_badges_cursor = self.db.badges.find(prerequisite_query)
            prerequisite_badges = await prerequisite_badges_cursor.to_list(length=None)
            
            if not prerequisite_badges:
                print(f"    ✅ No prerequisite badges found for level {level_rank}")
                return True
            
            # Get user's current badge statuses
            user_badge_statuses = await self._get_user_badge_statuses(company_id, site_code)
            
            # Check if user has achieved all prerequisite badges
            missing_prerequisites = []
            for prereq_badge in prerequisite_badges:
                prereq_badge_id = str(prereq_badge.get('_id'))  # Use _id for consistency
                prereq_name = prereq_badge.get('name', 'Unknown')
                prereq_level = prereq_badge.get('level_rank', 0)
                
                # Check if user has this prerequisite badge with correct status
                user_status = user_badge_statuses.get(prereq_badge_id)
                if not user_status or user_status.get('status') not in ['passed', 'achieved']:
                    missing_prerequisites.append({
                        'badge_id': prereq_badge_id,
                        'name': prereq_name,
                        'level_rank': prereq_level,
                        'current_status': user_status.get('status') if user_status else 'not_found'
                    })
            
            if missing_prerequisites:
                print(f"    ❌ Missing prerequisites for {badge.get('name')}:")
                for missing in missing_prerequisites:
                    print(f"      - {missing['name']} (Level {missing['level_rank']}): {missing['current_status']}")
                return False
            
            print(f"    ✅ All prerequisites met for level {level_rank} badge")
            return True
            
        except Exception as e:
            print(f"    ⚠️  Error checking prerequisites: {e}")
            return True  # Fail open
    
    async def _get_user_badge_statuses(self, company_id: str, site_code: str) -> Dict[str, Dict]:
        """Get user's current badge statuses from user_badges collection"""
        try:
            # Query user_badges collection for this company/site
            user_badges_cursor = self.db.user_badges.find({
                "company_id": str(company_id),  # Ensure string type
                "site_code": site_code
            })
            
            user_badges = await user_badges_cursor.to_list(length=None)
            
            # Create a mapping of badge_id to status
            badge_statuses = {}
            for user_badge in user_badges:
                badge_id = str(user_badge.get('badge_id'))
                badge_statuses[badge_id] = {
                    'status': user_badge.get('status'),
                    'achieved_at': user_badge.get('achieved_at'),
                    'updated_at': user_badge.get('updated_at')
                }
            
            print(f"    📋 Found {len(badge_statuses)} existing badge statuses for user")
            return badge_statuses
            
        except Exception as e:
            print(f"    ⚠️  Error fetching user badge statuses: {e}")
            return {}
    
    async def process_single_badge_evaluation(self, company_id: str, site_code: str, badge_id: str, 
                                            message_id: str, skip_prerequisites: bool = False) -> Dict[str, Any]:
        """Process a single badge evaluation without prerequisite checking (for batch processing)"""
        try:
            print(f"🔄 Processing badge evaluation: Company {company_id}, Site {site_code}, Badge {badge_id}")
            
            # Step 1: Get badge info to find associated criteria
            badge_info = await self.badge_verifier.get_badge_info(badge_id)
            
            if not badge_info["found"]:
                return {
                    "success": False,
                    "error": "Badge not found",
                    "eligible": False
                }
            
            criteria_id = badge_info["criteria_id"]
            print(f"📋 Badge {badge_id} is associated with criteria {criteria_id}")
            
            # Step 2: Get criteria info to find associated KPIs
            criteria_doc = await self.db.criterias.find_one({"_id": criteria_id})  # Use _id for consistency
            
            if not criteria_doc:
                return {
                    "success": False,
                    "error": "Criteria not found",
                    "eligible": False
                }
            
            kpi_ids = criteria_doc.get("kpi_ids", [])
            
            if not kpi_ids:
                return {
                    "success": False,
                    "error": "No KPIs found for criteria",
                    "eligible": False
                }
            
            print(f"📋 Found {len(kpi_ids)} KPIs for criteria {criteria_id}")
            
            # Step 3: Validate each KPI
            kpi_results = []
            for kpi_id in kpi_ids:
                print(f"🔍 Validating KPI {kpi_id}")
                
                try:
                    # Add timeout for individual KPI processing
                    kpi_result = await asyncio.wait_for(
                        self.kpi_validator.validate_and_save_kpi(
                            company_id=company_id,
                            site_code=site_code,
                            kpi_id=kpi_id
                        ),
                        timeout=self.timeout_per_badge
                    )
                    
                    kpi_results.append({
                        "kpi_id": kpi_id,
                        "result": kpi_result
                    })
                    
                    print(f"✅ KPI {kpi_id} validation completed")
                    
                except asyncio.TimeoutError:
                    print(f"❌ Timeout validating KPI {kpi_id}")
                    kpi_results.append({
                        "kpi_id": kpi_id,
                        "error": "Timeout during validation"
                    })
                    # Don't fail completely on timeout, continue with other KPIs
                    
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
                    "eligible": False,
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
                    "eligible": False,
                    "criteria_result": criteria_result,
                    "kpi_results": kpi_results
                }
            
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return {
                "success": False,
                "error": f"Unexpected error: {str(e)}",
                "eligible": False
            }
    
    async def process_badge_evaluation(self, company_id: str, site_code: str, badge_id: str, 
                                     message_id: str) -> Dict[str, Any]:
        """Process a single badge evaluation request with prerequisite checking"""
        try:
            # Step 1: Check prerequisites first
            badge_doc = await self.db.badges.find_one({"_id": badge_id})  # Use _id for consistency
            if badge_doc and not await self.check_badge_prerequisites(company_id, site_code, badge_doc):
                return {
                    "success": False,
                    "error": "Badge prerequisites not met",
                    "prerequisite_failed": True,
                    "eligible": False
                }
            
            # Step 2: Process the badge evaluation (without duplicate prerequisite check)
            return await self.process_single_badge_evaluation(
                company_id=company_id,
                site_code=site_code, 
                badge_id=badge_id,
                message_id=message_id,
                skip_prerequisites=True
            )
            
        except Exception as e:
            print(f"❌ Error in process_badge_evaluation: {e}")
            return {
                "success": False,
                "error": f"Badge evaluation failed: {str(e)}",
                "eligible": False
            }
    
    def _is_badge_criteria_met(self, evaluation_result) -> bool:
        """Determine if badge criteria is met based on evaluation result"""
        if evaluation_result is None:
            return False
        
        if isinstance(evaluation_result, dict):
            # Common patterns for success indicators
            if 'success' in evaluation_result:
                return evaluation_result['success'] and evaluation_result.get('eligible', False)
            elif 'status' in evaluation_result:
                return evaluation_result['status'] in ['success', 'passed', 'completed']
            elif 'criteria_met' in evaluation_result:
                return evaluation_result['criteria_met']
            elif 'result' in evaluation_result:
                return evaluation_result['result'] in ['pass', 'success', True]
            elif 'eligible' in evaluation_result:
                return evaluation_result['eligible']
            
            # If no clear indicator, assume success if we got a valid dict
            return True
        
        # For non-dict results, assume success if not None
        return evaluation_result is not None
    
    async def process_company_site_badges(self, group: Dict, badges: List[Dict]) -> Dict:
        """Process all badges for a company/site with prerequisite checking"""
        company_id = group['company_id']
        site_code = group['site_code']
        start_time = time.time()
        
        try:
            print(f"🏆 Processing {len(badges)} badges for {company_id}/{site_code}")
            
            # Sort badges by level_rank to ensure proper order
            sorted_badges = sorted(badges, key=lambda b: b.get('level_rank', 0))
            
            # Limit badges per group for Lambda timeout management
            if len(sorted_badges) > self.max_badges_per_group:
                print(f"⚠️  Limiting badges from {len(sorted_badges)} to {self.max_badges_per_group}")
                sorted_badges = sorted_badges[:self.max_badges_per_group]
            
            badges_evaluated = 0
            failed_badge = None
            prerequisite_failed = False
            
            # Process badges sequentially and stop on first failure
            for badge in sorted_badges:
                badge_id = str(badge.get('_id'))  # Use _id field from badge document
                badge_name = badge.get('name', 'Unknown')
                level_rank = badge.get('level_rank', 0)
                
                print(f"  🔍 Evaluating badge: {badge_name} (Level {level_rank})")
                
                # Check prerequisites first
                if not await self.check_badge_prerequisites(company_id, site_code, badge):
                    failed_badge = badge_name
                    prerequisite_failed = True
                    print(f"  ❌ Prerequisites not met for {badge_name}")
                    break
                
                # Process badge evaluation without duplicate prerequisite checking
                message_id = f"queue_reeval_{company_id}_{site_code}_{badge_id}_{int(time.time())}"
                
                try:
                    result = await self.process_single_badge_evaluation(
                        company_id=company_id,
                        site_code=site_code,
                        badge_id=badge_id,
                        message_id=message_id,
                        skip_prerequisites=True  # Already checked above
                    )
                    
                    badges_evaluated += 1
                    
                    # Check if badge criteria is met
                    if not self._is_badge_criteria_met(result):
                        failed_badge = badge_name
                        print(f"  ❌ Badge criteria not met for {badge_name}")
                        break
                    else:
                        print(f"  ✅ Badge criteria met for {badge_name}")
                        
                except Exception as badge_error:
                    failed_badge = badge_name
                    print(f"  ❌ Error evaluating badge {badge_name}: {badge_error}")
                    break
            
            duration = time.time() - start_time
            success = failed_badge is None
            
            result_data = {
                'success': success,
                'company_id': company_id,
                'site_code': site_code,
                'badges_evaluated': badges_evaluated,
                'total_badges_available': len(badges),
                'failed_badge': failed_badge,
                'prerequisite_failed': prerequisite_failed,
                'duration': duration,
                'evaluation_ids': group['evaluation_ids'],
                'affected_collections': group['affected_collections']
            }
            
            return result_data
            
        except Exception as e:
            duration = time.time() - start_time
            print(f"  ❌ Exception processing badges for {company_id}/{site_code}: {e}")
            
            return {
                'success': False,
                'company_id': company_id,
                'site_code': site_code,
                'badges_evaluated': 0,
                'total_badges_available': len(badges),
                'failed_badge': None,
                'prerequisite_failed': False,
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
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": time.time()
                    }
                }
            )
            print(f"  📝 Marked {result.modified_count} evaluations as completed")
        except Exception as e:
            print(f"  ⚠️  Error marking evaluations as completed: {e}")
    
    async def process_badge_reevaluation_queue(self, max_groups: Optional[int] = None) -> Dict[str, Any]:
        """Main method to process badge re-evaluation queue with site expansion"""
        start_time = time.time()
        
        try:
            print("🎯 Starting enhanced badge re-evaluation from queue...")
            
            # Test API connection first if expansion is enabled
            if self.api_config['expand_enabled']:
                api_working = await self.test_api_connection()
                if not api_working:
                    print("⚠️  API connection test failed, but continuing with processing")
            
            # Fetch pending evaluations with site expansion and limit for Lambda optimization
            batch_size = max_groups or self.max_concurrent_groups
            pending_evaluations = await self.fetch_pending_evaluations(limit=batch_size * 10)
            
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
            for group in list(company_site_groups.values())[:batch_size]:
                group['affected_collections'] = list(group['affected_collections'])
                groups_to_process.append(group)
            
            print(f"🎯 Processing {len(groups_to_process)} company/site groups")
            
            # Process groups sequentially for Lambda reliability
            results = []
            for group in groups_to_process:
                try:
                    # Get badges that need re-evaluation for this group
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
                        
                        # Mark evaluations as completed if successful
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
            
            # Calculate metrics
            execution_time = time.time() - start_time
            successful = sum(1 for r in results if r.get('success'))
            failed = len(results) - successful
            total_badges_evaluated = sum(
                r.get('badges_evaluated', 0) for r in results 
                if isinstance(r, dict)
            )
            
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
            print(f"❌ Error in enhanced badge re-evaluation process: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time": time.time() - start_time
            }


# Global processor instance (reused across Lambda invocations)
processor = None

def lambda_handler(event, context):
    """
    Enhanced AWS Lambda handler for processing badge re-evaluation from queue.
    Supports both SQS messages (legacy) and direct queue processing (new).
    """
    global processor
    
    try:
        # Initialize processor if not already done
        if processor is None:
            processor = EnhancedLambdaBadgeProcessor()
        
        # Check if this is an SQS event (legacy support)
        records = event.get('Records', [])
        
        if records and records[0].get('eventSource') == 'aws:sqs':
            # Legacy SQS processing
            print("📨 Processing SQS records (legacy mode)")
            return process_sqs_records(records, context)
        else:
            # New enhanced queue-based processing
            print("🔄 Processing enhanced badge re-evaluation queue")
            return process_badge_queue(event, context)
    
    except Exception as e:
        print(f"❌ Unexpected error in enhanced lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def process_badge_queue(event, context):
    """Process badge re-evaluation queue (new enhanced primary method)"""
    global processor
    
    # Get remaining time for Lambda timeout management
    remaining_time = context.get_remaining_time_in_millis() if context else 300000
    print(f"⏰ Enhanced Lambda started with {remaining_time}ms remaining")
    
    # Calculate max groups based on remaining time
    estimated_time_per_group = 30000  # 30 seconds per group
    max_groups = min(10, (remaining_time - 60000) // estimated_time_per_group)  # Leave 60s buffer
    max_groups = max(1, max_groups)  # At least 1 group
    
    # Process queue in async context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(process_queue_async(max_groups))
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Enhanced badge re-evaluation completed",
                "result": result
            }, default=str)
        }
        
    finally:
        loop.close()

def process_sqs_records(records, context):
    """Process SQS records (legacy support)"""
    global processor
    
    # Process records in async context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        results = loop.run_until_complete(process_sqs_records_async(records))
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Processed {len(results)} records",
                "results": results
            }, default=str)
        }
        
    finally:
        loop.close()

async def process_queue_async(max_groups):
    """Process enhanced badge re-evaluation queue asynchronously"""
    global processor
    
    # Connect to database
    await processor.connect_to_db()
    
    try:
        # Process the enhanced badge re-evaluation queue
        result = await processor.process_badge_reevaluation_queue(max_groups=max_groups)
        return result
        
    finally:
        # Clean up connections
        processor.disconnect_from_db()

async def process_sqs_records_async(records):
    """Process SQS records asynchronously (legacy support)"""
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
                
                # Process the badge evaluation (with prerequisite checking for legacy support)
                result = await processor.process_badge_evaluation(
                    company_id=company_id,
                    site_code=site_code,
                    badge_id=badge_id,
                    message_id=message_id
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