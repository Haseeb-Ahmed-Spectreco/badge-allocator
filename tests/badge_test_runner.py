from typing import List, Dict, Any, Optional
from tests import ExecutionMode, Config
import asyncio
import time
import aiohttp  # Added for API calls
import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

# Load environment variables
load_dotenv()


class BadgeReevaluationRunner:
    """Efficient test runner for badge re-evaluation from badge_evaluation_queue"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.db = None
        
        # Load and validate API configuration
        self._load_api_config()
        
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
        
    async def __aenter__(self):
        # Connect to MongoDB
        config_dict = self.config.get_config()
        self.mongo_client = AsyncIOMotorClient(config_dict['MONGO_URI'])
        self.db = self.mongo_client[config_dict['MONGO_DB_NAME']]
        # Test connection
        await self.db.command("ping")
        print(f"✅ Connected to MongoDB: {config_dict['MONGO_DB_NAME']}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.mongo_client:
            self.mongo_client.close()
            print("🔌 MongoDB connection closed")
    
    def _print_summary(self, summary: Dict[str, Any]):
        """Print formatted test summary"""
        print(f"\n📊 BADGE RE-EVALUATION SUMMARY")
        print(f"{'='*50}")
        print(f"Mode: {summary['mode'].upper()}")
        print(f"Total pending evaluations: {summary['total_pending']}")
        print(f"Evaluations processed: {summary['processed_evaluations']}")
        print(f"Companies/sites affected: {summary['unique_company_sites']}")
        print(f"Total badges re-evaluated: {summary['total_badges_evaluated']}")
        print(f"Successful evaluations: {summary['successful']}")
        print(f"Failed evaluations: {summary['failed']}")
        print(f"Success rate: {summary['success_rate']:.2%}")
        print(f"Total execution time: {summary['execution_time']:.2f}s")
        print(f"Average time per evaluation: {summary['avg_time_per_evaluation']:.3f}s")
        print(f"Overall result: {'✅ PASSED' if summary['success'] else '❌ FAILED'}")

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
    
    async def fetch_pending_evaluations(self) -> List[Dict[str, Any]]:
        """
        Fetch all pending badge evaluations from badge_evaluation_queue
        Expand entries where site_code="all" or null into separate entries for each site
        """
        try:
            print("🔍 Fetching pending badge evaluations...")
            
            # Fetch all pending evaluations
            cursor = self.db.badge_evaluation_queue.find({"status": "pending"})
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
        """
        Find all badges that need re-evaluation based on affected collections.
        
        Logic:
        1. Find KPIs where evaluation_method.collections contains any of the affected collections
        2. Find criteria that reference these KPIs
        3. Find badges that use these criteria
        """
        try:
            # Step 1: Find KPIs with affected collections
            # evaluation_method.collections is a list of objects with 'collection' attribute
            kpi_cursor = self.db.kpis.find({
                "evaluation_method.collections.collection": {"$in": affected_collections}
            })
            affected_kpis = await kpi_cursor.to_list(length=None)
            affected_kpi_ids = [kpi["_id"] for kpi in affected_kpis]
            
            if not affected_kpi_ids:
                print(f"⚠️  No KPIs found for collections: {affected_collections}")
                return []
            
            print(f"📊 Found {len(affected_kpi_ids)} KPIs affected by collections: {affected_collections}")
            
            # Step 2: Find criteria that use these KPIs
            criteria_cursor = self.db.criterias.find({
                "kpi_ids": {"$in": affected_kpi_ids}
            })
            affected_criteria = await criteria_cursor.to_list(length=None)
            affected_criteria_ids = [criteria["_id"] for criteria in affected_criteria]
            
            if not affected_criteria_ids:
                print(f"⚠️  No criteria found for KPI IDs: {affected_kpi_ids}")
                return []
            
            print(f"📋 Found {len(affected_criteria_ids)} criteria affected")
            
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
    
    async def run_badge_reevaluation(self, processor, mode: ExecutionMode = ExecutionMode.SEQUENTIAL) -> Dict[str, Any]:
        """Run badge re-evaluation tests with specified mode"""
        start_time = time.time()
        
        # Fetch pending evaluations (now includes site expansion)
        pending_evaluations = await self.fetch_pending_evaluations()
        if not pending_evaluations:
            return {"success": False, "error": "No pending evaluations found", "results": []}
        
        # Group by company_id and site_code for efficient processing
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
        
        # Convert sets back to lists
        for group in company_site_groups.values():
            group['affected_collections'] = list(group['affected_collections'])
        
        test_groups = list(company_site_groups.values())
        
        # Apply sampling if needed
        if mode == ExecutionMode.SAMPLE:
            test_groups = test_groups[:self.config.sample_size]
        
        print(f"🎯 Processing {len(test_groups)} company/site groups with {mode.value} mode")
        
        # Execute based on mode
        if mode == ExecutionMode.CONCURRENT:
            results = await self._run_concurrent_reevaluation(processor, test_groups)
        elif mode == ExecutionMode.BATCH:
            results = await self._run_batch_reevaluation(processor, test_groups)
        else:  # SEQUENTIAL or SAMPLE
            results = await self._run_sequential_reevaluation(processor, test_groups)
        
        # Calculate metrics
        execution_time = time.time() - start_time
        successful = sum(1 for r in results if r.get('success'))
        failed = len(results) - successful
        success_rate = successful / len(results) if results else 0
        
        # Count total badges evaluated
        total_badges_evaluated = sum(r.get('badges_evaluated', 0) for r in results)
        
        summary = {
            "success": success_rate >= self.config.min_success_rate,
            "total_pending": len(pending_evaluations),
            "processed_evaluations": len(test_groups),
            "unique_company_sites": len(test_groups),
            "total_badges_evaluated": total_badges_evaluated,
            "successful": successful,
            "failed": failed,
            "success_rate": success_rate,
            "execution_time": execution_time,
            "avg_time_per_evaluation": execution_time / len(results) if results else 0,
            "mode": mode.value,
            "results": results if self.config.log_individual_results else []
        }
        
        self._print_summary(summary)
        return summary
    
    async def _run_sequential_reevaluation(self, processor, groups: List[Dict]) -> List[Dict]:
        """Run badge re-evaluation sequentially (recommended approach)"""
        results = []
        
        for i, group in enumerate(groups, 1):
            print(f"🔄 Processing group {i}/{len(groups)}: {group['company_id']}/{group['site_code']}")
            
            # Get badges that need re-evaluation for this group
            badges_to_evaluate = await self.get_badges_for_reevaluation(
                group['affected_collections']
            )
            
            if not badges_to_evaluate:
                results.append({
                    'success': True,  # No badges to evaluate is considered success
                    'company_id': group['company_id'],
                    'site_code': group['site_code'],
                    'badges_evaluated': 0,
                    'prerequisite_failed': False,
                    'message': 'No badges found for re-evaluation',
                    'duration': 0
                })
                continue
            
            # Process this company/site group
            result = await self._process_company_site_badges(
                processor, group, badges_to_evaluate
            )
            results.append(result)
            
            # Fail-fast logic: if any company/site fails and fail_fast is enabled
            if self.config.fail_fast and not result.get('success'):
                print("🛑 Fail-fast mode: stopping on first company/site failure")
                break
            
            # Delay between company/site processing
            if i < len(groups):
                await asyncio.sleep(self.config.delay_between_requests)
        
        return results
    
    async def _run_concurrent_reevaluation(self, processor, groups: List[Dict]) -> List[Dict]:
        """Run badge re-evaluation with controlled concurrency"""
        from rich.progress import Progress, TextColumn, BarColumn
        
        with Progress() as progress:
            task_id = progress.add_task("Re-evaluating badges...", total=len(groups))
            semaphore = asyncio.Semaphore(self.config.max_concurrent)
            completed = 0
            
            async def process_with_progress(group):
                nonlocal completed
                async with semaphore:
                    # Get badges for this group
                    badges_to_evaluate = await self.get_badges_for_reevaluation(
                        group['affected_collections']
                    )
                    
                    if not badges_to_evaluate:
                        result = {
                            'success': True,
                            'company_id': group['company_id'],
                            'site_code': group['site_code'],
                            'badges_evaluated': 0,
                            'prerequisite_failed': False,
                            'message': 'No badges found for re-evaluation',
                            'duration': 0
                        }
                    else:
                        result = await self._process_company_site_badges(
                            processor, group, badges_to_evaluate
                        )
                    
                    completed += 1
                    progress.update(task_id, completed=completed)
                    return result
            
            tasks = [process_with_progress(group) for group in groups]
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_batch_reevaluation(self, processor, groups: List[Dict]) -> List[Dict]:
        """Run badge re-evaluation in batches"""
        batches = [groups[i:i + self.config.batch_size] 
                  for i in range(0, len(groups), self.config.batch_size)]
        
        all_results = []
        for batch_num, batch in enumerate(batches, 1):
            print(f"🔄 Processing batch {batch_num}/{len(batches)}")
            
            batch_tasks = []
            for group in batch:
                badges_to_evaluate = await self.get_badges_for_reevaluation(
                    group['affected_collections']
                )
                if badges_to_evaluate:
                    batch_tasks.append(
                        self._process_company_site_badges(processor, group, badges_to_evaluate)
                    )
                else:
                    # Add a successful result for groups with no badges to evaluate
                    batch_tasks.append(asyncio.create_task(asyncio.coroutine(lambda: {
                        'success': True,
                        'company_id': group['company_id'],
                        'site_code': group['site_code'],
                        'badges_evaluated': 0,
                        'message': 'No badges found for re-evaluation',
                        'duration': 0
                    })()))
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            all_results.extend(batch_results)
            
            if batch_num < len(batches):
                await asyncio.sleep(self.config.delay_between_batches)
        
        return all_results
    
    async def _process_company_site_badges(self, processor, group: Dict, badges: List[Dict]) -> Dict:
        """
        Process all badges for a specific company/site.
        Stop processing if any badge criteria is not met.
        """
        company_id = group['company_id']
        site_code = group['site_code']
        start_time = time.time()
        prerequisite_failed = False
        
        try:
            print(f"🏆 Evaluating {len(badges)} badges for {company_id}/{site_code}")
            
            badges_evaluated = 0
            failed_badge = None
            
            # Process badges sequentially and stop on first failure
            for badge in badges:
                badge_id = badge.get('_id')
                badge_name = badge.get('name', 'Unknown')
                
                print(f"  🔍 Evaluating badge: {badge_name} ({badge_id})")
                
                # Create unique message ID for this evaluation
                message_id = f"reeval_{company_id}_{site_code}_{badge_id}_{int(time.time())}"
                
                # Process badge evaluation using the existing processor
                result = await processor.process_badge_evaluation(
                    company_id=company_id,
                    site_code=site_code,
                    badge_id=str(badge_id),
                    message_id=message_id
                )
                
                badges_evaluated += 1
                
                # Check if badge criteria is met
                if not self._is_badge_criteria_met(result):
                    failed_badge = badge_name
                    print(f"  ❌ Badge criteria not met for {badge_name} - stopping evaluation")
                    break
                else:
                    print(f"  ✅ Badge criteria met for {badge_name}")
            
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
            
            if success:
                # Mark all related evaluations as completed
                await self._mark_evaluations_completed(group['evaluation_ids'])
                print(f"  ✅ All {badges_evaluated} badges passed for {company_id}/{site_code}")
            else:
                print(f"  ❌ Badge evaluation failed at badge '{failed_badge}' for {company_id}/{site_code}")
            
            return result_data
            
        except Exception as e:
            duration = time.time() - start_time
            print(f"  ❌ Exception during badge evaluation for {company_id}/{site_code}: {e}")
            
            return {
                'success': False,
                'company_id': company_id,
                'site_code': site_code,
                'badges_evaluated': 0,
                'total_badges_available': len(badges),
                'failed_badge': None,
                'prerequisite_failed': False,
                'error': str(e),
                'exception_type': type(e).__name__,
                'duration': duration,
                'evaluation_ids': group['evaluation_ids'],
                'affected_collections': group['affected_collections']
            }
    
    async def _check_badge_prerequisites(self, company_id: str, site_code: str, badge: Dict) -> bool:
        """
        Check if user has achieved all prerequisite badges based on level_rank.
        Returns True if prerequisites are met or if this is the lowest level badge.
        """
        try:
            level_rank = badge.get('level_rank', 0)
            badge_type = badge.get('type', badge.get('category'))  # Assuming badges have type or category
            
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
                prereq_badge_id = str(prereq_badge.get('badge_id'))
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
            # In case of error, allow evaluation to proceed (fail open)
            return True
    
    async def _get_user_badge_statuses(self, company_id: str, site_code: str) -> Dict[str, Dict]:
        """
        Get user's current badge statuses from user_badges collection.
        Returns a dict mapping badge_id to status info.
        """
        try:
            # Query user_badges collection for this company/site
            user_badges_cursor = self.db.user_badges.find({
                "company_id": company_id,
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

    def _is_badge_criteria_met(self, evaluation_result) -> bool:
        """
        Determine if badge criteria is met based on evaluation result.
        Customize this logic based on your processor's return format.
        """
        if evaluation_result is None:
            return False
        
        if isinstance(evaluation_result, dict):
            # Common patterns for success indicators
            if 'success' in evaluation_result:
                return evaluation_result['success']
            elif 'status' in evaluation_result:
                return evaluation_result['status'] in ['success', 'passed', 'completed']
            elif 'criteria_met' in evaluation_result:
                return evaluation_result['criteria_met']
            elif 'result' in evaluation_result:
                return evaluation_result['result'] in ['pass', 'success', True]
            
            # If no clear indicator, assume success if we got a valid dict
            return True
        
        # For non-dict results, assume success if not None
        return evaluation_result is not None
    
    async def _mark_evaluations_completed(self, evaluation_ids: List[str]):
        """Mark badge evaluations as completed in the queue"""
        try:
            result = await self.db.badge_evaluation_queue.update_many(
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
            print(f"  ⚠️  Failed to mark evaluations as completed: {e}")