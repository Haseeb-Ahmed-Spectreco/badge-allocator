#!/usr/bin/env python3
"""
Nightly Batch Badge Evaluation System
Collects changes during the day and processes them once at night
"""

import asyncio
import logging
import os
import signal
from datetime import datetime, timezone, time
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import schedule
import threading
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class PendingEvaluationEntry:
    """Represents a pending badge evaluation entry"""
    company_id: str
    site_code: str
    triggered_by: List[str] = field(default_factory=list)  # What changes triggered this
    badge_ids: List[str] = field(default_factory=list)     # Specific badges to evaluate
    priority: int = 2
    first_triggered: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    change_count: int = 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'company_id': self.company_id,
            'site_code': self.site_code,
            'triggered_by': self.triggered_by,
            'badge_ids': self.badge_ids,
            'priority': self.priority,
            'first_triggered': self.first_triggered,
            'last_updated': self.last_updated,
            'change_count': self.change_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PendingEvaluationEntry':
        return cls(
            company_id=data['company_id'],
            site_code=data['site_code'],
            triggered_by=data.get('triggered_by', []),
            badge_ids=data.get('badge_ids', []),
            priority=data.get('priority', 2),
            first_triggered=data.get('first_triggered', datetime.now(timezone.utc)),
            last_updated=data.get('last_updated', datetime.now(timezone.utc)),
            change_count=data.get('change_count', 1)
        )
    
    @property
    def key(self) -> str:
        return f"{self.company_id}:{self.site_code}"

class PendingEvaluationsManager:
    """Manages the MongoDB collection of pending badge evaluations"""
    
    def __init__(self, mongo_uri: str = None, db_name: str = None):
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI")
        self.db_name = db_name or os.getenv("MONGO_DB_NAME", "ensogove")
        self.collection_name = "pending_badge_evaluations"
        
        self.client = None
        self.db = None
        self.collection = None
        
        # Statistics
        self.stats = {
            'entries_added': 0,
            'entries_updated': 0,
            'entries_processed': 0,
            'entries_failed': 0,
            'last_batch_run': None,
            'last_batch_size': 0
        }
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            # Create indexes for efficient querying
            await self.collection.create_index([
                ("company_id", 1),
                ("site_code", 1)
            ], unique=True)
            
            await self.collection.create_index("last_updated")
            await self.collection.create_index("priority")
            
            logger.info("Connected to MongoDB for pending evaluations")
            
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    async def add_pending_evaluation(self, company_id: str, site_code: str, 
                                  triggered_by: str = None, badge_ids: List[str] = None,
                                  priority: int = 2) -> bool:
        """
        Add or update a pending evaluation entry
        
        Args:
            company_id: Company identifier
            site_code: Site identifier
            triggered_by: What triggered this evaluation (e.g., 'kpi_data.environmental_score')
            badge_ids: Specific badges to evaluate (if known)
            priority: Evaluation priority
            
        Returns:
            bool: True if entry was added/updated successfully
        """
        try:
            now = datetime.now(timezone.utc)
            
            # Check if entry already exists
            existing = await self.collection.find_one({
                "company_id": company_id,
                "site_code": site_code
            })
            
            if existing:
                # Update existing entry
                update_data = {
                    "last_updated": now,
                    "$inc": {"change_count": 1}
                }
                
                # Add to triggered_by list if not already present
                if triggered_by and triggered_by not in existing.get('triggered_by', []):
                    update_data["$addToSet"] = {"triggered_by": triggered_by}
                
                # Merge badge_ids
                if badge_ids:
                    existing_badges = set(existing.get('badge_ids', []))
                    new_badges = set(badge_ids)
                    all_badges = list(existing_badges.union(new_badges))
                    update_data["badge_ids"] = all_badges
                
                # Update priority to highest (lowest number)
                if priority < existing.get('priority', 999):
                    update_data["priority"] = priority
                
                await self.collection.update_one(
                    {"company_id": company_id, "site_code": site_code},
                    {"$set": update_data}
                )
                
                self.stats['entries_updated'] += 1
                logger.debug(f"Updated pending evaluation: {company_id}:{site_code}")
                
            else:
                # Create new entry
                entry = PendingEvaluationEntry(
                    company_id=company_id,
                    site_code=site_code,
                    triggered_by=[triggered_by] if triggered_by else [],
                    badge_ids=badge_ids or [],
                    priority=priority,
                    first_triggered=now,
                    last_updated=now
                )
                
                await self.collection.insert_one(entry.to_dict())
                self.stats['entries_added'] += 1
                logger.info(f"Added pending evaluation: {company_id}:{site_code}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding pending evaluation: {e}")
            return False
    
    async def get_pending_evaluations(self, limit: int = None, 
                                    priority_threshold: int = None) -> List[PendingEvaluationEntry]:
        """
        Get all pending evaluations, optionally filtered and limited
        
        Args:
            limit: Maximum number of entries to return
            priority_threshold: Only return entries with priority <= threshold
            
        Returns:
            List of pending evaluation entries
        """
        try:
            query = {}
            
            if priority_threshold is not None:
                query["priority"] = {"$lte": priority_threshold}
            
            # Sort by priority (ascending) then by first_triggered (ascending)
            cursor = self.collection.find(query).sort([
                ("priority", 1),
                ("first_triggered", 1)
            ])
            
            if limit:
                cursor = cursor.limit(limit)
            
            entries = []
            async for doc in cursor:
                entries.append(PendingEvaluationEntry.from_dict(doc))
            
            return entries
            
        except Exception as e:
            logger.error(f"Error getting pending evaluations: {e}")
            return []
    
    async def remove_pending_evaluation(self, company_id: str, site_code: str) -> bool:
        """Remove a pending evaluation entry"""
        try:
            result = await self.collection.delete_one({
                "company_id": company_id,
                "site_code": site_code
            })
            
            if result.deleted_count > 0:
                logger.debug(f"Removed pending evaluation: {company_id}:{site_code}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error removing pending evaluation: {e}")
            return False
    
    async def clear_all_pending(self) -> int:
        """Clear all pending evaluations (use with caution)"""
        try:
            result = await self.collection.delete_many({})
            logger.info(f"Cleared {result.deleted_count} pending evaluations")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error clearing pending evaluations: {e}")
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about pending evaluations"""
        try:
            total_pending = await self.collection.count_documents({})
            
            # Get priority distribution
            priority_pipeline = [
                {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            
            priority_dist = {}
            async for doc in self.collection.aggregate(priority_pipeline):
                priority_dist[doc["_id"]] = doc["count"]
            
            # Get oldest pending
            oldest_doc = await self.collection.find_one(
                {}, sort=[("first_triggered", 1)]
            )
            oldest_pending = oldest_doc.get("first_triggered") if oldest_doc else None
            
            return {
                **self.stats,
                'total_pending': total_pending,
                'priority_distribution': priority_dist,
                'oldest_pending': oldest_pending
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return self.stats

class NightlyBatchProcessor:
    """Processes pending badge evaluations in nightly batches"""
    
    def __init__(self, pending_manager: PendingEvaluationsManager = None):
        self.pending_manager = pending_manager or PendingEvaluationsManager()
        
        # Import your existing badge processor
        try:
            import sys
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from aws.lambda_processor import LambdaBadgeProcessor  # Adjust import path
            self.badge_processor_class = LambdaBadgeProcessor
        except ImportError:
            logger.error("Could not import LambdaBadgeProcessor. Please adjust the import path.")
            self.badge_processor_class = None
        
        # Configuration
        self.batch_size = int(os.getenv("NIGHTLY_BATCH_SIZE", "50"))
        self.max_concurrent = int(os.getenv("NIGHTLY_MAX_CONCURRENT", "5"))
        self.nightly_time = os.getenv("NIGHTLY_PROCESSING_TIME", "02:00")  # 2 AM default
        
        self.stats = {
            'last_run': None,
            'last_run_duration': None,
            'total_processed': 0,
            'total_succeeded': 0,
            'total_failed': 0,
            'runs_completed': 0
        }
    
    async def process_nightly_batch(self, max_entries: int = None) -> Dict[str, Any]:
        """
        Process all pending evaluations in a nightly batch
        
        Args:
            max_entries: Maximum number of entries to process (None = all)
            
        Returns:
            Dictionary with processing results
        """
        start_time = datetime.now(timezone.utc)
        logger.info("🌙 Starting nightly badge evaluation batch...")
        
        try:
            # Connect to databases
            await self.pending_manager.connect()
            
            # Get pending evaluations
            pending_entries = await self.pending_manager.get_pending_evaluations(
                limit=max_entries
            )
            
            if not pending_entries:
                logger.info("No pending evaluations to process")
                return {
                    'processed': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'duration_seconds': 0,
                    'message': 'No pending evaluations'
                }
            
            logger.info(f"Processing {len(pending_entries)} pending evaluations...")
            
            # Process in batches
            results = {
                'processed': 0,
                'succeeded': 0, 
                'failed': 0,
                'errors': []
            }
            
            # Create badge processor instance
            if not self.badge_processor_class:
                raise Exception("Badge processor class not available")
            
            badge_processor = self.badge_processor_class(
                shared_db_client=self.pending_manager.client,
                shared_db=self.pending_manager.db
            )
            
            try:
                await badge_processor.connect_to_db()
                
                # Process entries in batches
                for i in range(0, len(pending_entries), self.batch_size):
                    batch = pending_entries[i:i + self.batch_size]
                    batch_results = await self._process_batch(badge_processor, batch)
                    
                    results['processed'] += batch_results['processed']
                    results['succeeded'] += batch_results['succeeded']
                    results['failed'] += batch_results['failed']
                    results['errors'].extend(batch_results['errors'])
                    
                    logger.info(f"Processed batch {i//self.batch_size + 1}: "
                              f"{batch_results['succeeded']} succeeded, "
                              f"{batch_results['failed']} failed")
                    
                    # Small delay between batches to avoid overwhelming the system
                    await asyncio.sleep(1)
                
            finally:
                badge_processor.disconnect_from_db()
            
            # Update statistics
            duration = datetime.now(timezone.utc) - start_time
            
            self.stats.update({
                'last_run': start_time,
                'last_run_duration': duration.total_seconds(),
                'total_processed': self.stats['total_processed'] + results['processed'],
                'total_succeeded': self.stats['total_succeeded'] + results['succeeded'],
                'total_failed': self.stats['total_failed'] + results['failed'],
                'runs_completed': self.stats['runs_completed'] + 1
            })
            
            # Update pending manager stats
            self.pending_manager.stats.update({
                'last_batch_run': start_time,
                'last_batch_size': results['processed'],
                'entries_processed': self.pending_manager.stats['entries_processed'] + results['succeeded'],
                'entries_failed': self.pending_manager.stats['entries_failed'] + results['failed']
            })
            
            results['duration_seconds'] = duration.total_seconds()
            
            logger.info(f"🎉 Nightly batch completed in {duration.total_seconds():.1f}s: "
                       f"{results['succeeded']} succeeded, {results['failed']} failed")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in nightly batch processing: {e}")
            return {
                'processed': 0,
                'succeeded': 0,
                'failed': 0,
                'duration_seconds': 0,
                'error': str(e)
            }
        finally:
            await self.pending_manager.disconnect()
    
    async def _process_batch(self, badge_processor, entries: List[PendingEvaluationEntry]) -> Dict[str, Any]:
        """Process a batch of pending evaluations"""
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Process entries concurrently
        tasks = [
            self._process_single_entry(badge_processor, entry, semaphore)
            for entry in entries
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results
        batch_results = {
            'processed': len(entries),
            'succeeded': 0,
            'failed': 0,
            'errors': []
        }
        
        for i, result in enumerate(results):
            entry = entries[i]
            
            if isinstance(result, Exception):
                batch_results['failed'] += 1
                batch_results['errors'].append(f"{entry.key}: {str(result)}")
                logger.error(f"Failed to process {entry.key}: {result}")
            elif result:
                batch_results['succeeded'] += 1
                # Remove from pending evaluations
                await self.pending_manager.remove_pending_evaluation(
                    entry.company_id, entry.site_code
                )
            else:
                batch_results['failed'] += 1
                batch_results['errors'].append(f"{entry.key}: Processing returned False")
        
        return batch_results
    
    async def _process_single_entry(self, badge_processor, entry: PendingEvaluationEntry, 
                                  semaphore: asyncio.Semaphore) -> bool:
        """Process a single pending evaluation entry"""
        async with semaphore:
            try:
                # Determine which badges to evaluate
                badge_ids = entry.badge_ids
                
                if not badge_ids:
                    # If no specific badges, get all badges for the company/site
                    badge_ids = await self._get_badges_for_company_site(
                        badge_processor, entry.company_id, entry.site_code
                    )
                
                if not badge_ids:
                    logger.warning(f"No badges found for {entry.key}")
                    return False
                
                # Process each badge
                success_count = 0
                for badge_id in badge_ids:
                    try:
                        result = await badge_processor.process_badge_evaluation(
                            company_id=entry.company_id,
                            site_code=entry.site_code,
                            badge_id=badge_id,
                            message_id=f"nightly_batch_{int(datetime.now().timestamp())}"
                        )
                        
                        if result.get("success", False):
                            success_count += 1
                            logger.debug(f"✅ Processed badge {badge_id} for {entry.key}")
                        else:
                            logger.warning(f"❌ Failed badge {badge_id} for {entry.key}: {result.get('error')}")
                    
                    except Exception as e:
                        logger.error(f"Error processing badge {badge_id} for {entry.key}: {e}")
                
                # Consider successful if at least one badge was processed
                success = success_count > 0
                
                if success:
                    logger.info(f"✅ Completed evaluation for {entry.key}: "
                              f"{success_count}/{len(badge_ids)} badges succeeded")
                else:
                    logger.error(f"❌ All badges failed for {entry.key}")
                
                return success
                
            except Exception as e:
                logger.error(f"Exception processing entry {entry.key}: {e}")
                return False
    
    async def _get_badges_for_company_site(self, badge_processor, company_id: str, 
                                         site_code: str) -> List[str]:
        """Get all applicable badges for a company/site"""
        try:
            # This would query your database to find badges that apply to this company/site
            # Adjust the query based on your schema
            
            # Option 1: Get from company document
            company_doc = await badge_processor.db.companies.find_one({
                "company_id": company_id,
                "sites.site_code": site_code
            })
            
            if company_doc:
                site_data = next((site for site in company_doc.get('sites', []) 
                                if site.get('site_code') == site_code), {})
                badge_ids = site_data.get('badge_ids', [])
                
                if badge_ids:
                    return badge_ids
            
            # Option 2: Get all active badges (fallback)
            badges_cursor = badge_processor.db.badges.find({"active": True})
            badge_ids = []
            async for badge in badges_cursor:
                badge_ids.append(badge.get('badge_id'))
            
            return badge_ids[:10]  # Limit to prevent overload
            
        except Exception as e:
            logger.error(f"Error getting badges for {company_id}:{site_code}: {e}")
            return []

# Modified change monitor that adds to pending evaluations instead of queue

class PendingEvaluationChangeMonitor:
    """Change monitor that adds entries to pending evaluations collection"""
    
    def __init__(self, mongo_uri: str = None, db_name: str = None):
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI")
        self.db_name = db_name or os.getenv("MONGO_DB_NAME", "ensogove")
        
        self.client = None
        self.db = None
        self.pending_manager = PendingEvaluationsManager(mongo_uri, db_name)
        
        # Configuration for different collections (similar to before)
        self.watch_configs = {}
        self.running = False
        self.change_streams = {}
        
        # Statistics
        self.stats = {
            'total_changes_processed': 0,
            'evaluations_added': 0,
            'errors': 0,
            'by_collection': {}
        }
        
        self.setup_default_watchers()
    
    def setup_default_watchers(self):
        """Setup default watch configurations"""
        from mongodb_change_monitor import create_kpi_data_watcher, create_company_profile_watcher, create_site_data_watcher, create_compliance_watcher
        
        self.watch_configs = {
            'kpi_data': create_kpi_data_watcher(),
            'companies': create_company_profile_watcher(),
            'sites': create_site_data_watcher(),
            'compliance_records': create_compliance_watcher()
        }
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            await self.pending_manager.connect()
            logger.info("Connected to MongoDB for change monitoring")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
        await self.pending_manager.disconnect()
        logger.info("Disconnected from MongoDB")
    
    async def start_monitoring(self):
        """Start monitoring all configured collections"""
        await self.connect()
        self.running = True
        
        # Start change stream for each configured collection
        tasks = []
        for collection_name, config in self.watch_configs.items():
            if config.enabled:
                task = asyncio.create_task(
                    self._monitor_collection(collection_name, config)
                )
                tasks.append(task)
                self.change_streams[collection_name] = task
        
        logger.info(f"Started monitoring {len(tasks)} collections for pending evaluations")
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            await self.stop_monitoring()
    
    async def stop_monitoring(self):
        """Stop monitoring all collections"""
        self.running = False
        
        for collection_name, task in self.change_streams.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info(f"Cancelled monitoring for {collection_name}")
        
        self.change_streams.clear()
        await self.disconnect()
        logger.info("Stopped MongoDB change monitoring")
    
    async def _monitor_collection(self, collection_name: str, config):
        """Monitor changes for a specific collection"""
        from mongodb_change_monitor import ChangeType
        
        collection = self.db[collection_name]
        
        # Build pipeline for change stream (reuse from original monitor)
        pipeline = []
        if config.change_types:
            operation_types = [ct.value for ct in config.change_types]
            pipeline.append({
                "$match": {
                    "operationType": {"$in": operation_types}
                }
            })
        
        logger.info(f"Starting change stream for collection: {collection_name}")
        
        try:
            async with collection.watch(pipeline, full_document='updateLookup') as stream:
                async for change in stream:
                    if not self.running:
                        break
                    
                    try:
                        await self._process_change(collection_name, config, change)
                    except Exception as e:
                        logger.error(f"Error processing change for {collection_name}: {e}")
                        self.stats['errors'] += 1
                        
        except Exception as e:
            logger.error(f"Error in change stream for {collection_name}: {e}")
            if self.running:
                logger.info(f"Retrying change stream for {collection_name} in 5 seconds...")
                await asyncio.sleep(5)
                if self.running:
                    await self._monitor_collection(collection_name, config)
    
    async def _process_change(self, collection_name: str, config, change: Dict[str, Any]):
        """Process a single change event by adding to pending evaluations"""
        try:
            operation_type = change.get('operationType')
            document_key = change.get('documentKey', {})
            full_document = change.get('fullDocument')
            
            logger.debug(f"Processing {operation_type} change in {collection_name}")
            
            # Extract company_id and site_code
            company_id, site_code = self._extract_identifiers(
                config, full_document, document_key, change
            )
            
            if not company_id or not site_code:
                logger.warning(f"Could not extract company_id/site_code from change in {collection_name}")
                return
            
            # Determine what triggered this and which badges might be affected
            changed_fields = self._get_changed_fields(change)
            triggered_by = f"{collection_name}.{','.join(changed_fields[:3])}"  # Limit length
            
            # Get badge IDs if mapped
            badge_ids = []
            if config.badge_mapping:
                for field in changed_fields:
                    if field in config.badge_mapping:
                        badge_ids.extend(config.badge_mapping[field])
            
            # Remove duplicates
            badge_ids = list(set(badge_ids))
            
            # Add to pending evaluations
            success = await self.pending_manager.add_pending_evaluation(
                company_id=company_id,
                site_code=site_code,
                triggered_by=triggered_by,
                badge_ids=badge_ids,
                priority=config.priority
            )
            
            if success:
                self.stats['evaluations_added'] += 1
                logger.info(f"🔄 Added pending evaluation: {company_id}:{site_code} "
                          f"(triggered by {triggered_by})")
            
            # Update statistics
            self.stats['total_changes_processed'] += 1
            
            if collection_name not in self.stats['by_collection']:
                self.stats['by_collection'][collection_name] = {
                    'changes': 0, 'evaluations_added': 0
                }
            
            self.stats['by_collection'][collection_name]['changes'] += 1
            if success:
                self.stats['by_collection'][collection_name]['evaluations_added'] += 1
            
        except Exception as e:
            logger.error(f"Error processing change: {e}")
            self.stats['errors'] += 1
    
    def _extract_identifiers(self, config, full_document: Dict, 
                           document_key: Dict, change: Dict) -> tuple[Optional[str], Optional[str]]:
        """Extract company_id and site_code from change event"""
        company_id = None
        site_code = None
        
        # Try to get from full document first
        if full_document:
            company_id = str(full_document.get(config.company_id_field, ''))
            site_code = full_document.get(config.site_code_field, '')
        
        # For delete operations, try document key
        if not company_id or not site_code:
            if document_key:
                company_id = company_id or str(document_key.get(config.company_id_field, ''))
                site_code = site_code or document_key.get(config.site_code_field, '')
        
        return company_id if company_id else None, site_code if site_code else None
    
    def _get_changed_fields(self, change: Dict) -> List[str]:
        """Extract list of changed fields from change event"""
        changed_fields = []
        
        operation_type = change.get('operationType')
        
        if operation_type == 'update':
            update_desc = change.get('updateDescription', {})
            
            # Updated fields
            updated_fields = update_desc.get('updatedFields', {})
            changed_fields.extend(updated_fields.keys())
            
            # Removed fields
            removed_fields = update_desc.get('removedFields', [])
            changed_fields.extend(removed_fields)
            
        elif operation_type in ['insert', 'replace']:
            # For inserts/replaces, consider all fields as changed
            full_document = change.get('fullDocument', {})
            changed_fields.extend(full_document.keys())
        
        return changed_fields

# Scheduler for nightly processing

class NightlyScheduler:
    """Handles scheduling of nightly batch processing"""
    
    def __init__(self, processor: NightlyBatchProcessor):
        self.processor = processor
        self.scheduler_thread = None
        self.running = False
        
        # Parse nightly time
        try:
            time_parts = self.processor.nightly_time.split(':')
            self.nightly_hour = int(time_parts[0])
            self.nightly_minute = int(time_parts[1])
        except:
            self.nightly_hour = 2
            self.nightly_minute = 0
            logger.warning(f"Invalid nightly time format, using 02:00")
    
    def start_scheduler(self):
        """Start the nightly scheduler"""
        self.running = True
        
        # Schedule the nightly job
        schedule.every().day.at(f"{self.nightly_hour:02d}:{self.nightly_minute:02d}").do(
            self._run_nightly_job
        )
        
        logger.info(f"📅 Scheduled nightly processing at {self.nightly_hour:02d}:{self.nightly_minute:02d}")
        
        # Start scheduler in background thread
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
    
    def stop_scheduler(self):
        """Stop the nightly scheduler"""
        self.running = False
        schedule.clear()
        logger.info("Stopped nightly scheduler")
    
    def _scheduler_loop(self):
        """Background scheduler loop"""
        while self.running:
            schedule.run_pending()
            # Check every minute
            import time
            time.sleep(60)
    
    def _run_nightly_job(self):
        """Run the nightly batch job"""
        logger.info("🌙 Nightly job triggered by scheduler")
        
        # Run the async batch processing in a new event loop
        # (since we're in a background thread)
        def run_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self.processor.process_nightly_batch())
                logger.info(f"🎉 Nightly job completed: {result}")
            except Exception as e:
                logger.error(f"❌ Nightly job failed: {e}")
            finally:
                loop.close()
        
        # Run in thread pool to avoid blocking scheduler
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(run_async)

# Complete nightly batch system

class CompleteBatchSystem:
    """Complete system combining change monitoring with nightly batch processing"""
    
    def __init__(self, config_file: str = None):
        self.config_file = config_file
        
        # Initialize components
        self.pending_manager = PendingEvaluationsManager()
        self.change_monitor = PendingEvaluationChangeMonitor()
        self.batch_processor = NightlyBatchProcessor(self.pending_manager)
        self.scheduler = NightlyScheduler(self.batch_processor)
        
        self.running = False
    
    async def start(self):
        """Start the complete batch system"""
        logger.info("🚀 Starting Complete Nightly Batch Badge System...")
        
        self.running = True
        
        # Start nightly scheduler
        self.scheduler.start_scheduler()
        
        # Start change monitoring (this will run indefinitely)
        try:
            await self.change_monitor.start_monitoring()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the complete batch system"""
        logger.info("🛑 Stopping Complete Nightly Batch Badge System...")
        
        self.running = False
        
        # Stop components
        self.scheduler.stop_scheduler()
        await self.change_monitor.stop_monitoring()
        
        logger.info("✅ Complete Nightly Batch Badge System stopped")
    
    async def run_manual_batch(self, max_entries: int = None) -> Dict[str, Any]:
        """Manually trigger a batch processing run"""
        logger.info("🔧 Running manual batch processing...")
        return await self.batch_processor.process_nightly_batch(max_entries)
    
    async def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        return {
            'pending_evaluations': await self.pending_manager.get_stats(),
            'change_monitor': self.change_monitor.stats,
            'batch_processor': self.batch_processor.stats,
            'system': {
                'running': self.running,
                'nightly_time': f"{self.scheduler.nightly_hour:02d}:{self.scheduler.nightly_minute:02d}"
            }
        }

if __name__ == "__main__":
    # CLI interface
    import argparse
    
    parser = argparse.ArgumentParser(description='Nightly Batch Badge Evaluation System')
    parser.add_argument('--config', help='Configuration file path')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start the complete system')
    
    # Manual batch command
    batch_parser = subparsers.add_parser('batch', help='Run manual batch processing')
    batch_parser.add_argument('--max-entries', type=int, help='Maximum entries to process')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show system statistics')
    
    # Pending command
    pending_parser = subparsers.add_parser('pending', help='Manage pending evaluations')
    pending_subparsers = pending_parser.add_subparsers(dest='pending_action')
    
    list_parser = pending_subparsers.add_parser('list', help='List pending evaluations')
    list_parser.add_argument('--limit', type=int, default=20, help='Limit results')
    
    add_parser = pending_subparsers.add_parser('add', help='Add pending evaluation')
    add_parser.add_argument('company_id', help='Company ID')
    add_parser.add_argument('site_code', help='Site code')
    add_parser.add_argument('--badges', nargs='*', help='Badge IDs')
    add_parser.add_argument('--priority', type=int, default=2, help='Priority')
    
    clear_parser = pending_subparsers.add_parser('clear', help='Clear all pending')
    clear_parser.add_argument('--confirm', action='store_true', help='Confirm clearing')
    
    args = parser.parse_args()
    
    async def run_cli():
        system = CompleteBatchSystem(args.config)
        
        if args.command == 'start':
            # Setup signal handlers
            def signal_handler(signum, frame):
                logger.info(f"Received signal {signum}")
                system.running = False
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            await system.start()
            
        elif args.command == 'batch':
            result = await system.run_manual_batch(args.max_entries)
            print(f"Batch processing result: {result}")
            
        elif args.command == 'stats':
            stats = await system.get_system_stats()
            print("=== System Statistics ===")
            print(f"Pending evaluations: {stats['pending_evaluations']['total_pending']}")
            print(f"Changes processed: {stats['change_monitor']['total_changes_processed']}")
            print(f"Evaluations added: {stats['change_monitor']['evaluations_added']}")
            print(f"Last batch run: {stats['batch_processor']['last_run']}")
            print(f"Total processed in batches: {stats['batch_processor']['total_processed']}")
            
        elif args.command == 'pending':
            pending_manager = PendingEvaluationsManager()
            await pending_manager.connect()
            
            try:
                if args.pending_action == 'list':
                    entries = await pending_manager.get_pending_evaluations(limit=args.limit)
                    print(f"=== {len(entries)} Pending Evaluations ===")
                    for entry in entries:
                        print(f"{entry.company_id}:{entry.site_code} - "
                              f"Priority: {entry.priority}, Changes: {entry.change_count}, "
                              f"Triggered by: {', '.join(entry.triggered_by)}")
                
                elif args.pending_action == 'add':
                    success = await pending_manager.add_pending_evaluation(
                        company_id=args.company_id,
                        site_code=args.site_code,
                        badge_ids=args.badges or [],
                        priority=args.priority
                    )
                    print(f"Added pending evaluation: {success}")
                
                elif args.pending_action == 'clear':
                    if args.confirm:
                        count = await pending_manager.clear_all_pending()
                        print(f"Cleared {count} pending evaluations")
                    else:
                        print("Use --confirm to actually clear all pending evaluations")
                        
            finally:
                await pending_manager.disconnect()
    
    if not hasattr(args, 'command') or not args.command:
        parser.print_help()
    else:
        asyncio.run(run_cli())