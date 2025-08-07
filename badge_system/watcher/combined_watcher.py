#!/usr/bin/env python3
"""
Enhanced Combined Database Watcher with Badge Evaluation
Runs both MongoDB and SQL watchers simultaneously with automatic badge evaluation entries
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from dotenv import load_dotenv

# Import your enhanced watcher classes
# Make sure these files are available in your project
try:
    from mongo_watcher import MongoDBPollingWatcher
    MONGO_AVAILABLE = True
    print("✅ Enhanced MongoDB watcher imported successfully")
except ImportError as e:
    MONGO_AVAILABLE = False
    print(f"⚠️  Enhanced MongoDB watcher not available: {e}")

try:
    from sql_watcher import SQLDatabaseWatcher  
    SQL_AVAILABLE = True
    print("✅ Enhanced SQL watcher imported successfully")
except ImportError as e:
    SQL_AVAILABLE = False
    print(f"⚠️  Enhanced SQL watcher not available: {e}")

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EnhancedCombinedWatcherConfig:
    """Configuration for the enhanced combined database watcher with badge evaluation"""
    # MongoDB configuration
    mongo_enabled: bool = False
    mongo_uri: str = None
    mongo_db_name: str = None
    mongo_collections: List[Dict[str, str]] = None
    
    # SQL configuration
    sql_enabled: bool = False
    sql_host: str = None
    sql_user: str = None
    sql_password: str = None
    sql_database: str = None
    sql_port: int = None
    sql_tables: List[Dict[str, str]] = None
    
    # Badge evaluation configuration
    badge_evaluation_enabled: bool = True
    badge_collection_name: str = "badge_evaluation_queue"
    
    # Common configuration
    poll_interval: int = 30
    batch_size: int = 1000

class EnhancedCombinedDatabaseWatcher:
    """Enhanced combined watcher that runs both MongoDB and SQL watchers with badge evaluation"""
    
    def __init__(self, config: EnhancedCombinedWatcherConfig = None):
        self.config = config or EnhancedCombinedWatcherConfig()
        
        # Individual watchers
        self.mongo_watcher = None
        self.sql_watcher = None
        
        # Combined state
        self.running = False
        self.change_handlers = []
        self.running_tasks = []
        
        # Combined statistics
        self.combined_stats = {
            'total_changes': 0,
            'changes_by_source': {'mongodb': 0, 'sql': 0},
            'changes_by_collection_table': {},
            'changes_by_operation': {},
            'successful_extractions': 0,
            'failed_extractions': 0,
            'badge_entries_created': 0,
            'badge_entries_updated': 0,
            'start_time': None
        }
    
    def configure_mongodb(self, mongo_uri: str = None, db_name: str = None, 
                         collections: List[Dict[str, str]] = None):
        """Configure MongoDB watcher"""
        if not MONGO_AVAILABLE:
            logger.error("❌ Enhanced MongoDB watcher not available - check import")
            return False
            
        self.config.mongo_enabled = True
        self.config.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.config.mongo_db_name = db_name or os.getenv("MONGO_DB_NAME", "ensogove")
        self.config.mongo_collections = collections or []
        
        logger.info(f"🍃 Configured MongoDB: {self.config.mongo_db_name} with {len(self.config.mongo_collections)} collections")
        return True
    
    def configure_sql(self, host: str = None, user: str = None, password: str = None,
                     database: str = None, port: int = None, tables: List[Dict[str, str]] = None):
        """Configure SQL watcher"""
        if not SQL_AVAILABLE:
            logger.error("❌ Enhanced SQL watcher not available - check import")
            return False
            
        self.config.sql_enabled = True
        self.config.sql_host = host or os.getenv("SQL_HOST", "127.0.0.1")
        self.config.sql_user = user or os.getenv("SQL_USER", "root")
        self.config.sql_password = password or os.getenv("SQL_PASSWORD", "")
        self.config.sql_database = database or os.getenv("SQL_DATABASE", "ensogove")
        self.config.sql_port = port or int(os.getenv("SQL_PORT", "3306"))
        self.config.sql_tables = tables or []
        
        logger.info(f"🗄️ Configured SQL: {self.config.sql_database} with {len(self.config.sql_tables)} tables")
        return True
    
    def configure_badge_evaluation(self, enabled: bool = True, collection_name: str = "badge_evaluation_queue"):
        """Configure badge evaluation settings"""
        self.config.badge_evaluation_enabled = enabled
        self.config.badge_collection_name = collection_name
        
        if enabled:
            logger.info(f"🏆 Badge evaluation enabled - collection: {collection_name}")
        else:
            logger.info("❌ Badge evaluation disabled")
    
    def add_change_handler(self, handler: Callable):
        """Add a unified change handler for both MongoDB and SQL changes"""
        self.change_handlers.append(handler)
        logger.info(f"🔧 Added combined change handler: {handler.__name__}")
    
    def set_poll_interval(self, seconds: int):
        """Set polling interval for both watchers"""
        self.config.poll_interval = seconds
        logger.info(f"⏰ Set polling interval to {seconds} seconds for both watchers")
    
    async def _initialize_watchers(self):
        """Initialize individual watchers based on configuration"""
        try:
            # Initialize MongoDB watcher if enabled
            if self.config.mongo_enabled and self.config.mongo_collections and MONGO_AVAILABLE:
                logger.info("🍃 Initializing enhanced MongoDB watcher...")
                self.mongo_watcher = MongoDBPollingWatcher(
                    mongo_uri=self.config.mongo_uri,
                    db_name=self.config.mongo_db_name
                )
                self.mongo_watcher.watch_collections(self.config.mongo_collections)
                self.mongo_watcher.set_poll_interval(self.config.poll_interval)
                
                # Configure badge evaluation
                if self.config.badge_evaluation_enabled:
                    self.mongo_watcher.enable_badge_evaluation(self.config.badge_collection_name)
                else:
                    self.mongo_watcher.disable_badge_evaluation()
                
                self.mongo_watcher.add_change_handler(self._mongo_change_wrapper)
                logger.info("✅ Enhanced MongoDB watcher initialized")
            
            # Initialize SQL watcher if enabled
            if self.config.sql_enabled and self.config.sql_tables and SQL_AVAILABLE:
                logger.info("🗄️ Initializing enhanced SQL watcher...")
                self.sql_watcher = SQLDatabaseWatcher(
                    host=self.config.sql_host,
                    user=self.config.sql_user,
                    password=self.config.sql_password,
                    database=self.config.sql_database,
                    port=self.config.sql_port
                )
                self.sql_watcher.watch_tables(self.config.sql_tables)
                self.sql_watcher.set_poll_interval(self.config.poll_interval)
                
                # Configure badge evaluation
                if self.config.badge_evaluation_enabled:
                    self.sql_watcher.enable_badge_evaluation(
                        self.config.badge_collection_name,
                        self.config.mongo_uri,
                        self.config.mongo_db_name
                    )
                else:
                    self.sql_watcher.disable_badge_evaluation()
                
                self.sql_watcher.add_change_handler(self._sql_change_wrapper)
                logger.info("✅ Enhanced SQL watcher initialized")
                
        except Exception as e:
            logger.error(f"❌ Error initializing watchers: {e}")
            raise
    
    async def _mongo_change_wrapper(self, change_info: Dict[str, Any]):
        """Wrapper for MongoDB changes to add source information and update stats"""
        # Add source information
        change_info['source'] = 'mongodb'
        change_info['source_type'] = 'collection'
        change_info['source_name'] = change_info['collection_name']
        
        # Update combined statistics
        self._update_stats('mongodb', change_info)
        
        # Call all registered handlers
        await self._call_handlers(change_info)
    
    async def _sql_change_wrapper(self, change_info: Dict[str, Any]):
        """Wrapper for SQL changes to add source information and update stats"""
        # Add source information
        change_info['source'] = 'sql'
        change_info['source_type'] = 'table'
        change_info['source_name'] = change_info['table_name']
        
        # Update combined statistics
        self._update_stats('sql', change_info)
        
        # Call all registered handlers
        await self._call_handlers(change_info)
    
    def _update_stats(self, source: str, change_info: Dict[str, Any]):
        """Update combined statistics"""
        self.combined_stats['total_changes'] += 1
        self.combined_stats['changes_by_source'][source] += 1
        
        source_name = change_info['source_name']
        if source_name not in self.combined_stats['changes_by_collection_table']:
            self.combined_stats['changes_by_collection_table'][source_name] = 0
        self.combined_stats['changes_by_collection_table'][source_name] += 1
        
        operation_type = change_info.get('operation_type', 'unknown')
        if operation_type not in self.combined_stats['changes_by_operation']:
            self.combined_stats['changes_by_operation'][operation_type] = 0
        self.combined_stats['changes_by_operation'][operation_type] += 1
        
        if change_info.get('company_id') or change_info.get('site_code'):
            self.combined_stats['successful_extractions'] += 1
        else:
            self.combined_stats['failed_extractions'] += 1
    
    async def _call_handlers(self, change_info: Dict[str, Any]):
        """Call all registered change handlers"""
        for handler in self.change_handlers:
            try:
                await handler(change_info)
            except Exception as e:
                logger.error(f"❌ Error in combined change handler {handler.__name__}: {e}")
    
    async def _run_mongo_watcher(self):
        """Run MongoDB watcher in background"""
        if not self.mongo_watcher:
            return
            
        try:
            logger.info("🍃 Starting enhanced MongoDB watcher...")
            await self.mongo_watcher.start_watching()
        except Exception as e:
            logger.error(f"❌ Error in MongoDB watcher: {e}")
    
    async def _run_sql_watcher(self):
        """Run SQL watcher in background"""
        if not self.sql_watcher:
            return
            
        try:
            logger.info("🗄️ Starting enhanced SQL watcher...")
            await self.sql_watcher.start_watching()
        except Exception as e:
            logger.error(f"❌ Error in SQL watcher: {e}")
    
    async def _status_reporter(self):
        """Periodically report combined status including badge evaluation stats"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Report every 5 minutes
                
                if self.running:
                    await self._log_detailed_status()
                    
            except Exception as e:
                logger.error(f"❌ Error in status reporter: {e}")
                await asyncio.sleep(60)
    
    async def _log_detailed_status(self):
        """Log detailed combined status including badge evaluation"""
        mongo_changes = self.combined_stats['changes_by_source']['mongodb']
        sql_changes = self.combined_stats['changes_by_source']['sql']
        total_changes = self.combined_stats['total_changes']
        success_rate = (
            self.combined_stats['successful_extractions'] / max(1, total_changes) * 100
            if total_changes > 0 else 0
        )
        
        logger.info(f"📊 Combined Status: {total_changes} total changes "
                   f"(MongoDB: {mongo_changes}, SQL: {sql_changes}) - "
                   f"Success rate: {success_rate:.1f}%")
        
        # Update badge evaluation stats from individual watchers
        await self._update_combined_badge_stats()
        
        # Log badge evaluation status
        if self.config.badge_evaluation_enabled:
            total_badge_created = self.combined_stats['badge_entries_created']
            total_badge_updated = self.combined_stats['badge_entries_updated']
            logger.info(f"🏆 Badge Evaluation: {total_badge_created} created, "
                       f"{total_badge_updated} updated this session")
        
        # Log individual watcher stats if available
        if self.mongo_watcher:
            mongo_stats = self.mongo_watcher.get_stats()
            logger.info(f"🍃 MongoDB: {mongo_stats['polling_cycles']} cycles, "
                       f"{mongo_stats['total_changes']} changes")
        
        if self.sql_watcher:
            sql_stats = self.sql_watcher.get_stats()
            logger.info(f"🗄️ SQL: {sql_stats['polling_cycles']} cycles, "
                       f"{sql_stats['total_changes']} changes")
    
    async def _update_combined_badge_stats(self):
        """Update combined badge statistics from individual watchers"""
        total_created = 0
        total_updated = 0
        
        if self.mongo_watcher:
            mongo_stats = self.mongo_watcher.get_stats()
            total_created += mongo_stats.get('badge_entries_created', 0)
            total_updated += mongo_stats.get('badge_entries_updated', 0)
        
        if self.sql_watcher:
            sql_stats = self.sql_watcher.get_stats()
            total_created += sql_stats.get('badge_entries_created', 0)
            total_updated += sql_stats.get('badge_entries_updated', 0)
        
        self.combined_stats['badge_entries_created'] = total_created
        self.combined_stats['badge_entries_updated'] = total_updated
    
    async def start_watching(self):
        """Start watching both databases simultaneously"""
        if not self.config.mongo_enabled and not self.config.sql_enabled:
            logger.error("❌ No databases configured to watch")
            return
        
        self.running = True
        self.combined_stats['start_time'] = datetime.now(timezone.utc)
        
        logger.info("🚀 Starting Enhanced Combined Database Watcher...")
        logger.info(f"   MongoDB enabled: {self.config.mongo_enabled}")
        logger.info(f"   SQL enabled: {self.config.sql_enabled}")
        logger.info(f"   Badge evaluation: {'enabled' if self.config.badge_evaluation_enabled else 'disabled'}")
        logger.info(f"   Poll interval: {self.config.poll_interval} seconds")
        
        try:
            # Initialize watchers
            await self._initialize_watchers()
            
            # Create tasks for each enabled watcher
            tasks = []
            
            if self.config.mongo_enabled and self.mongo_watcher:
                mongo_task = asyncio.create_task(self._run_mongo_watcher())
                tasks.append(('MongoDB', mongo_task))
                self.running_tasks.append(mongo_task)
            
            if self.config.sql_enabled and self.sql_watcher:
                sql_task = asyncio.create_task(self._run_sql_watcher())
                tasks.append(('SQL', sql_task))
                self.running_tasks.append(sql_task)
            
            # Create status reporter task
            status_task = asyncio.create_task(self._status_reporter())
            tasks.append(('Status', status_task))
            self.running_tasks.append(status_task)
            
            logger.info(f"✅ Started {len(tasks)} watcher tasks")
            
            # Wait for all tasks (they should run indefinitely)
            if tasks:
                try:
                    # Wait for any task to complete (shouldn't happen unless there's an error)
                    done, pending = await asyncio.wait(
                        [task for _, task in tasks],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # If we get here, one of the tasks completed unexpectedly
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            logger.error(f"❌ Task completed with error: {e}")
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Received shutdown signal")
                finally:
                    await self.stop_watching()
            else:
                logger.error("❌ No watcher tasks to run")
                
        except Exception as e:
            logger.error(f"❌ Error starting enhanced combined watcher: {e}")
            await self.stop_watching()
            raise
    
    async def stop_watching(self):
        """Stop all watchers gracefully"""
        logger.info("🛑 Stopping Enhanced Combined Database Watcher...")
        
        self.running = False
        
        # Cancel all running tasks
        for task in self.running_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"❌ Error stopping task: {e}")
        
        # Stop individual watchers
        if self.mongo_watcher:
            try:
                await self.mongo_watcher.stop_watching()
            except Exception as e:
                logger.error(f"❌ Error stopping MongoDB watcher: {e}")
        
        if self.sql_watcher:
            try:
                await self.sql_watcher.stop_watching()
            except Exception as e:
                logger.error(f"❌ Error stopping SQL watcher: {e}")
        
        self.running_tasks.clear()
        logger.info("✅ Enhanced Combined Database Watcher stopped")
    
    async def get_badge_evaluation_summary(self) -> Dict[str, Any]:
        """Get combined badge evaluation statistics from both watchers"""
        if not self.config.badge_evaluation_enabled:
            return {"enabled": False}
        
        combined_stats = {
            "enabled": True,
            "collection_name": self.config.badge_collection_name,
            "total_entries": 0,
            "recent_entries_1h": 0,
            "status_counts": {},
            "entries_created_this_session": 0,
            "entries_updated_this_session": 0,
            "sources": {}
        }
        
        try:
            # Get stats from MongoDB watcher
            if self.mongo_watcher:
                mongo_badge_stats = await self.mongo_watcher.get_badge_evaluation_stats()
                if mongo_badge_stats.get("enabled"):
                    combined_stats["sources"]["mongodb"] = mongo_badge_stats
                    combined_stats["entries_created_this_session"] += mongo_badge_stats.get("entries_created_this_session", 0)
                    combined_stats["entries_updated_this_session"] += mongo_badge_stats.get("entries_updated_this_session", 0)
            
            # Get stats from SQL watcher
            if self.sql_watcher:
                sql_badge_stats = await self.sql_watcher.get_badge_evaluation_stats()
                if sql_badge_stats.get("enabled"):
                    combined_stats["sources"]["sql"] = sql_badge_stats
                    # Don't double count - these stats come from the same MongoDB collection
                    if "mongodb" not in combined_stats["sources"]:
                        combined_stats["total_entries"] = sql_badge_stats.get("total_entries", 0)
                        combined_stats["recent_entries_1h"] = sql_badge_stats.get("recent_entries_1h", 0)
                        combined_stats["status_counts"] = sql_badge_stats.get("status_counts", {})
            
            # If we have MongoDB stats, use those for the totals (since both write to same collection)
            if "mongodb" in combined_stats["sources"]:
                mongo_stats = combined_stats["sources"]["mongodb"]
                combined_stats["total_entries"] = mongo_stats.get("total_entries", 0)
                combined_stats["recent_entries_1h"] = mongo_stats.get("recent_entries_1h", 0)
                combined_stats["status_counts"] = mongo_stats.get("status_counts", {})
            
            return combined_stats
            
        except Exception as e:
            logger.error(f"❌ Error getting combined badge evaluation stats: {e}")
            return {"enabled": True, "error": str(e)}
    
    def get_combined_stats(self) -> Dict[str, Any]:
        """Get combined statistics from both watchers"""
        uptime = None
        if self.combined_stats['start_time']:
            uptime = (datetime.now(timezone.utc) - self.combined_stats['start_time']).total_seconds()
        
        stats = {
            **self.combined_stats,
            'uptime_seconds': uptime,
            'running': self.running,
            'mongodb_enabled': self.config.mongo_enabled,
            'sql_enabled': self.config.sql_enabled,
            'badge_evaluation_enabled': self.config.badge_evaluation_enabled,
            'badge_collection_name': self.config.badge_collection_name,
            'poll_interval': self.config.poll_interval,
            'extraction_success_rate': (
                self.combined_stats['successful_extractions'] / 
                max(1, self.combined_stats['total_changes']) * 100
                if self.combined_stats['total_changes'] > 0 else 0
            )
        }
        
        # Add individual watcher stats if available
        if self.mongo_watcher:
            stats['mongo_watcher_stats'] = self.mongo_watcher.get_stats()
        
        if self.sql_watcher:
            stats['sql_watcher_stats'] = self.sql_watcher.get_stats()
        
        return stats

# Example change handler with badge evaluation awareness
async def enhanced_combined_change_handler(change_info: Dict[str, Any]):
    """Enhanced example handler for combined database changes with badge evaluation info"""
    source = change_info.get('source', 'unknown')
    source_name = change_info.get('source_name', 'unknown')
    
    print(f"🔄 ENHANCED COMBINED CHANGE - {source.upper()}:")
    print(f"   Source: {source_name} ({change_info.get('source_type', 'unknown')})")
    print(f"   Operation: {change_info.get('operation_type', 'unknown')}")
    print(f"   Company ID: {change_info.get('company_id', 'N/A')}")
    print(f"   Site Code: {change_info.get('site_code', 'N/A')}")
    print(f"   Timestamp: {change_info.get('timestamp', 'N/A')}")
    
    if source == 'mongodb':
        print(f"   Collection: {change_info.get('collection_name', 'N/A')}")
        print(f"   Document ID: {change_info.get('change_id', 'N/A')}")
    elif source == 'sql':
        print(f"   Table: {change_info.get('table_name', 'N/A')}")
        print(f"   Primary Key: {change_info.get('primary_key', 'N/A')}")
    
    # Badge evaluation info
    company_id = change_info.get('company_id')
    site_code = change_info.get('site_code', '')
    if company_id:
        print(f"🏆 Badge evaluation entry created/updated for: {company_id}:{site_code}")
    
    print("-" * 70)

async def main():
    """Main function to demonstrate enhanced combined database watching with badge evaluation"""
    print("🚀 Enhanced Combined Database Watcher with Badge Evaluation")
    print("=" * 70)
    
    # Create configuration
    config = EnhancedCombinedWatcherConfig(
        poll_interval=20,  # Poll every 20 seconds
        batch_size=1000,
        badge_evaluation_enabled=True,
        badge_collection_name="badge_evaluation_queue"
    )
    
    # Create enhanced combined watcher
    watcher = EnhancedCombinedDatabaseWatcher(config)
    
    # =============================================================================
    # CONFIGURATION SECTION - ENABLE THE DATABASES YOU WANT TO WATCH
    # =============================================================================
    
    # MongoDB Configuration - ENABLED
    if MONGO_AVAILABLE:
        mongo_collections = [
            {
                'collection_name': 'company_goals',           # Your MongoDB collection name
                'timestamp_field': 'updatedAt',       # Your timestamp field (or None for ObjectId)
                'company_id_field': 'company_id',   # Your company ID field
                'site_code_field': 'site_code'        # Your site code field
            },
            # Add more collections here if needed
        ]
        
        success = watcher.configure_mongodb(collections=mongo_collections)
        if success:
            print("✅ Enhanced MongoDB watcher configured successfully")
        else:
            print("❌ Enhanced MongoDB watcher configuration failed")
    
    # SQL Configuration - ENABLED  
    if SQL_AVAILABLE:
        sql_tables = [
            {
                'table_name': 'companies',           # Your SQL table name
                'timestamp_field': 'updated_at',     # Your timestamp field (or None for ID-based)
                'id_field': 'id',                    # Your primary key field
                'company_id_field': 'id',            # Your company ID field
                'site_code_field': None              # Your site code field (None if not available)
            },
            # Add more tables here if needed
        ]
        
        success = watcher.configure_sql(tables=sql_tables)
        if success:
            print("✅ Enhanced SQL watcher configured successfully")
        else:
            print("❌ Enhanced SQL watcher configuration failed")
    
    # Badge Evaluation Configuration
    watcher.configure_badge_evaluation(enabled=True, collection_name="badge_evaluation_queue")
    
    # =============================================================================
    
    # Add change handler
    watcher.add_change_handler(enhanced_combined_change_handler)
    
    # Check if any databases are configured
    if not watcher.config.mongo_enabled and not watcher.config.sql_enabled:
        print("\n⚠️  No databases could be configured!")
        print("🔧 Please check:")
        print("   1. Your enhanced watcher files are available")
        print("   2. Your database connection settings in .env file")
        print("   3. Your collection/table names are correct")
        return
    
    try:
        print(f"\n🔧 Starting enhanced combined watcher...")
        print("💡 Make changes to your configured databases to see them detected!")
        print("💡 Badge evaluation entries will be created automatically!")
        print("💡 Both databases will be monitored simultaneously!")
        print("💡 Press Ctrl+C to stop")
        print("-" * 70)
        
        # Start watching (this will run until interrupted)
        await watcher.start_watching()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping enhanced combined watcher...")
        
        # Show final statistics
        stats = watcher.get_combined_stats()
        print("\n📊 Final Combined Statistics:")
        print(f"Total changes processed: {stats['total_changes']}")
        print(f"MongoDB changes: {stats['changes_by_source']['mongodb']}")
        print(f"SQL changes: {stats['changes_by_source']['sql']}")
        print(f"Successful extractions: {stats['successful_extractions']}")
        print(f"Failed extractions: {stats['failed_extractions']}")
        print(f"Success rate: {stats['extraction_success_rate']:.1f}%")
        print(f"Changes by source: {stats['changes_by_collection_table']}")
        
        # Show badge evaluation summary
        if watcher.config.badge_evaluation_enabled:
            try:
                badge_summary = await watcher.get_badge_evaluation_summary()
                print(f"\n🏆 Badge Evaluation Summary:")
                print(f"Total entries in queue: {badge_summary.get('total_entries', 0)}")
                print(f"Recent entries (1h): {badge_summary.get('recent_entries_1h', 0)}")
                print(f"Created this session: {badge_summary.get('entries_created_this_session', 0)}")
                print(f"Updated this session: {badge_summary.get('entries_updated_this_session', 0)}")
                print(f"Status counts: {badge_summary.get('status_counts', {})}")
            except Exception as e:
                print(f"❌ Error getting badge evaluation summary: {e}")
        
        if 'mongo_watcher_stats' in stats:
            mongo_stats = stats['mongo_watcher_stats']
            print(f"\n🍃 MongoDB Stats: {mongo_stats['polling_cycles']} cycles")
        
        if 'sql_watcher_stats' in stats:
            sql_stats = stats['sql_watcher_stats']
            print(f"🗄️ SQL Stats: {sql_stats['polling_cycles']} cycles")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())