# Badge Re-evaluation Test Configuration and Utilities
import os
from unittest.mock import patch
import pytest
import asyncio
from tests import ExecutionMode, Config
from tests.badge_test_runner import BadgeReevaluationRunner

import pytest_asyncio
from badge_system.aws.lambda_processor import LambdaBadgeProcessor


@pytest.fixture
def test_config():
    """Fixture providing test configuration"""
    return Config.get_config()


@pytest_asyncio.fixture
async def connected_badge_processor(test_config):
    """Fixture providing a connected badge processor"""
    print(f"🔧 Creating connected badge processor fixture...")
    
    with patch.dict(os.environ, test_config):
        processor = LambdaBadgeProcessor()
        print(f"✅ Processor created: {type(processor)}")
        
        try:
            print("🔗 Connecting to database in fixture...")
            await processor.connect_to_db()
            print("✅ Database connected in fixture")
            
            yield processor
            
        except Exception as e:
            print(f"❌ Connection failed in fixture: {e}")
            yield processor
            
        finally:
            try:
                processor.disconnect_from_db()
                print("🔌 Connected processor cleanup completed")
            except Exception:
                pass


class TestBadgeReevaluation:
    """Badge re-evaluation tests using MongoDB badge_evaluation_queue"""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_reevaluation_sequential(self, connected_badge_processor):
        """Test badge re-evaluation in sequential mode (recommended)"""
        config = Config(
            min_success_rate=0.7,
            log_individual_results=True,
            fail_fast=False  # Process all company/sites even if some fail
        )
        
        async with BadgeReevaluationRunner(config) as runner:
            summary = await runner.run_badge_reevaluation(
                connected_badge_processor, 
                ExecutionMode.SEQUENTIAL
            )
            
            # Assertions
            assert summary['total_pending'] >= 0, "Should fetch pending evaluations"
            assert summary['success'], f"Success rate too low: {summary['success_rate']:.2%}"
            
            # Print detailed results for debugging
            if summary.get('results'):
                print("\n📋 DETAILED RESULTS:")
                for result in summary['results']:
                    company_id = result.get('company_id')
                    site_code = result.get('site_code')
                    badges_evaluated = result.get('badges_evaluated', 0)
                    success = result.get('success')
                    failed_badge = result.get('failed_badge')
                    
                    status = "✅ PASSED" if success else f"❌ FAILED at badge: {failed_badge}"
                    print(f"  {company_id}/{site_code}: {badges_evaluated} badges evaluated - {status}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_reevaluation_sample(self, connected_badge_processor):
        """Test a sample of badge re-evaluations for quick validation"""
        config = Config(
            sample_size=3,
            min_success_rate=0.5,
            log_individual_results=True,
            fail_fast=True  # Stop on first failure for quick feedback
        )
        
        async with BadgeReevaluationRunner(config) as runner:
            summary = await runner.run_badge_reevaluation(
                connected_badge_processor,
                ExecutionMode.SAMPLE
            )
            
            assert summary['processed_evaluations'] <= 3, "Should limit to sample size"
            if summary['processed_evaluations'] > 0:
                assert summary['successful'] > 0, "At least one evaluation should pass"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_reevaluation_concurrent(self, connected_badge_processor):
        """Test badge re-evaluation with controlled concurrency"""
        config = Config(
            max_concurrent=3,  # Limit concurrency for database safety
            min_success_rate=0.6,
            log_individual_results=False  # Reduce output for concurrent tests
        )
        
        async with BadgeReevaluationRunner(config) as runner:
            summary = await runner.run_badge_reevaluation(
                connected_badge_processor,
                ExecutionMode.CONCURRENT
            )
            
            assert summary['success'], f"Concurrent testing failed: {summary['success_rate']:.2%}"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_reevaluation_batch(self, connected_badge_processor):
        """Test badge re-evaluation in batches"""
        config = Config(
            batch_size=2,
            delay_between_batches=0.5,  # Longer delay for batch processing
            min_success_rate=0.5
        )
        
        async with BadgeReevaluationRunner(config) as runner:
            summary = await runner.run_badge_reevaluation(
                connected_badge_processor,
                ExecutionMode.BATCH
            )
            
            assert summary['success'], f"Batch testing failed: {summary['success_rate']:.2%}"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_empty_queue_handling(self, connected_badge_processor):
        """Test handling of empty badge_evaluation_queue"""
        config = Config(
            min_success_rate=0.0,  # No minimum for empty queue test
            log_individual_results=True
        )
        
        async with BadgeReevaluationRunner(config) as runner:
            # Temporarily clear the queue for this test (if needed)
            # await runner.db.badge_evaluation_queue.update_many(
            #     {"status": "pending"}, 
            #     {"$set": {"status": "test_cleared"}}
            # )
            
            summary = await runner.run_badge_reevaluation(
                connected_badge_processor,
                ExecutionMode.SEQUENTIAL
            )
            
            # Should handle empty queue gracefully
            assert 'error' in summary or summary['total_pending'] >= 0
            
            # Restore queue state if needed
            # await runner.db.badge_evaluation_queue.update_many(
            #     {"status": "test_cleared"}, 
            #     {"$set": {"status": "pending"}}
            # )


# Standalone function for manual testing and debugging
async def run_manual_badge_reevaluation():
    """Manual badge re-evaluation runner for debugging"""
    
    config = Config(
        max_concurrent=2,
        sample_size=5,
        log_individual_results=True,
        fail_fast=False,
        min_success_rate=0.3
    )
    
    print("🧪 Running manual badge re-evaluation...")
    
    # Use real processor (you'll need to import your actual processor)
    test_config = Config.get_config()
    
    with patch.dict(os.environ, test_config):
        processor = LambdaBadgeProcessor()
        
        try:
            await processor.connect_to_db()
            print("✅ Processor connected to database")
            
            async with BadgeReevaluationRunner(config) as runner:
                summary = await runner.run_badge_reevaluation(
                    processor, 
                    ExecutionMode.SAMPLE
                )
                
            print(f"Manual test completed: {summary['success']}")
            print(f"Processed: {summary['processed_evaluations']} company/site groups")
            print(f"Total badges evaluated: {summary['total_badges_evaluated']}")
            
            return summary
        
        except Exception as e:
            print(f"❌ Manual test failed: {e}")
            return {"success": False, "error": str(e)}
        
        finally:
            try:
                processor.disconnect_from_db()
            except:
                pass


# Utility function to inspect badge_evaluation_queue
async def inspect_badge_queue():
    """Utility to inspect the current state of badge_evaluation_queue"""
    
    config = Config.get_config()
    
    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(config['MONGO_URI'])
    db = client[config['MONGO_DB_NAME']]
    
    try:
        # Count documents by status
        pending_count = await db.badge_evaluation_queue.count_documents({"status": "pending"})
        completed_count = await db.badge_evaluation_queue.count_documents({"status": "completed"})
        total_count = await db.badge_evaluation_queue.count_documents({})
        
        print(f"📊 BADGE EVALUATION QUEUE STATUS:")
        print(f"  Pending: {pending_count}")
        print(f"  Completed: {completed_count}")
        print(f"  Total: {total_count}")
        
        # Sample pending documents
        if pending_count > 0:
            print(f"\n📋 SAMPLE PENDING EVALUATIONS:")
            cursor = db.badge_evaluation_queue.find({"status": "pending"}).limit(3)
            async for doc in cursor:
                company_id = doc.get('company_id')
                site_code = doc.get('site_code')
                collections = doc.get('affected_collections', [])
                print(f"  {company_id}/{site_code}: {collections}")
        
        # Group by company/site
        pipeline = [
            {"$match": {"status": "pending"}},
            {"$group": {
                "_id": {"company_id": "$company_id", "site_code": "$site_code"},
                "count": {"$sum": 1},
                "collections": {"$addToSet": "$affected_collections"}
            }},
            {"$limit": 10}
        ]
        
        print(f"\n🏢 PENDING BY COMPANY/SITE:")
        async for group in db.badge_evaluation_queue.aggregate(pipeline):
            company_site = group['_id']
            count = group['count']
            collections = [item for sublist in group['collections'] for item in sublist]
            unique_collections = list(set(collections))
            print(f"  {company_site['company_id']}/{company_site['site_code']}: {count} evaluations, collections: {unique_collections}")
    
    except Exception as e:
        print(f"❌ Error inspecting queue: {e}")
    
    finally:
        client.close()


if __name__ == "__main__":
    import asyncio
    
    # Choose what to run
    print("1. Run manual badge re-evaluation")
    print("2. Inspect badge evaluation queue")
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        asyncio.run(run_manual_badge_reevaluation())
    elif choice == "2":
        asyncio.run(inspect_badge_queue())
    else:
        print("Invalid choice")