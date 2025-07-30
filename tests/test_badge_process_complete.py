# Test Configuration and Utilities for Efficient Badge Processing Tests
import os
from unittest.mock import patch
import pytest
import asyncio
from tests import ExecutionMode, Config, DynamicBadgeTestRunner, rich_loader

import pytest_asyncio

from badge_system.aws.lambda_processor import LambdaBadgeProcessor
  
@pytest.fixture
def test_config():
    """Fixture providing test configuration"""
    return Config.get_config()

@pytest.fixture
def badge_processor(test_config):
    """Fixture providing a configured badge processor (SYNC - NO ASYNC HERE)"""
    print(f"🔧 Creating badge processor fixture...")
    
    with patch.dict(os.environ, test_config):
        processor = LambdaBadgeProcessor()
        print(f"✅ Badge processor created: {type(processor)}")
        yield processor
        
        # Cleanup
        try:
            processor.disconnect_from_db()
            print("🔌 Badge processor cleanup completed")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")

@pytest_asyncio.fixture
async def connected_badge_processor(test_config):
    """Fixture providing a connected badge processor (PROPER ASYNC FIXTURE)"""
    print(f"🔧 Creating connected badge processor fixture...")
    
    with patch.dict(os.environ, test_config):
        processor = LambdaBadgeProcessor()
        print(f"✅ Processor created: {type(processor)}")
        
        try:
            print("🔗 Connecting to database in fixture...")
            await processor.connect_to_db()
            print("✅ Database connected in fixture")
            
            # THIS IS THE KEY - yield the actual processor object
            yield processor
            
        except Exception as e:
            print(f"❌ Connection failed in fixture: {e}")
            # Still yield the processor so tests can handle the error
            yield processor
            
        finally:
            # Cleanup
            try:
                processor.disconnect_from_db()
                print("🔌 Connected processor cleanup completed")
            except Exception:
                pass

# Integration with pytest
class TestBadgeProcessorEfficient:
    """Efficient badge processor tests using the runner"""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_processing_concurrent(self, connected_badge_processor):
        """Test with high concurrency for speed"""
        config = Config(
            max_concurrent=10,
            min_success_rate=0.6,
            log_individual_results=False
        )
        
        async with DynamicBadgeTestRunner(config) as runner:
            async with rich_loader("Running SAMPLE badge processing..."):
                summary = await runner.run_tests(
                    connected_badge_processor,
                    ExecutionMode.CONCURRENT
                )
            assert summary['success'], f"Success rate too low: {summary['success_rate']:.2%}"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_processing_sample(self, connected_badge_processor):
        """Test a sample for quick validation"""
        config = Config(
            sample_size=3,
            min_success_rate=0.5,
            log_individual_results=True
        )
        
        async with DynamicBadgeTestRunner(config) as runner:
            summary = await runner.run_tests(
                connected_badge_processor,
                ExecutionMode.SAMPLE
            )
            assert summary['successful'] > 0, "At least one test should pass"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_badge_processing_batch(self, connected_badge_processor):
        """Test in batches for controlled resource usage"""
        config = Config(
            batch_size=2,
            delay_between_batches=0.3,
            min_success_rate=0.4
        )
        
        async with DynamicBadgeTestRunner(config) as runner:
            summary = await runner.run_tests(
                connected_badge_processor,
                ExecutionMode.BATCH
            )
            assert summary['success'], f"Batch testing failed: {summary['success_rate']:.2%}"

# Standalone function for manual testing
async def run_manual_test():
    """Manual test runner you can execute independently"""
    # You would import your processor here
    # from your_module import connected_badge_processor
    
    config = Config(
        max_concurrent=3,
        sample_size=5,
        log_individual_results=True
    )
    
    print("🧪 Running manual badge processing tests...")
    
    # Mock processor for demonstration
    class MockProcessor:
        async def process_badge_evaluation(self, **kwargs):
            await asyncio.sleep(0.1)  # Simulate processing time
            return {"success": True, "mock": True}
    
    processor = MockProcessor()
    
    async with DynamicBadgeTestRunner(config) as runner:
        summary = await runner.run_tests(processor, ExecutionMode.SAMPLE)
        
    print(f"Manual test completed: {summary['success']}")
    return summary

if __name__ == "__main__":
    asyncio.run(run_manual_test())