import os
from enum import Enum
from dataclasses import dataclass
from typing import Dict

class ExecutionMode(Enum):
    """Different test execution modes"""
    FULL = "full"           # Test all pairs
    SAMPLE = "sample"       # Test a sample of pairs
    BATCH = "batch"         # Test in batches
    CONCURRENT = "concurrent" # Test with high concurrency
    SEQUENTIAL = "sequential" # Test one by one

@dataclass
class Config:
    """Configuration for badge processing tests"""
    
    @classmethod
    def get_config(cls) -> Dict[str, str]:
        """Get test configuration from environment variables"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
            
        return {
            'MONGO_URI': os.getenv(
                'TEST_MONGO_URI', 
                'mongodb://localhost:27017/?directConnection=true&serverSelectionTimeoutMS=2000'
            ),
            'MONGO_DB_NAME': os.getenv('TEST_MONGO_DB_NAME', 'ensogove'),
            'BASE_URL': os.getenv('TEST_BASE_URL', 'https://devapi.spectreco.com'),
            'REGION_API_URL': os.getenv('TEST_REGION_API_URL', 'https://dev-region.spectreco.com'),
            'TOKEN': os.getenv('TEST_TOKEN', 'test_token_placeholder')
        }
    api_url: str = "http://localhost:8000/api/allcompanies/sites"
    max_concurrent: int = 5
    batch_size: int = 3
    sample_size: int = 5
    delay_between_requests: float = 0.1
    delay_between_batches: float = 0.5
    timeout_per_request: int = 30
    badge_id: str = "badge_bronze_001"
    retry_count: int = 2
    
    # Performance settings
    enable_performance_tracking: bool = True
    log_individual_results: bool = True
    
    # Error handling
    fail_fast: bool = False  # Stop on first error
    min_success_rate: float = 0.5  # Minimum required success rate
   