from typing import List, Dict, Any, Optional
from tests import ExecutionMode, Config
import aiohttp
import time
import asyncio


class DynamicBadgeTestRunner:
    """Efficient test runner for dynamic company-site badge processing"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout_per_request)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def _print_summary(self, summary: Dict[str, Any]):
        """Print formatted test summary"""
        print(f"\n📊 TEST EXECUTION SUMMARY")
        print(f"{'='*50}")
        print(f"Mode: {summary['mode'].upper()}")
        print(f"Total pairs available: {summary['total_pairs']}")
        print(f"Pairs tested: {summary['tested_pairs']}")
        print(f"Successful: {summary['successful']}")
        print(f"Failed: {summary['failed']}")
        print(f"Success rate: {summary['success_rate']:.2%}")
        print(f"Total execution time: {summary['execution_time']:.2f}s")
        print(f"Average time per test: {summary['avg_time_per_test']:.3f}s")
        print(f"Overall result: {'✅ PASSED' if summary['success'] else '❌ FAILED'}")
    
    async def fetch_company_site_pairs(self) -> List[Dict[str, str]]:
        """Efficiently fetch company-site pairs with retry logic"""
        for attempt in range(self.config.retry_count + 1):
            try:
                print(f"🔍 Fetching company-site pairs (attempt {attempt + 1})... for {self.config.api_url}")
                
                config_dict = self.config.get_config()
                token = config_dict['TOKEN']
                headers = {
                'Authorization': f'{token}'
            }
                async with self.session.get(self.config.api_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Handle different response formats
                        if isinstance(data, dict):
                            pairs = data.get('data', data.get('results', []))
                        elif isinstance(data, list):
                            pairs = data
                        else:
                            pairs = []
                        
                        print(f"✅ Fetched {len(pairs)} company-site pairs")
                        return pairs
                    else:
                        print(f"❌ API returned status {response.status}")
                        if attempt < self.config.retry_count:
                            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                            continue
                        return []
                        
            except Exception as e:
                print(f"❌ Error fetching pairs (attempt {attempt + 1}): {e}")
                if attempt < self.config.retry_count:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
                return []
        
        return []
    
    async def run_tests(self, processor, mode: ExecutionMode = ExecutionMode.CONCURRENT) -> Dict[str, Any]:
        """Run badge processing tests with specified mode"""
        start_time = time.time()
        
        # Fetch pairs
        all_pairs = await self.fetch_company_site_pairs()
        if not all_pairs:
            return {"success": False, "error": "No pairs found", "results": []}
        
        # Select pairs based on mode
        if mode == ExecutionMode.SAMPLE:
            test_pairs = all_pairs[:self.config.sample_size]
        else:
            test_pairs = all_pairs
        
        print(f"🎯 Running {mode.value} test mode with {len(test_pairs)} pairs")
        
        # Execute tests based on mode
        if mode == ExecutionMode.CONCURRENT:
            results = await self._run_concurrent_tests(processor, test_pairs)
        elif mode == ExecutionMode.BATCH:
            results = await self._run_batch_tests(processor, test_pairs)
        elif mode == ExecutionMode.SEQUENTIAL:
            results = await self._run_sequential_tests(processor, test_pairs)
        else:
            results = await self._run_concurrent_tests(processor, test_pairs)
        
        # Calculate metrics
        execution_time = time.time() - start_time
        successful = sum(1 for r in results if r.get('success'))
        failed = len(results) - successful
        success_rate = successful / len(results) if results else 0
        
        summary = {
            "success": success_rate >= self.config.min_success_rate,
            "total_pairs": len(all_pairs),
            "tested_pairs": len(test_pairs),
            "successful": successful,
            "failed": failed,
            "success_rate": success_rate,
            "execution_time": execution_time,
            "avg_time_per_test": execution_time / len(results) if results else 0,
            "mode": mode.value,
            "results": results if self.config.log_individual_results else []
        }
        
        self._print_summary(summary)
        return summary
    
    async def _run_concurrent_tests(self, processor, pairs: List[Dict]) -> List[Dict]:
        """Run tests with controlled concurrency"""
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        async def process_with_semaphore(pair):
            async with semaphore:
                return await self._process_single_pair(processor, pair)
        
        tasks = [process_with_semaphore(pair) for pair in pairs]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_batch_tests(self, processor, pairs: List[Dict]) -> List[Dict]:
        """Run tests in batches"""
        batches = [pairs[i:i + self.config.batch_size] 
                  for i in range(0, len(pairs), self.config.batch_size)]
        
        all_results = []
        for batch_num, batch in enumerate(batches, 1):
            print(f"🔄 Processing batch {batch_num}/{len(batches)}")
            
            batch_tasks = [self._process_single_pair(processor, pair) for pair in batch]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            all_results.extend(batch_results)
            
            if batch_num < len(batches):
                await asyncio.sleep(self.config.delay_between_batches)
        
        return all_results
    
    async def _run_sequential_tests(self, processor, pairs: List[Dict]) -> List[Dict]:
        """Run tests sequentially (safest but slowest)"""
        results = []
        for i, pair in enumerate(pairs, 1):
            if self.config.log_individual_results:
                print(f"🔄 Processing {i}/{len(pairs)}: {pair.get('company_id')}/{pair.get('site_code')}")
            
            result = await self._process_single_pair(processor, pair)
            results.append(result)
            
            if self.config.fail_fast and not result.get('success'):
                print("🛑 Fail-fast mode: stopping on first error")
                break
            
            if i < len(pairs):
                await asyncio.sleep(self.config.delay_between_requests)
        
        return results
    
    async def _process_single_pair(self, processor, pair: Dict) -> Dict:
        """Process a single company-site pair with error handling"""
        company_id = pair.get('company_id')
        site_code = pair.get('site_code')
        
        if not company_id or not site_code:
            return {
                'success': False,
                'company_id': company_id,
                'site_code': site_code,
                'error': 'Missing company_id or site_code',
                'duration': 0
            }
        
        start_time = time.time()
        
        try:
            message_id = f"auto_test_{company_id}_{site_code}_{int(time.time())}"
            
            result = await processor.process_badge_evaluation(
                company_id=company_id,
                site_code=site_code,
                badge_id=self.config.badge_id,
                message_id=message_id
            )
            
            duration = time.time() - start_time
            success = result is not None and isinstance(result, dict)
            
            return {
                'success': success,
                'company_id': company_id,
                'site_code': site_code,
                'error': None if success else 'Invalid result format',
                'duration': duration,
                'result_summary': {
                    'has_result': result is not None,
                    'is_dict': isinstance(result, dict),
                    'result_keys': list(result.keys()) if isinstance(result, dict) else []
                }
            }
            
        except Exception as e:
            duration = time.time() - start_time
            return {
                'success': False,
                'company_id': company_id,
                'site_code': site_code,
                'error': str(e),
                'exception_type': type(e).__name__,
                'duration': duration
            }
  