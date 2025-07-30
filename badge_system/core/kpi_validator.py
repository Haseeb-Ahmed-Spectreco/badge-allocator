#!/usr/bin/env python3
"""
KPI Validation Script

A standalone Python script to validate KPIs using either field checks from MongoDB 
or API checks, and save the results to the kpi_data collection.

Usage:
    python kpi_validator.py --company_id <id> --site_code <code> --kpi_id <id> [--token <token>] [--base_url <url>] [--framework_id <id>]

Example:
    python kpi_validator.py --company_id 123 --site_code ABC --kpi_id kpi_001
"""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from dotenv import load_dotenv

from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

from badge_system.exceptions import HTTPRequestError
from badge_system.client.http_client import HttpClient

import traceback
import asyncio

load_dotenv()


class KpiValidator:
    """
    KPI Validator class to validate KPIs using either field checks from MongoDB
    or API checks, and save results to kpi_data collection.
    """
    
    # Constructor to initialize the KPI Validator with database and API configuration.
    def __init__(self, mongo_uri: str, db_name: str, base_url: str, region_url: str, 
                 http_client: HttpClient, shared_db_client: Optional[AsyncIOMotorClient] = None, 
                 shared_db = None):
        """
        Initialize the KPI Validator with database and API configuration.
        
        Args:
            mongo_uri: MongoDB connection URI
            db_name: Database name
            base_url: Base URL for API endpoints
            region_url: Region URL for API Endpoints
            http_client: HTTP client for making API requests
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.base_url = base_url
        self.region_url = region_url
        self.http_client = http_client
        
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
    
    # Async context manager exit method to handle exceptions and cleanup.
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print("⚠️ Exception caught in KPI Validator async context manager:")
            print(f"  Type: {exc_type.__name__}")
            print(f"  Message: {exc_val}")
        tb_lines = traceback.format_exception(exc_type, exc_val, exc_tb)
        formatted_traceback = ''.join(tb_lines)
        print("🔍 Traceback:")
        print(formatted_traceback)
        if self._owns_connection:
            self.disconnect_from_db()


    ############################################################################
                # Helper Functions for data retrieval and validation
    ############################################################################
    
    # Get nested values from a dictionary using dot notation.
    def get_nested_value(self, obj: Dict[str, Any], path: str, default_value: Any = None) -> Any:
        """
        Get nested values from dictionary using dot notation.
        
        Args:
            obj: Dictionary to search in
            path: Dot-separated path (e.g., "user.profile.name")
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

    # Replace template variables in objects (recursive)
    def replace_placeholders(self, obj: Any, company_id: str, site_code: str, **kwargs) -> Any:
        """
        Replace template variables in objects recursively.
        
        Args:
            obj: Object to process (dict, list, str, or other)
            company_id: Company identifier
            site_code: Site code
            **kwargs: Additional variables for replacement
            
        Returns:
            Object with replaced placeholders
        """
        # Create variables dictionary
        variables = {
            'company_id': str(company_id),
            'site_code': str(site_code),
            **kwargs
        }
        
        if isinstance(obj, dict):
            return {key: self.replace_placeholders(value, company_id, site_code, **kwargs) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_placeholders(item, company_id, site_code, **kwargs) for item in obj]
        elif isinstance(obj, str):
            result = obj
            for var_name, var_value in variables.items():
                placeholder = f"{{{{{var_name}}}}}"
                if placeholder in result:
                    result = result.replace(placeholder, str(var_value))
            return result
        else:
            return obj
  

    ############################################################################
                # Helper Functions for evaluation methods' functions
    ############################################################################
    
    # Fetch data from API endpoint with dynamic variable replacement.
    async def get_endpoint_results(self, endpoint: str, company_id: str, site_code: str, is_regional: bool, **kwargs) -> Dict[str, Any]:
        """
        Fetch data from API endpoint with dynamic variable replacement.
        
        Args:
            endpoint: API endpoint with placeholders (e.g., "/api/company/info/{company_id}")
            company_id: Company identifier
            site_code: Site code
            is_regional: Whether to use regional API
            **kwargs: Additional variables for replacement
            
        Returns:
            Dict containing API response data
        """
        try:
            # Create a dictionary of all available variables for replacement
            variables = {
                'company_id': str(company_id),
                'site_code': str(site_code),
                **kwargs  # Include any additional variables passed
            }
            
            # Replace variables in the endpoint
            formatted_endpoint = endpoint
            
            # Replace {variable} format
            for key, value in variables.items():
                formatted_endpoint = formatted_endpoint.replace(f"{{{key}}}", value)
            
            print(f"🔗 Calling endpoint: {formatted_endpoint}")
            
            # Make the API request
            response = await self.http_client.get(endpoint=formatted_endpoint, is_regional=is_regional)
            return response
            
        except HTTPRequestError as e:
            print(f"❌ Error fetching data from {endpoint}: {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error in get_endpoint_results: {e}")
            raise
    
    # Validate a field value against the given rule.
    def _validate_field_value(self, value: Any, rule: Dict[str, Any]) -> bool:
        """Validate a field value against the given rule."""
        # Check if field must exist
        if rule.get("mustExist") and (value is None or value == ""):
            return False

        # Check expected value
        if "expectedValue" in rule and value != rule["expectedValue"] and rule["expectedValue"] is not None:
            return False

        # Check value type
        if "valueType" in rule:
            expected_type = rule["valueType"]
            
            if expected_type == "string" and not isinstance(value, str):
                return False
            elif expected_type == "number" and not isinstance(value, (int, float)):
                return False
            elif expected_type == "boolean" and not isinstance(value, bool):
                return False
            elif expected_type == "array" and not isinstance(value, list):
                return False
            elif expected_type == "object" and not isinstance(value, dict):
                return False

        # Check minimum length
        if "minLength" in rule and rule["minLength"] is not None:
            if isinstance(value, (str, list)) and len(value) < rule["minLength"]:
                return False

        return True
    
    # Validate a response field against the given rule.
    def _validate_response_field(self, response: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Validate a response field against the given rule."""
        field_name = rule.get("field")
        value = self.get_nested_value(response, field_name)
        
        return self._validate_field_value(value, rule)


    ############################################################################
                        # Main validation and saving function
    ############################################################################
    
    # Validate a KPI and save the results to the database.
    async def validate_and_save_kpi(self, company_id: str, site_code: str, kpi_id: str, 
                                   evidence_urls: List[str] = None) -> Dict[str, Any]:
        """
        Validate a KPI and save the results to the database.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            kpi_id: KPI identifier
            evidence_urls: List of evidence URLs (optional)
            
        Returns:
            Dictionary containing validation and save results
        """
        try:
            # Validate the KPI
            validation_result = await self.validate_kpi(company_id, site_code, kpi_id)
            
            # Save the results to database
            save_result = await self.save_kpi_data(
                company_id=company_id,
                site_code=site_code,
                kpi_id=kpi_id,
                validation_result=validation_result,
                evidence_urls=evidence_urls
            )
            
            return {
                "validation_result": validation_result,
                "save_result": save_result
            }
            
        except Exception as e:
            print(f"❌ Error in validate_and_save_kpi: {e}")
            return {"error": str(e), "status": 500}
    
    # Check and apply the KPI evaluation method
    async def validate_kpi(self, company_id: str, site_code: str, kpi_id: str) -> Dict[str, Any]:
        """
        Validate a KPI using the specified evaluation method.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            kpi_id: KPI identifier
            framework_id: Framework identifier (optional)
            
        Returns:
            Dictionary containing validation results
        """
        try:
            # Get the KPI from database
            kpi: Dict[str, Any] = await self.db.kpis.find_one({"kpi_id": kpi_id})
            
            if not kpi:
                return {"error": "KPI not found", "status": 404}

            method = kpi.get("evaluation_method", {})
            method_type = method.get("type")
            
            print(f"🔍 Validating KPI: {kpi_id} using method type: {method_type}")

            # Route to appropriate validation method
            if method_type == "field_checks":
                return await self._validate_field_checks(company_id, site_code, method)
            
            elif method_type == "api_checks":
                return await self._validate_api_checks(method, company_id, site_code)
            
            elif method_type == "combination":
                return await self._validate_combination(method, company_id, site_code)
            
            elif method_type == "expression":
                return await self._validate_expression(method, company_id, site_code)
            
            elif method_type == "custom_fn":
                return await self._validate_custom_function(method, company_id, site_code)
            
            else:
                return {"error": f"Unsupported evaluation method type: {method_type}", "status": 400}

        except Exception as e:
            print(f"❌ Error validating KPI: {e}")
            return {"error": str(e), "status": 500}
    
    # Save KPI validation results to the kpi_data collection.
    async def save_kpi_data(self, company_id: str, site_code: str, kpi_id: str, 
                           validation_result: Dict[str, Any], evidence_urls: List[str] = None) -> Dict[str, Any]:
        """
        Save KPI validation results to the kpi_data collection.
        
        Args:
            company_id: Company identifier
            site_code: Site code
            kpi_id: KPI identifier
            validation_result: Results from KPI validation
            evidence_urls: List of evidence URLs (optional)
            
        Returns:
            Dictionary containing save operation result
        """
        try:
            now = datetime.now(timezone.utc)

            # Prepare the document to save
            kpi_data_doc = {
                "kpi_id": kpi_id,
                "company_id": str(company_id),
                "site_code": site_code,
                "complete": validation_result.get("success", False),
                "evaluation_date": now,
                "validation_result": validation_result,  # Store the full validation result
                "evidence_urls": evidence_urls or [],
                "created_at": now,
                "updated_at": now
            }
            
            # Define the filter for upsert operation
            filter_criteria = {
                "kpi_id": kpi_id,
                "company_id": str(company_id),
                "site_code": site_code,
            }
            
            # Perform upsert operation
            result = await self.db.kpi_data.update_one(
                filter_criteria,
                {"$set": kpi_data_doc},
                upsert=True
            )
            
            operation_result = {
                "success": True,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None,
                "modified_count": result.modified_count,
                "matched_count": result.matched_count
            }
            
            if result.upserted_id:
                print(f"📝 Created new KPI data record with ID: {result.upserted_id}")
            else:
                print(f"📝 Updated existing KPI data record. Modified: {result.modified_count}")
            
            return operation_result
            
        except Exception as e:
            print(f"❌ Error saving KPI data: {e}")
            return {"success": False, "error": str(e)}
    
    
    ############################################################################
                        # Functions for evaluation methods
    ############################################################################
    
    # Validate API checks
    async def _validate_api_checks(self, method: Dict[str, Any], company_id: str, site_code: str) -> Dict[str, Any]:
        """Handle API checks validation."""
        try:
            print("🌐 Running API checks validation...")
            results = []

            for api in method.get("apis", []):
                endpoint = api.get("endpoint", "")
                method_type = api.get("method", "GET")
                is_regional = api.get("isRegional", False)
                
                print(f"  🔍 Checking API: {method_type} {endpoint}")

                try:
                    # Make API request using the HTTP client
                    if method_type.upper() == "GET":
                        response = await self.get_endpoint_results(
                            endpoint=endpoint,
                            company_id=company_id,
                            site_code=site_code,
                            is_regional=is_regional
                        )
                        
                        print(f"  ✅ API response received")

                    elif method_type.upper() == "POST":
                        response = await self.http_client.post(endpoint=endpoint, data={})
                    else:
                        results.append({
                            "endpoint": endpoint,
                            "valid": False,
                            "error": f"Unsupported HTTP method: {method_type}"
                        })
                        continue
                    # Validate response keys
                    valid = True
                    validation_details = []
                    
                    for key_check in api.get("responseKeysToCheck", []):
                        field_valid = self._validate_response_field(response, key_check)
                                                    
                        validation_details.append({
                            "field": key_check.get("field"),
                            "valid": field_valid
                        })
                        if not field_valid and key_check.get("mustExist", True):
                            valid = False
                        
                    results.append({
                        "endpoint": endpoint,
                        "method": method_type,
                        "valid": valid,
                        "field_details": validation_details
                    })

                except HTTPRequestError as e:
                    print(f"  ❌ API request failed: {e}")
                    results.append({
                        "endpoint": endpoint,
                        "valid": False,
                        "error": str(e)
                    })
                except Exception as e:
                    print(f"  ❌ Unexpected error: {e}")
                    results.append({
                        "endpoint": endpoint,
                        "valid": False,
                        "error": str(e)
                    })

            # Apply logic (all or any)
            logic = method.get("logic", "all")
            if logic == "all":
                all_valid = all(result["valid"] for result in results)
            else:  # logic == "any"
                all_valid = any(result["valid"] for result in results)

            return {
                "success": all_valid,
                "type": "api_checks",
                "logic": logic,
                "details": results
            }

        except Exception as e:
            print(f"❌ Error in API checks: {e}")
            return {"error": str(e), "status": 500}

    # Validate field checks from MongoDB with aggregation support
    async def _validate_field_checks(self, company_id: str, site_code: str, method: Dict[str, Any]) -> Dict[str, Any]:
        """Handle field checks validation from MongoDB with aggregation support."""
        try:
            print("📊 Running field checks validation...")
            all_field_results = []
            now = datetime.now(timezone.utc)

            current_date = now.strftime("%d-%m-%Y")
                    
            if "collections" in method:
                # Iterate through collections
                for collection_config in method.get("collections", []):
                    collection_name = collection_config.get("collection")
                    use_aggregation = collection_config.get("useAggregation", False)
                    fields_to_check = collection_config.get("fieldsToCheck", [])
                    expect_single_result = collection_config.get("expectSingleResult", False)
                    
                    print(f"  🔍 Checking collection: {collection_name} (aggregation: {use_aggregation})")
                    
                    # Get collection reference
                    collection = self.db[collection_name]
                    result = None
                    
                    try:
                        if use_aggregation:
                            # Handle aggregation pipeline
                            aggregation_pipeline = collection_config.get("aggregationPipeline", [])
                            min_result_length = collection_config.get("minResultLength")
                            
                            if not aggregation_pipeline:
                                print(f"    ❌ No aggregation pipeline provided for collection: {collection_name}")
                                all_field_results.append({
                                    "collection": collection_name,
                                    "field": "aggregation_pipeline",
                                    "valid": False,
                                    "error": "No aggregation pipeline provided"
                                })
                                continue
                            
                            # Replace placeholders in aggregation pipeline
                            resolved_pipeline = self.replace_placeholders(
                                aggregation_pipeline, company_id, site_code, current_date
                            )
                            
                            print(f"    🔄 Executing aggregation pipeline...")
                            
                            # Execute aggregation
                            aggregation_result = []
                            async for doc in collection.aggregate(resolved_pipeline):
                                aggregation_result.append(doc)
                            
                            # Handle single result vs array based on expectSingleResult
                            if expect_single_result:
                                result = aggregation_result[0] if aggregation_result else None
                                print(f"    📄 Got single result: {result is not None}")
                            else:
                                result = aggregation_result
                                print(f"    📋 Got {len(aggregation_result)} results ")
                            
                            # Check minimum result length for aggregation
                            if min_result_length is not None:
                                actual_length = len(aggregation_result)
                                length_valid = actual_length >= min_result_length
                                
                                all_field_results.append({
                                    "collection": collection_name,
                                    "field": "aggregation_result_length",
                                    "valid": length_valid,
                                    "rule": f"minResultLength: {min_result_length}",
                                    "actual_length": actual_length
                                })
                                
                                print(f"    ✓ Result length check: {'✅ Valid' if length_valid else '❌ Invalid'} ({actual_length}/{min_result_length})")
                                
                                if not length_valid and not fields_to_check:
                                    # If only checking length and it fails, continue to next collection
                                    continue
                        
                        else:
                            # Handle regular query
                            query = collection_config.get("query", {})
                            min_length = collection_config.get("minLength")
                            
                            # Replace placeholders in query
                            if isinstance(query, str):
                                try:
                                    query = json.loads(query)
                                except json.JSONDecodeError:
                                    print(f"    ❌ Invalid JSON in query: {query}")
                                    all_field_results.append({
                                        "collection": collection_name,
                                        "field": "query",
                                        "valid": False,
                                        "error": "Invalid JSON in query"
                                    })
                                    continue
                            
                            resolved_query = self.replace_placeholders(query, company_id, site_code)
                            
                            print(f"    🔍 Executing query: {resolved_query}")
                            
                            # Execute find query
                            if min_length is not None and min_length > 0:
                                # Check document count
                                document_count = await collection.count_documents(resolved_query)
                                length_valid = document_count >= min_length
                                
                                all_field_results.append({
                                    "collection": collection_name,
                                    "field": "document_count",
                                    "valid": length_valid,
                                    "rule": f"minLength: {min_length}",
                                    "actual_count": document_count
                                })
                                
                                print(f"    ✓ Document count check: {'✅ Valid' if length_valid else '❌ Invalid'} ({document_count}/{min_length})")
                                
                                if not length_valid and not fields_to_check:
                                    # If only checking count and it fails, continue to next collection
                                    continue
                            
                            # Get the document for field validation
                            result = await collection.find_one(resolved_query)
                            print(f"    📄 Document found: {result is not None}")
                        
                        # Validate fields if specified
                        if fields_to_check:
                            if result is None:
                                # Add failed results for all fields if no result
                                for field_config in fields_to_check:
                                    field_name = field_config.get("field")
                                    all_field_results.append({
                                        "collection": collection_name,
                                        "field": field_name,
                                        "valid": False,
                                        "error": "No result found for field validation"
                                    })
                            else:
                                # Validate each field
                                for field_config in fields_to_check:
                                    field_name = field_config.get("field")
                                    
                                    # Get the field value
                                    value = self.get_nested_value(result, field_name)
                                    valid = self._validate_field_value(value, field_config)
                                    
                                    all_field_results.append({
                                        "collection": collection_name,
                                        "field": field_name,
                                        "valid": valid,
                                        "rule": field_config,
                                        "value": value
                                    })
                                    
                                    print(f"    ✓ Field '{field_name}': {'✅ Valid' if valid else '❌ Invalid'} (value: {value})")
                    
                    except Exception as e:
                        print(f"    ❌ Error processing collection {collection_name}: {e}")
                        all_field_results.append({
                            "collection": collection_name,
                            "field": "collection_processing",
                            "valid": False,
                            "error": str(e)
                        })
            
            else:
                # No field configuration found
                return {
                    "success": False,
                    "type": "field_checks",
                    "error": "No collections configuration found"
                }

            # Apply logic (all or any)
            logic = method.get("logic", "all")
            if logic == "all":
                all_valid = all(result["valid"] for result in all_field_results)
            else:  # logic == "any"
                all_valid = any(result["valid"] for result in all_field_results)

            print(f"  📊 Field checks result: {'✅ Valid' if all_valid else '❌ Invalid'} (logic: {logic})")

            return {
                "success": all_valid,
                "type": "field_checks",
                "logic": logic,
                "field_details": all_field_results,
                "total_checks": len(all_field_results),
                "passed_checks": sum(1 for result in all_field_results if result["valid"])
            }

        except Exception as e:
            print(f"❌ Error in field checks: {e}")
            return {"error": str(e), "status": 500}
    
    # Validate combination of field and API checks
    async def _validate_combination(self, method: Dict[str, Any], company_id: str, site_code: str) -> Dict[str, Any]:
        """Handle combination validation (both field and API checks)."""
        try:
            print("🔄 Running combination validation...")
            
            # Run field checks if they exist
            field_result = None
            if method.get("collections"):
                field_result = await self._validate_field_checks(company_id, site_code, method)
            
            # Run API checks if they exist
            api_result = None
            if method.get("apis"):
                api_result = await self._validate_api_checks(method=method, company_id=company_id, site_code=site_code)
            
            # Combine results based on logic
            logic = method.get("logic", "all")
            results = []
            
            if field_result:
                results.append(field_result.get("success", False))
            if api_result:
                results.append(api_result.get("success", False))
            
            if logic == "all":
                all_valid = all(results) if results else False
            else:  # logic == "any"
                all_valid = any(results) if results else False
            
            return {
                "success": all_valid,
                "type": "combination",
                "logic": logic,
                "field_checks": field_result,
                "api_checks": api_result
            }

        except Exception as e:
            print(f"❌ Error in combination validation: {e}")
            return {"error": str(e), "status": 500}

    # Validate expressions
    async def _validate_expression(self, method: Dict[str, Any], company_id: str, site_code: str) -> Dict[str, Any]:
        """Handle expression validation."""
        try:
            print("📝 Running expression validation...")
            expression = method.get("expression", "")
            
            if not expression:
                return {"error": "No expression provided", "status": 400}
            
            # For security, this is a placeholder implementation
            # In production, you'd want to use a safe expression evaluator
            print(f"  Expression to evaluate: {expression}")
            
            # This is a simplified implementation - you'd need a proper expression parser
            return {
                "success": False,
                "type": "expression",
                "error": "Expression validation not fully implemented",
                "expression": expression
            }

        except Exception as e:
            print(f"❌ Error in expression validation: {e}")
            return {"error": str(e), "status": 500}

    # Validate custom functions
    async def _validate_custom_function(self, method: Dict[str, Any], company_id: str, site_code: str) -> Dict[str, Any]:
        """Handle custom function validation."""
        try:
            print("⚙️ Running custom function validation...")
            custom_fn = method.get("custom_fn", {})
            fn_name = custom_fn.get("name")
            fn_params = custom_fn.get("params", {})
            
            if not fn_name:
                return {"error": "No custom function name provided", "status": 400}
            
            print(f"  Custom function: {fn_name} with params: {fn_params}")
            
            # Replace placeholders in function parameters
            resolved_params = self.replace_placeholders(fn_params, company_id, site_code)
            
            # This is a placeholder - you'd implement your custom functions here
            # Example implementation for specific functions:
            if fn_name == "checkTargetDataCoverage":
                return await self._custom_test_target_data_coverage(resolved_params)
            
            return {
                "success": False,
                "type": "custom_fn",
                "error": "Custom function validation not implemented",
                "function_name": fn_name,
                "params": resolved_params
            }

        except Exception as e:
            print(f"❌ Error in custom function validation: {e}")
            return {"error": str(e), "status": 500}

    # Custom function implementation example
    async def _custom_test_target_data_coverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Example implementation of custom target data coverage check."""
        try:
            company_id = params.get("company_id")
            site_code = params.get("site_code")
            if not company_id:
                return {"success": False, "error": "company_id parameter required"}
            
            print(f"  🎯 Checking target data coverage for company: {company_id}")
            
            # Execute the aggregation pipeline for target data coverage
            pipeline = [
                {
                    "$match": {
                        "company_id": company_id,
                        "site_code": site_code
                    }
                },
                {
                    "$lookup": {
                        "from": "codes",
                        "localField": "internal_code_id",
                        "foreignField": "_id",
                        "as": "code_info"
                    }
                },
                {
                    "$match": {
                        "code_info.code_visibility": {
                            "$in": ["all", "target", "baseline_target"]
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "cdata",
                        "let": {
                            "internal_code_id": "$internal_code_id",
                            "site_code": "$site_code"
                        },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$internal_code_id", "$$internal_code_id"]},
                                            {"$eq": ["$company_code", company_id]},
                                            {"$eq": ["$site_code", "$$site_code"]},
                                            {"$eq": ["$type", "target"]}
                                        ]
                                    }
                                }
                            }
                        ],
                        "as": "target_data"
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_codes": {"$sum": 1},
                        "codes_with_target": {
                            "$sum": {
                                "$cond": [
                                    {"$gt": [{"$size": "$target_data"}, 0]},
                                    1,
                                    0
                                ]
                            }
                        },
                        "missing_codes": {
                            "$push": {
                                "$cond": [
                                    {"$eq": [{"$size": "$target_data"}, 0]},
                                    {
                                        "internal_code_id": "$internal_code_id",
                                        "site_code": "$site_code"
                                    },
                                    "$$REMOVE"
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "total_codes": 1,
                        "codes_with_target": 1,
                        "percentage": {
                            "$cond": [
                                {"$eq": ["$total_codes", 0]},
                                0,
                                {"$multiply": [{"$divide": ["$codes_with_target", "$total_codes"]}, 100]}
                            ]
                        },
                        "all_have_target": {"$eq": ["$total_codes", "$codes_with_target"]},
                        "missing_codes": 1,
                        "codes_missing_target": {"$subtract": ["$total_codes", "$codes_with_target"]},
                        "completion_status": {
                            "$cond": [
                                {"$eq": ["$total_codes", "$codes_with_target"]},
                                "complete",
                                "incomplete"
                            ]
                        }
                    }
                }
            ]
            
            # Execute aggregation
            result = []
            async for doc in self.db.company_codes.aggregate(pipeline):
                result.append(doc)
            
            if not result:
                return {
                    "success": True,
                    "passed": False,
                    "score": 0,
                    "message": "No company codes found",
                    "details": {"total_codes": 0, "codes_with_target": 0, "percentage": 0}
                }
            
            data = result[0]
            
            return {
                "success": True,
                "passed": data.get("all_have_target", False),
                "score": round(data.get("percentage", 0), 2),
                "message": f"Target data coverage: {data.get('codes_with_target', 0)}/{data.get('total_codes', 0)} codes",
                "details": {
                    "total_codes": data.get("total_codes", 0),
                    "codes_with_target": data.get("codes_with_target", 0),
                    "percentage": round(data.get("percentage", 0), 2),
                    "all_have_target": data.get("all_have_target", False)
                }
            }
            
        except Exception as e:
            print(f"  ❌ Error in target data coverage check: {e}")
            return {"success": False, "error": str(e)}

    # Custom function implementation example
    async def _custom_test_task_completion(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Check task completion status for projects based on company_id and site_code."""
        try:
            company_id = params.get("company_id")
            site_code = params.get("site_code")
            now = datetime.now(timezone.utc)

            current_date = params.get("current_date",now.strftime("%d-%m-%Y"))
            
            if not company_id:
                return {"success": False, "error": "company_id parameter required"}
            

            # Execute the aggregation pipeline for task completion
            pipeline = [
                {
                    "$match": {
                        "company_id": company_id,
                        "site_code": site_code
                    }
                },
                {
                    "$lookup": {
                        "from": "project_details",
                        "localField": "_id",
                        "foreignField": "project_id",
                        "as": "tasks"
                    }
                },
                {
                    "$addFields": {
                        "tasks": {
                            "$sortArray": {
                                "input": "$tasks",
                                "sortBy": { "started_at": 1 }
                            }
                        },
                        "current_date": current_date
                    }
                },
                {
                    "$addFields": {
                        "project_start_date": { "$arrayElemAt": ["$tasks.started_at", 0] },
                        "project_due_date": { "$arrayElemAt": ["$tasks.due_date", -1] },
                        "task_status_check": {
                            "$map": {
                                "input": "$tasks",
                                "as": "task",
                                "in": {
                                    "task_id": "$$task._id",
                                    "title": "$$task.title",
                                    "is_past_due": {
                                        "$and": [
                                            { "$lt": ["$$task.due_date", "$current_date"] },
                                            { "$ne": ["$$task.status", "Complete"] }
                                        ]
                                    },
                                    "is_active_task": {
                                        "$and": [
                                            { "$lte": ["$$task.started_at", "$current_date"] },
                                            { "$gte": ["$$task.due_date", "$current_date"] },
                                            { "eq": ["$$task.status", "In Progress"] }
                                        ]
                                    },
                                    "is_future_task": { "$gt": ["$$task.started_at", "$current_date"] },
                                    
                                    
                                    "status": "$$task.status"
                                }
                            }
                        }
                    }
                },
                {
                    "$addFields": {
                        "passed_check": {
                            "$and": [
                                {
                                    "$eq": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$task_status_check",
                                                    "as": "tsc",
                                                    "cond": { "$and": ["$$tsc.is_past_due", { "$ne": ["$$tsc.status", "Complete"] }] }
                                                }
                                            }
                                        },
                                        0
                                    ]
                                },
                                {
                                    "$eq": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$task_status_check",
                                                    "as": "tsc",
                                                    "cond": { "$and": [{"$lt": ["$$tsc.started_at", "$current_date"]}, { "$eq": ["$$tsc.status", "New"] }] }
                                                }
                                            }
                                        },
                                        0
                                    ]
                                }
                            ]
                        },
                        "total_tasks": { "$size": "$tasks" },
                        "completed_tasks": {
                            "$size": {
                                "$filter": {
                                    "input": "$tasks",
                                    "as": "task",
                                    "cond": { "$eq": ["$$task.status", "Complete"] }
                                }
                            }
                        },
                        "failed_tasks": {
                            "$size": {
                                "$filter": {
                                    "input": "$tasks",
                                    "as": "task",
                                    "cond": { "$eq": ["$$task.is_past_due", True] }
                                }
                            }   
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "title": 1,
                        "project_start_date": 1,
                        "project_due_date": 1,
                        "current_date": 1,
                        "total_tasks": 1,
                        "completed_tasks": 1,
                        "failed_tasks": 1,
                        "passed_check": 1,
                        "completion_percentage": {
                            "$cond": [
                                { "$eq": ["$total_tasks", 0] },
                                0,
                                { "$multiply": [{ "$divide": ["$completed_tasks", "$total_tasks"] }, 100] }
                            ]
                        },
                        "validation_details": {
                            "past_due_violations": {
                                "$filter": {
                                    "input": "$task_status_check",
                                    "as": "tsc",
                                    "cond": { "$and": ["$$tsc.is_past_due", { "$ne": ["$$tsc.status", "Complete"] }] }
                                }
                            },
                            "active_new_violations": {
                                "$filter": {
                                    "input": "$task_status_check",
                                    "as": "tsc",
                                    "cond": { "$and": ["$$tsc.is_active_task", { "$eq": ["$$tsc.status", "New"] }] }
                                }
                            }
                        }
                    }
                }
            ]

            # Execute aggregation
            result = []
            async for doc in self.db.projects.aggregate(pipeline):
                result.append(doc)
            
            if not result:
                return {
                    "success": True,
                    "passed": False,
                    "score": 0,
                    "message": "No projects found",
                    "details": {
                        "total_tasks": 0,
                        "completed_tasks": 0,
                        "completion_percentage": 0,
                        "passed_check": False
                    }
                }
            
            # Process results (assuming we get one project at a time)
            response_data = []
            for project in result:
                response_data.append({
                    "project_id": str(project["_id"]),
                    "project_title": project.get("title", ""),
                    "passed": project.get("passed_check", False),
                    "score": round(project.get("completion_percentage", 0), 2),
                    "message": f"Task completion: {project.get('completed_tasks', 0)}/{project.get('total_tasks', 0)} tasks",
                    "fail_message": f"Tasks Failed: {project.get('failed_tasks', 0)}",
                    "details": {
                        "total_tasks": project.get("total_tasks", 0),
                        "completed_tasks": project.get("completed_tasks", 0),
                        "completion_percentage": round(project.get("completion_percentage", 0), 2),
                        "passed_check": project.get("passed_check", False),
                        "validation_issues": {
                            "past_due_tasks": len(project.get("validation_details", {}).get("past_due_violations", [])),
                            "active_new_tasks": len(project.get("validation_details", {}).get("active_new_violations", []))
                        }
                    }
                })

            return {
                "success": True,
                "data": response_data,
                "message": f"Found {len(response_data)} projects"
            }
            
        except Exception as e:
            print(f"  ❌ Error in task completion check: {e}")
            return {"success": False, "error": str(e)}
    
async def main():
    """Main function to run the script from command line."""
    # Load environment variables
    MONGODB_URI = os.getenv("MONGO_URI")
    DATABASE_NAME = os.getenv("MONGO_DB_NAME", "ensogove")
    REGION_URL = os.getenv("REGION_API_URL")
    AUTH_TOKEN = os.getenv("TOKEN")
    BASE_URL = os.getenv("BASE_URL")

    if not all([MONGODB_URI, BASE_URL, AUTH_TOKEN]):
        print("❌ Missing required environment variables")
        sys.exit(1)

    try:
        # Initialize HTTP client
        httpClient = HttpClient(base_url=BASE_URL, region_url=REGION_URL, token=AUTH_TOKEN)

        # Initialize KPI Validator
        validator = KpiValidator(
            mongo_uri=MONGODB_URI,
            db_name=DATABASE_NAME,
            region_url=REGION_URL,
            base_url=BASE_URL,
            http_client=httpClient
        )

        async with validator:
            # # Example validation with saving to database
            result = await validator.validate_and_save_kpi(
                company_id="555",
                site_code="MM-0012",
                kpi_id="KPI-TASK-COVERAGE-001",
                evidence_urls=["https://example.com/evidence1.pdf", "https://example.com/evidence2.pdf"]
            )
            
            result = await validator._custom_test_task_completion(params={
                "company_id": "555", "site_code": ""
            })
            
            print("\n📊 Complete Result:")
            print(json.dumps(result, indent=2, default=str))
            
            
            


    except HTTPRequestError as http_err:
        http_err.log_http_error()
        sys.exit(1)
    except (ValueError, TypeError) as e:
        print(f"❌ An initialization error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
    finally:
        await httpClient.close()


if __name__ == "__main__":
    asyncio.run(main())