"""
Badge Evaluation System

A comprehensive system for evaluating and allocating badges.
"""

__version__ = "0.1.0"

# Core classes
from .core import BadgeVerifier, CriteriaEvaluator, KpiValidator

# AWS integration
from .aws import LambdaBadgeProcessor, AWSBadgeQueueClient

# Client and utilities
from .client import HttpClient
from .exceptions import DBError, HTTPRequestError

__all__ = [
    "BadgeVerifier", "CriteriaEvaluator", "KpiValidator",
    "LambdaBadgeProcessor", "AWSBadgeQueueClient", 
    "HttpClient", "DBError", "HTTPRequestError"
]