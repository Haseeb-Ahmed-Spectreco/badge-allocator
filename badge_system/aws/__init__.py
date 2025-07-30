"""AWS integration components"""

from .lambda_processor import LambdaBadgeProcessor
from .queue_client import AWSBadgeQueueClient

__all__ = ["LambdaBadgeProcessor", "AWSBadgeQueueClient"]