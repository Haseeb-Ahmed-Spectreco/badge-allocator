"""
Progress Bar Context Manager for Test Loading
"""

import asyncio
import io
import os
import sys
from contextlib import asynccontextmanager, redirect_stdout
from typing import List, Dict, Any, Callable
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.console import Console

console = Console()

@asynccontextmanager
async def suppress_prints_only():
    """
    Context manager that only suppresses print statements, not stderr or other I/O
    This is safer than suppressing all stdout/stderr
    """
    # Redirect only stdout (where print() goes), leave stderr alone
    null_stdout = io.StringIO()
    original_stdout = sys.stdout
    
    try:
        sys.stdout = null_stdout
        yield
    finally:
        sys.stdout = original_stdout


@asynccontextmanager
async def progress_tracker(total: int, description: str = "Processing..."):
    """
    Simple progress bar context manager (no output suppression by default)
    
    Args:
        total: Total number of items to process
        description: Description to show in progress bar
    
    Yields:
        update_progress: Function to call when an item is completed
    """
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("({task.completed}/{task.total})"),
        TimeElapsedColumn(),
    ) as progress:
        
        task_id = progress.add_task(description, total=total)
        completed = 0
        
        def update_progress():
            nonlocal completed
            completed += 1
            progress.update(task_id, completed=completed)
        
        yield update_progress