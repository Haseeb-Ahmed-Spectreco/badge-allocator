from contextlib import asynccontextmanager, redirect_stdout
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

import io


console = Console()
@asynccontextmanager
async def rich_loader(task_description):
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
        console=console,
    )
    task_id = progress.add_task(task_description, start=True)
    with progress:
        # Redirect prints to rich console
        with console.capture() as capture:
            yield
        # Print captured output after loader
        console.print(capture.get())