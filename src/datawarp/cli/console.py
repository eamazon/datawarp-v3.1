"""
Shared Rich console and theme for DataWarp CLI.
"""
from rich.console import Console
from rich.theme import Theme

# Simple theme - dark blue for everything (readable on light terminals)
custom_theme = Theme({
    "info": "blue",
    "success": "blue",
    "warning": "blue",
    "error": "bold red",
    "highlight": "bold blue",
    "muted": "blue",
    "dim": "blue",
    "url": "underline blue",
    "period": "blue",
    "count": "blue",
    "table.header": "bold blue",
    "table.cell": "blue",
    "prompt.default": "blue",
})

console = Console(theme=custom_theme, highlight=False)
