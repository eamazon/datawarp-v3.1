"""
Shared Rich console and theme for DataWarp CLI.
"""
from rich.console import Console
from rich.theme import Theme

# Custom theme for better visibility (avoid cyan which is hard to read)
custom_theme = Theme({
    "prompt.default": "bold white",
})

console = Console(theme=custom_theme)
