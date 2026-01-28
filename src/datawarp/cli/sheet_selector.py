"""
Sheet analysis and selection UI for bootstrap command.

Handles Excel sheet previewing, grain detection display, and user selection.
"""
from typing import List
from urllib.parse import unquote

from rich.prompt import Prompt
from rich.table import Table

from datawarp.cli.console import console
from datawarp.cli.helpers import infer_sheet_description
from datawarp.loader import FileExtractor
from datawarp.metadata import detect_grain


def analyze_sheets(local_path: str, sheets: List[str]) -> List[dict]:
    """
    Analyze all sheets and return preview info with grain detection.

    Returns list of dicts with: name, grain, rows, cols, description, df, grain_info
    """
    previews = []
    with console.status("Detecting sheet types..."):
        for sheet in sheets:
            try:
                extractor = FileExtractor(local_path, sheet)
                structure = extractor.infer_structure()

                if not structure.is_valid:
                    previews.append({
                        'name': sheet, 'grain': 'invalid', 'rows': 0, 'cols': 0,
                        'description': 'Could not parse structure', 'df': None
                    })
                    continue

                df = extractor.to_dataframe()
                if df.empty:
                    previews.append({
                        'name': sheet, 'grain': 'empty', 'rows': 0, 'cols': 0,
                        'description': 'No data rows', 'df': None
                    })
                    continue

                grain_info = detect_grain(df)
                previews.append({
                    'name': sheet, 'grain': grain_info['grain'],
                    'rows': len(df), 'cols': len(df.columns),
                    'description': grain_info['description'] or infer_sheet_description(sheet),
                    'df': df, 'grain_info': grain_info,
                })
            except Exception as e:
                previews.append({
                    'name': sheet, 'grain': 'error', 'rows': 0, 'cols': 0,
                    'description': str(e)[:50], 'df': None
                })
    return previews


def display_sheet_table(previews: List[dict], filename: str) -> None:
    """Display a table of sheets with their properties."""
    table = Table(title=f"Sheets in {unquote(filename)}")
    table.add_column("#", style="dim", width=3)
    table.add_column("Sheet Name", style="bold white")
    table.add_column("Grain", style="green")
    table.add_column("Rows", justify="right")
    table.add_column("Cols", justify="right")
    table.add_column("Description")

    for i, sp in enumerate(previews, 1):
        grain_style = "green" if sp['grain'] not in ('unknown', 'empty', 'invalid', 'error') else "dim"
        is_data = sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0
        table.add_row(
            str(i) if is_data else f"[dim]{i}[/]", sp['name'],
            f"[{grain_style}]{sp['grain']}[/]",
            str(sp['rows']) if sp['rows'] > 0 else "-",
            str(sp['cols']) if sp['cols'] > 0 else "-",
            sp['description'][:40] + "..." if len(sp['description']) > 40 else sp['description'],
        )
    console.print(table)

    # Print summary counts
    entity_grains = ('trust', 'icb', 'gp_practice', 'region')
    known = [sp for sp in previews if sp['grain'] in entity_grains and sp['rows'] > 0]
    national = [sp for sp in previews if sp['grain'] in ('unknown', 'national') and sp['rows'] > 0]
    skipped = [sp for sp in previews if sp['grain'] in ('empty', 'invalid', 'error')]
    console.print(f"\n  [green]{len(known)} with entity[/], [yellow]{len(national)} national/unknown[/], [dim]{len(skipped)} skipped[/]")


def select_sheets(previews: List[dict], skip_unknown: bool) -> List[dict]:
    """
    Let user select which sheets to load.

    Returns list of selected sheet preview dicts (with df and grain_info).
    """
    # Single sheet with data - auto-select
    if len(previews) == 1 and previews[0]['df'] is not None:
        return previews

    entity_grains = ('trust', 'icb', 'gp_practice', 'region')
    known_indices = [i for i, sp in enumerate(previews, 1) if sp['grain'] in entity_grains and sp['rows'] > 0]
    national_indices = [i for i, sp in enumerate(previews, 1) if sp['grain'] in ('unknown', 'national') and sp['rows'] > 0]

    # Determine default selection and hint
    if known_indices:
        default_indices = known_indices
        hint = "Defaulting to sheets with detected entities (ICB/Trust/etc)"
    elif national_indices:
        if skip_unknown:
            console.print("\n  [yellow]Warning: All sheets have national/unknown entities[/]")
            console.print("  [dim]To load anyway, re-run with: --no-skip-unknown[/]\n")
            default_indices = []
            hint = None
        else:
            default_indices = [i for i, sp in enumerate(previews, 1)
                             if sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0]
            hint = "Loading all data sheets (--no-skip-unknown enabled)"
    else:
        default_indices = []
        hint = "No data sheets found"

    if hint:
        console.print(f"  [dim]{hint}[/]")

    default_str = ','.join(map(str, default_indices)) if default_indices else ''
    selection = Prompt.ask(f"\n  Select sheets (numbers, 'all', or enter for default)", default=default_str)

    # Parse selection
    if selection.lower() == 'all':
        indices = [i for i, sp in enumerate(previews, 1)
                  if sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0]
    elif not selection.strip():
        indices = default_indices
    else:
        try:
            indices = [int(x.strip()) for x in selection.split(',') if x.strip()]
        except ValueError:
            # Try matching by sheet name
            indices = [i for i, sp in enumerate(previews, 1) if sp['name'] in selection]

    # Return valid selections with data
    return [previews[i-1] for i in indices if 1 <= i <= len(previews) and previews[i-1]['df'] is not None]
