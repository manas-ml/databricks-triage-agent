"""
CLI entry point for the Databricks ETL triage agent.

Usage:
    python main.py --run-id 12345
    python main.py --job-id 99
    python main.py --run-id 12345 --job-id 99 --verbose
    python main.py --run-id 12345 --output json > report.json
"""
import json
import os
import sys

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from agent import TriageAgent
from tools import DatabricksToolExecutor

console = Console()


@click.command()
@click.option("--job-id", type=int, default=None, help="Databricks Job ID to triage.")
@click.option("--run-id", type=int, default=None, help="Databricks Job Run ID to triage.")
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Print each tool call and result as the agent runs.",
)
def triage(job_id: int | None, run_id: int | None, output: str, verbose: bool) -> None:
    """Triage a Databricks ETL pipeline failure and apply automated fixes."""
    load_dotenv()

    # Validate required environment variables
    missing = _check_env_vars()
    if missing:
        console.print(
            f"[bold red]Missing required environment variables:[/bold red] {', '.join(missing)}\n"
            "Copy .env.example to .env and fill in the values."
        )
        sys.exit(1)

    if not job_id and not run_id:
        console.print(
            "[bold red]Error:[/bold red] Provide at least --job-id or --run-id."
        )
        sys.exit(1)

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")

    executor = DatabricksToolExecutor(
        host=host,
        token=token,
        default_warehouse_id=warehouse_id,
    )
    agent = TriageAgent(tool_executor=executor, verbose=verbose)

    if not verbose:
        with console.status("[bold cyan]Running triage agent…[/bold cyan]", spinner="dots"):
            report = agent.run(job_id=job_id, run_id=run_id)
    else:
        console.print(Panel("[bold cyan]Databricks ETL Triage Agent[/bold cyan]", expand=False))
        report = agent.run(job_id=job_id, run_id=run_id)

    _render_output(report, output, agent, job_id, run_id)


def _render_output(
    report: str,
    output_format: str,
    agent: TriageAgent,
    job_id: int | None,
    run_id: int | None,
) -> None:
    if output_format == "json":
        payload = {
            "job_id": job_id,
            "run_id": run_id,
            "iterations": agent.iteration_count,
            "tools_called": agent.tools_called,
            "report": report,
        }
        print(json.dumps(payload, indent=2))
    else:
        console.print()
        console.print(
            Panel(
                Markdown(report),
                title="[bold green]Triage Report[/bold green]",
                border_style="green",
            )
        )
        console.print(
            f"\n[dim]Completed in {agent.iteration_count} iteration(s), "
            f"{len(agent.tools_called)} tool call(s): "
            f"{', '.join(agent.tools_called)}[/dim]"
        )


def _check_env_vars() -> list[str]:
    required = ["ANTHROPIC_API_KEY", "DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    return [v for v in required if not os.environ.get(v)]


if __name__ == "__main__":
    triage()
