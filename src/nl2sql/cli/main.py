from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from nl2sql.cli.client import APIError, api_client
from nl2sql.cli.config import CLIConfig, load_config, save_config

app = typer.Typer(help="Interact with the nl2sql service")
console = Console()

project_app = typer.Typer(help="Manage projects")
connector_app = typer.Typer(help="Manage connectors")
schema_app = typer.Typer(help="Schema snapshot operations")
train_app = typer.Typer(help="Training workflows")
runs_app = typer.Typer(help="Stream and monitor runs")

app.add_typer(project_app, name="project")
app.add_typer(connector_app, name="connector")
app.add_typer(schema_app, name="schema")
app.add_typer(train_app, name="train")
app.add_typer(runs_app, name="runs")


def _api_call(ctx: typer.Context, method: str, url: str, *, payload: Any | None = None) -> Any:
    config: CLIConfig = ctx.obj
    with api_client(config) as client:
        return client.request(method, url, json=payload)


@app.callback()
def main(
    ctx: typer.Context,
    base_url: Optional[str] = typer.Option(None, "--base-url", help="Override the API base URL"),
) -> None:
    config = load_config()
    if base_url:
        config.base_url = base_url
        save_config(config)
    ctx.obj = config


@app.command()
def login(token: Optional[str] = typer.Option(None, "--token", help="API token")) -> None:
    config = load_config()
    token = token or typer.prompt("API token", hide_input=True)
    config.token = token
    save_config(config)
    console.print("[green]Token saved.[/green]")


def _handle_api(ctx: typer.Context, method: str, url: str, *, payload: Any | None = None) -> Any:
    try:
        return _api_call(ctx, method, url, payload=payload)
    except APIError as exc:
        console.print(f"[red]{exc.detail}[/red]")
        raise typer.Exit(code=1) from exc


@project_app.command("create")
def project_create(ctx: typer.Context, name: str) -> None:
    data = _handle_api(ctx, "POST", "/projects", payload={"name": name})
    console.print_json(data=data)


@connector_app.command("add")
def connector_add(
    ctx: typer.Context,
    *,
    type: str = typer.Option(..., "--type", help="Connector type"),
    name: str = typer.Option(..., "--name", help="Connector name"),
    dsn: str = typer.Option(..., "--dsn", help="Connector DSN"),
) -> None:
    data = _handle_api(ctx, "POST", "/connectors", payload={"type": type, "name": name, "dsn": dsn})
    console.print_json(data=data)


@connector_app.command("test")
def connector_test(ctx: typer.Context, connector_id: str) -> None:
    data = _handle_api(ctx, "POST", f"/connectors/{connector_id}/test")
    console.print_json(data=data)


@schema_app.command("snapshot")
def schema_snapshot(ctx: typer.Context, connector_id: str) -> None:
    data = _handle_api(ctx, "POST", f"/connectors/{connector_id}/schema/snapshot")
    console.print_json(data=data)


@train_app.command("start")
def train_start(
    ctx: typer.Context,
    *,
    project: str = typer.Option(..., "--project"),
    schema: str = typer.Option(..., "--schema"),
    cfg: Path = typer.Option(..., "--cfg"),
) -> None:
    payload = {"project": project, "schema_snapshot": schema, "config_ref": str(cfg)}
    data = _handle_api(ctx, "POST", "/train", payload=payload)
    console.print_json(data=data)


@runs_app.command("watch")
def runs_watch(ctx: typer.Context, run_id: str) -> None:
    config: CLIConfig = ctx.obj
    with api_client(config) as client:
        try:
            with client.stream("GET", f"/runs/{run_id}/logs/stream") as response:
                for line in response.iter_lines():
                    if line:
                        console.print(f"[cyan]{line}[/cyan]")
        except APIError as exc:
            console.print(f"[red]{exc.detail}[/red]")
            raise typer.Exit(code=1) from exc


@app.command("deploy")
def deploy_create(ctx: typer.Context, *, run: str = typer.Option(..., "--run"), label: str = typer.Option(..., "--label")) -> None:
    data = _handle_api(ctx, "POST", "/deployments", payload={"run": run, "label": label})
    console.print_json(data=data)


@app.command("plan")
def plan(
    ctx: typer.Context,
    question: str,
    deployment: str = typer.Option(..., "--deployment"),
    connector: str = typer.Option(..., "--connector"),
    as_json: bool = typer.Option(False, "--json", help="Output raw JSON"),
) -> None:
    payload = {"question": question, "deployment": deployment, "connector": connector}
    data = _handle_api(ctx, "POST", "/inference/plan", payload=payload)

    config: CLIConfig = ctx.obj
    config.last_run_id = data.get("run_id")
    save_config(config)

    if as_json:
        console.print_json(data=data)
        return

    table = Table(title=f"Run {data['run_id']}")
    table.add_column("SQL")
    table.add_column("Rationale")
    table.add_column("Cost")
    for candidate in data.get("candidates", []):
        table.add_row(candidate["sql"], candidate["rationale"], str(candidate["est_cost"]))
    console.print(table)
    if clarifications := data.get("clarifications"):
        console.print("[yellow]Clarifications:[/yellow]")
        for item in clarifications:
            console.print(f" - {item}")


@app.command("exec")
def exec_sql(
    ctx: typer.Context,
    *,
    sql: str = typer.Option(..., "--sql"),
    connector: str = typer.Option(..., "--connector"),
    limit: int = typer.Option(100, "--limit"),
    save: Optional[Path] = typer.Option(None, "--save", help="Save CSV output"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Override the run id"),
) -> None:
    config: CLIConfig = ctx.obj
    chosen_run = run_id or config.last_run_id
    if not chosen_run:
        console.print("[red]No run id available. Run 'nl2sql plan' first or pass --run-id.[/red]")
        raise typer.Exit(code=1)

    payload = {
        "run_id": chosen_run,
        "approved_sql": sql,
        "connector": connector,
        "limit": limit,
    }
    data = _handle_api(ctx, "POST", "/inference/execute", payload=payload)

    if save:
        rows = data.get("rows", [])
        fieldnames = list(rows[0].keys()) if rows else []
        save.parent.mkdir(parents=True, exist_ok=True)
        with save.open("w", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        console.print(f"[green]Results saved to {save}[/green]")
    else:
        console.print_json(data=data)


@app.command("chat")
def chat(
    ctx: typer.Context,
    *,
    deployment: str = typer.Option(..., "--deployment"),
    connector: str = typer.Option(..., "--connector"),
) -> None:
    history: list[dict[str, str]] = []

    while True:
        user_input = typer.prompt("You")
        if user_input.strip().lower() in {"exit", "quit"}:
            console.print("[cyan]Bye![/cyan]")
            return
        history.append({"role": "user", "content": user_input})
        payload = {"deployment": deployment, "connector": connector, "history": history}
        data = _handle_api(ctx, "POST", "/inference/chat", payload=payload)
        history = data.get("messages", history)
        console.print(f"[green]Assistant:[/green] {history[-1]['content']}")
        config: CLIConfig = ctx.obj
        config.last_run_id = data.get("run_id")
        save_config(config)


@app.command("history")
def history(ctx: typer.Context, limit: int = typer.Option(20, "--limit")) -> None:
    data = _handle_api(ctx, "GET", f"/inference/runs?limit={limit}")
    table = Table(title="Recent inference runs")
    table.add_column("Run")
    table.add_column("Question")
    table.add_column("Status")
    for item in data.get("items", []):
        table.add_row(item["id"], item.get("question", ""), item.get("status", ""))
    console.print(table)


if __name__ == "__main__":
    app()
