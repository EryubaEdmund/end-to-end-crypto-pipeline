"""Validates dags/airflow_workflows.py without needing Airflow installed.

This sandbox doesn't have the full `apache-airflow` package installed (it's
a heavy dependency normally provided by the official Airflow Docker image —
see docker/docker-compose.yml). Instead, we statically parse the DAG file
and assert on its structure: that it's syntactically valid, defines the
expected @dag-decorated functions with the expected ids/schedules, that
each contains @task-decorated functions, and that each DAG factory is
actually invoked at module level (otherwise Airflow would never see it).
"""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "airflow_workflows.py"


def _load_tree() -> ast.Module:
    source = DAG_FILE.read_text()
    return ast.parse(source, filename=str(DAG_FILE))


def _decorator_call(node: ast.FunctionDef, name: str) -> ast.Call | None:
    for dec in node.decorator_list:
        if isinstance(dec, ast.Call):
            dec_name = dec.func.id if isinstance(dec.func, ast.Name) else getattr(dec.func, "attr", None)
            if dec_name == name:
                return dec
        elif isinstance(dec, ast.Name) and dec.id == name:
            return None
    return None


def _kwarg_value(call: ast.Call, key: str):
    for kw in call.keywords:
        if kw.arg == key and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


def test_dag_file_is_syntactically_valid():
    tree = _load_tree()
    assert isinstance(tree, ast.Module)


def _find_dag_functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    dag_funcs = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            dag_call = _decorator_call(node, "dag")
            if dag_call is not None:
                dag_id = _kwarg_value(dag_call, "dag_id") or node.name
                dag_funcs[dag_id] = node
    return dag_funcs


def test_expected_dags_are_defined_with_schedules():
    tree = _load_tree()
    dag_funcs = _find_dag_functions(tree)
    assert "crypto_market_ingestion" in dag_funcs
    assert "crypto_market_klines_backfill" in dag_funcs

    for dag_id, node in dag_funcs.items():
        dag_call = _decorator_call(node, "dag")
        schedule = _kwarg_value(dag_call, "schedule_interval")
        assert schedule, f"{dag_id} is missing a schedule_interval"


def test_each_dag_defines_at_least_one_task():
    tree = _load_tree()
    dag_funcs = _find_dag_functions(tree)
    for dag_id, node in dag_funcs.items():
        task_funcs = [
            n for n in ast.walk(node)
            if isinstance(n, ast.FunctionDef) and n is not node and _has_task_decorator(n)
        ]
        assert task_funcs, f"{dag_id} defines no @task functions"


def _has_task_decorator(node: ast.FunctionDef) -> bool:
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "task":
            return True
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name) and dec.func.id == "task":
            return True
    return False


def test_dag_factories_are_invoked_at_module_level():
    tree = _load_tree()
    dag_funcs = _find_dag_functions(tree)
    called_names = {
        node.value.func.id
        for node in tree.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
    }
    for dag_id, node in dag_funcs.items():
        assert node.name in called_names, f"{dag_id} ({node.name}) is defined but never instantiated"
