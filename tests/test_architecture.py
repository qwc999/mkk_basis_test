import ast
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("layer", "allowed"),
    [
        ("domain", {"app.domain"}),
        ("application", {"app.application", "app.domain"}),
    ],
)
def test_inner_layers_depend_only_on_stdlib_and_inner_layers(layer, allowed):
    root = Path(__file__).resolve().parents[1] / "app" / layer
    for path in root.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                assert node.level == 0, f"Use explicit layer imports in {path}"
                modules = [node.module or ""]
            else:
                continue
            for module in modules:
                assert module.split(".")[0] in sys.stdlib_module_names or any(
                    module == prefix or module.startswith(prefix + ".") for prefix in allowed
                ), f"{path.name} imports {module} across the {layer} boundary"


def test_http_presentation_does_not_access_infrastructure():
    root = Path(__file__).resolve().parents[1] / "app/presentation"
    for path in root.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            assert not any(
                name.startswith(
                    ("app.infrastructure", "sqlalchemy", "httpx", "faststream", "aio_pika")
                )
                for name in names
            ), f"Infrastructure import in HTTP presentation: {path.name}"
