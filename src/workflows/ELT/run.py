from __future__ import annotations

import argparse
import atexit
import contextlib
import functools
import hashlib
import importlib
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
ELT_ROOT = SRC_ROOT / "workflows" / "ELT"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

REMOTE_PROJECT = os.environ.get("REMOTE_PROJECT", "flytesnacks").strip() or "flytesnacks"
REMOTE_DOMAIN = os.environ.get("REMOTE_DOMAIN", "development").strip() or "development"
TASK_NAMESPACE = os.environ.get("TASK_NAMESPACE", f"{REMOTE_PROJECT}-{REMOTE_DOMAIN}").strip()

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = (
    os.environ.get("ELT_PROFILE")
    or ("dev" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod")
).strip().lower()

SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark").strip() or "spark"
ELT_TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:2026-04-04-14-10--725b6e2@sha256:772077658f980c872bf4a43a24dc7ecf1549f8530765fa4f64b994c8ec199149",
).strip()
if not ELT_TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must not be empty")
os.environ["ELT_TASK_IMAGE"] = ELT_TASK_IMAGE

WORKFLOW_SOURCE_FILE = SRC_ROOT / "workflows" / "ELT" / "launch_plans.py"
WORKFLOW_SOURCE_REL = WORKFLOW_SOURCE_FILE.relative_to(SRC_ROOT)
WORKFLOW_IMPORT_MODULE = os.environ.get("WORKFLOW_IMPORT_MODULE", "workflows.ELT.launch_plans").strip()

USE_PORT_FORWARD = os.environ.get("USE_PORT_FORWARD", "1").lower() in {"1", "true", "yes", "y", "on"}
FLYTE_ADMIN_NAMESPACE = os.environ.get("FLYTE_ADMIN_NAMESPACE", "flyte").strip() or "flyte"
FLYTE_ADMIN_HOST = os.environ.get("FLYTE_ADMIN_HOST", "127.0.0.1").strip() or "127.0.0.1"
FLYTE_ADMIN_PORT = int(os.environ.get("FLYTE_ADMIN_PORT", "30081"))
PORT_FORWARD_TARGET_PORT = int(os.environ.get("PORT_FORWARD_TARGET_PORT", "81"))
PORT_FORWARD_LOG = Path(os.environ.get("PORT_FORWARD_LOG", "/tmp/flyteadmin-portforward.log"))

ACTIVATE_SCHEDULED_LAUNCHPLANS = os.environ.get("ACTIVATE_SCHEDULED_LAUNCHPLANS", "1").lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
USE_LATEST = os.environ.get("USE_LATEST", "0").lower() in {"1", "true", "yes", "y", "on"}

PYFLYTE_REGISTER_EXTRA_ARGS = os.environ.get("PYFLYTE_REGISTER_EXTRA_ARGS", "").strip()

RESOURCE_QUOTA_NAME = os.environ.get("RESOURCE_QUOTA_NAME", "spark-workload-quota")

RESOURCE_QUOTA_KIND_REQUESTS_CPU = os.environ.get("RESOURCE_QUOTA_KIND_REQUESTS_CPU", "4")
RESOURCE_QUOTA_KIND_REQUESTS_MEMORY = os.environ.get("RESOURCE_QUOTA_KIND_REQUESTS_MEMORY", "8Gi")
RESOURCE_QUOTA_KIND_LIMITS_CPU = os.environ.get("RESOURCE_QUOTA_KIND_LIMITS_CPU", "8")
RESOURCE_QUOTA_KIND_LIMITS_MEMORY = os.environ.get("RESOURCE_QUOTA_KIND_LIMITS_MEMORY", "12Gi")
RESOURCE_QUOTA_KIND_PODS = os.environ.get("RESOURCE_QUOTA_KIND_PODS", "30")
RESOURCE_QUOTA_KIND_PVC = os.environ.get("RESOURCE_QUOTA_KIND_PVC", "20")
RESOURCE_QUOTA_KIND_SERVICES = os.environ.get("RESOURCE_QUOTA_KIND_SERVICES", "40")

RESOURCE_QUOTA_EKS_REQUESTS_CPU = os.environ.get("RESOURCE_QUOTA_EKS_REQUESTS_CPU", "12")
RESOURCE_QUOTA_EKS_REQUESTS_MEMORY = os.environ.get("RESOURCE_QUOTA_EKS_REQUESTS_MEMORY", "24Gi")
RESOURCE_QUOTA_EKS_LIMITS_CPU = os.environ.get("RESOURCE_QUOTA_EKS_LIMITS_CPU", "24")
RESOURCE_QUOTA_EKS_LIMITS_MEMORY = os.environ.get("RESOURCE_QUOTA_EKS_LIMITS_MEMORY", "48Gi")
RESOURCE_QUOTA_EKS_PODS = os.environ.get("RESOURCE_QUOTA_EKS_PODS", "100")
RESOURCE_QUOTA_EKS_PVC = os.environ.get("RESOURCE_QUOTA_EKS_PVC", "50")
RESOURCE_QUOTA_EKS_SERVICES = os.environ.get("RESOURCE_QUOTA_EKS_SERVICES", "100")

LAUNCH_PLAN_COMMANDS: dict[str, tuple[str, ...]] = {
    "elt_workflow": ("ELT_WORKFLOW_LP_NAME", "ELT_WORKFLOW_LP"),
    "iceberg_maintenance_daily_lp": (
        "ICEBERG_MAINTENANCE_DAILY_LP_NAME",
        "ICEBERG_MAINTENANCE_DAILY_LP",
    ),
    "iceberg_maintenance_weekly_lp": (
        "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
        "ICEBERG_MAINTENANCE_WEEKLY_LP",
    ),
    "iceberg_maintenance_workflow": (
        "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
        "ICEBERG_MAINTENANCE_WEEKLY_LP",
    ),
}
EXECUTION_COMMANDS = tuple(LAUNCH_PLAN_COMMANDS.keys())


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}", file=sys.stderr)


def fatal(msg: str) -> NoReturn:
    raise SystemExit(f"FATAL: {msg}")


def require_bin(name: str) -> None:
    if shutil.which(name) is None:
        fatal(f"{name} not found in PATH")


def run_cmd(
    args: Sequence[str],
    *,
    check: bool = True,
    cwd: Path | None = None,
    input_text: str | None = None,
    capture_output: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    cp = subprocess.run(
        list(args),
        cwd=str(cwd or REPO_ROOT),
        text=True,
        input=input_text,
        capture_output=capture_output,
        check=False,
        env=env,
        close_fds=True,
    )
    if check and cp.returncode != 0:
        detail: list[str] = []
        if cp.stdout:
            detail.append(f"stdout:\n{cp.stdout.rstrip()}")
        if cp.stderr:
            detail.append(f"stderr:\n{cp.stderr.rstrip()}")
        suffix = f"\n{chr(10).join(detail)}" if detail else ""
        raise RuntimeError(f"command failed ({cp.returncode}): {' '.join(args)}{suffix}")
    return cp


def port_is_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


PORT_FORWARD_PROC: subprocess.Popen[str] | None = None


def stop_port_forward_if_any() -> None:
    global PORT_FORWARD_PROC

    proc = PORT_FORWARD_PROC
    PORT_FORWARD_PROC = None

    if proc is None:
        return

    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)


def cleanup() -> None:
    if USE_PORT_FORWARD:
        stop_port_forward_if_any()


atexit.register(cleanup)


def start_port_forward() -> None:
    global PORT_FORWARD_PROC

    if not USE_PORT_FORWARD:
        return

    if PORT_FORWARD_PROC is not None and PORT_FORWARD_PROC.poll() is None and port_is_open(
        FLYTE_ADMIN_HOST, FLYTE_ADMIN_PORT
    ):
        return

    stop_port_forward_if_any()
    PORT_FORWARD_LOG.parent.mkdir(parents=True, exist_ok=True)

    log(
        "Starting flyteadmin port-forward "
        f"{FLYTE_ADMIN_HOST}:{FLYTE_ADMIN_PORT} -> "
        f"{FLYTE_ADMIN_NAMESPACE}/svc/flyteadmin:{PORT_FORWARD_TARGET_PORT}"
    )

    with PORT_FORWARD_LOG.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                FLYTE_ADMIN_NAMESPACE,
                "port-forward",
                "svc/flyteadmin",
                f"{FLYTE_ADMIN_PORT}:{PORT_FORWARD_TARGET_PORT}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            close_fds=True,
            start_new_session=True,
        )

    PORT_FORWARD_PROC = proc

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if port_is_open(FLYTE_ADMIN_HOST, FLYTE_ADMIN_PORT):
            return
        if proc.poll() is not None:
            break
        time.sleep(1)

    tail = ""
    if PORT_FORWARD_LOG.is_file():
        try:
            tail = "\n".join(
                PORT_FORWARD_LOG.read_text(encoding="utf-8", errors="replace")
                .strip()
                .splitlines()[-20:]
            )
        except Exception:
            tail = ""
    stop_port_forward_if_any()
    if tail:
        fatal(f"flyteadmin port-forward did not become ready\n{tail}")
    fatal("flyteadmin port-forward did not become ready")


def init_flytectl() -> None:
    if not USE_PORT_FORWARD:
        return
    run_cmd(
        [
            "flytectl",
            "config",
            "init",
            f"--host={FLYTE_ADMIN_HOST}:{FLYTE_ADMIN_PORT}",
            "--insecure",
            "--force",
        ],
        capture_output=True,
    )


def lint_sources() -> None:
    require_bin("ruff")
    log(f"Running ruff on {ELT_ROOT}")
    run_cmd(["ruff", "check", str(ELT_ROOT)])


def _module_attr_name(module: object, candidates: Sequence[str]) -> str:
    for candidate in candidates:
        value = getattr(module, candidate, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and hasattr(value, "name"):
            name = getattr(value, "name", None)
            if isinstance(name, str) and name.strip():
                return name.strip()
    raise AttributeError(f"none of these launch plan attributes were found: {list(candidates)}")


def import_check() -> None:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    required = (
        ("elt_workflow", ("ELT_WORKFLOW_LP_NAME", "ELT_WORKFLOW_LP")),
        (
            "iceberg_maintenance_daily_lp",
            ("ICEBERG_MAINTENANCE_DAILY_LP_NAME", "ICEBERG_MAINTENANCE_DAILY_LP"),
        ),
        (
            "iceberg_maintenance_weekly_lp",
            ("ICEBERG_MAINTENANCE_WEEKLY_LP_NAME", "ICEBERG_MAINTENANCE_WEEKLY_LP"),
        ),
    )
    for command, candidates in required:
        try:
            _module_attr_name(mod, candidates)
        except AttributeError as exc:
            fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose launch plan for '{command}': {exc}")
    log("import_ok")


def _quota_values() -> dict[str, str]:
    if K8S_CLUSTER == "kind":
        return {
            "requests_cpu": RESOURCE_QUOTA_KIND_REQUESTS_CPU,
            "requests_memory": RESOURCE_QUOTA_KIND_REQUESTS_MEMORY,
            "limits_cpu": RESOURCE_QUOTA_KIND_LIMITS_CPU,
            "limits_memory": RESOURCE_QUOTA_KIND_LIMITS_MEMORY,
            "pods": RESOURCE_QUOTA_KIND_PODS,
            "persistentvolumeclaims": RESOURCE_QUOTA_KIND_PVC,
            "services": RESOURCE_QUOTA_KIND_SERVICES,
        }
    return {
        "requests_cpu": RESOURCE_QUOTA_EKS_REQUESTS_CPU,
        "requests_memory": RESOURCE_QUOTA_EKS_REQUESTS_MEMORY,
        "limits_cpu": RESOURCE_QUOTA_EKS_LIMITS_CPU,
        "limits_memory": RESOURCE_QUOTA_EKS_LIMITS_MEMORY,
        "pods": RESOURCE_QUOTA_EKS_PODS,
        "persistentvolumeclaims": RESOURCE_QUOTA_EKS_PVC,
        "services": RESOURCE_QUOTA_EKS_SERVICES,
    }


def _ensure_namespace() -> None:
    run_cmd(
        [
            "kubectl",
            "create",
            "namespace",
            TASK_NAMESPACE,
            "--dry-run=client",
            "-o",
            "yaml",
        ],
        check=True,
        capture_output=True,
    )
    run_cmd(
        ["kubectl", "apply", "-f", "-"],
        input_text=f"""apiVersion: v1
kind: Namespace
metadata:
  name: {TASK_NAMESPACE}
""",
    )


def _bootstrap_manifest() -> str:
    q = _quota_values()
    return f"""apiVersion: v1
kind: ServiceAccount
metadata:
  name: {SPARK_SERVICE_ACCOUNT}
  namespace: {TASK_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {SPARK_SERVICE_ACCOUNT}
  namespace: {TASK_NAMESPACE}
rules:
  - apiGroups: [""]
    resources:
      - pods
      - pods/log
      - services
      - configmaps
      - persistentvolumeclaims
      - events
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
      - deletecollection
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {SPARK_SERVICE_ACCOUNT}
  namespace: {TASK_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: {SPARK_SERVICE_ACCOUNT}
    namespace: {TASK_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {SPARK_SERVICE_ACCOUNT}
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: {RESOURCE_QUOTA_NAME}
  namespace: {TASK_NAMESPACE}
spec:
  hard:
    requests.cpu: "{q['requests_cpu']}"
    requests.memory: "{q['requests_memory']}"
    limits.cpu: "{q['limits_cpu']}"
    limits.memory: "{q['limits_memory']}"
    pods: "{q['pods']}"
    persistentvolumeclaims: "{q['persistentvolumeclaims']}"
    services: "{q['services']}"
"""


def bootstrap_manifest_quota_line() -> str:
    q = _quota_values()
    return (
        f"requests.cpu={q['requests_cpu']}, "
        f"requests.memory={q['requests_memory']}, "
        f"limits.cpu={q['limits_cpu']}, "
        f"limits.memory={q['limits_memory']}, "
        f"pods={q['pods']}, "
        f"persistentvolumeclaims={q['persistentvolumeclaims']}, "
        f"services={q['services']}"
    )


def bootstrap_target_namespace() -> None:
    require_bin("kubectl")
    log(f"Applying namespace, RBAC, and quota bootstrap for {TASK_NAMESPACE}")
    _ensure_namespace()
    run_cmd(["kubectl", "apply", "-f", "-"], input_text=_bootstrap_manifest())
    log(f"Bootstrap applied for {TASK_NAMESPACE} with quota: {bootstrap_manifest_quota_line()}")


def _can_i(verb: str, resource: str) -> bool:
    cp = run_cmd(
        [
            "kubectl",
            "auth",
            "can-i",
            verb,
            resource,
            "-n",
            TASK_NAMESPACE,
            f"--as=system:serviceaccount:{TASK_NAMESPACE}:{SPARK_SERVICE_ACCOUNT}",
        ],
        check=False,
        capture_output=True,
    )
    return cp.returncode == 0 and cp.stdout.strip() == "yes"


def ensure_namespace_bootstrap_ready() -> None:
    require_bin("kubectl")
    log(f"Verifying namespace bootstrap for {TASK_NAMESPACE}")

    bootstrap_target_namespace()

    sa = run_cmd(
        ["kubectl", "get", "serviceaccount", SPARK_SERVICE_ACCOUNT, "-n", TASK_NAMESPACE],
        check=False,
        capture_output=True,
    )
    if sa.returncode != 0:
        fatal(f"service account {SPARK_SERVICE_ACCOUNT} missing in {TASK_NAMESPACE}")

    rq = run_cmd(
        ["kubectl", "get", "resourcequota", RESOURCE_QUOTA_NAME, "-n", TASK_NAMESPACE],
        check=False,
        capture_output=True,
    )
    if rq.returncode != 0:
        fatal(f"resource quota {RESOURCE_QUOTA_NAME} missing in {TASK_NAMESPACE}")

    required_resources = ["pods", "services", "configmaps", "persistentvolumeclaims"]
    required_verbs = ["get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"]
    missing: list[str] = []
    for resource in required_resources:
        for verb in required_verbs:
            if not _can_i(verb, resource):
                missing.append(f"{verb} {resource}")
    if not _can_i("get", "pods/log"):
        missing.append("get pods/log")

    if missing:
        fatal("service account lacks required Spark permissions: " + ", ".join(missing))

    log(f"Bootstrap verified for {TASK_NAMESPACE}")


def registration_tree_files() -> list[Path]:
    files: list[Path] = [WORKFLOW_SOURCE_FILE]
    for root in (
        SRC_ROOT / "workflows" / "ELT" / "tasks",
        SRC_ROOT / "workflows" / "ELT" / "workflows",
    ):
        if root.is_dir():
            files.extend(sorted(p for p in root.rglob("*.py") if p.is_file()))
    return [path for path in dedupe_paths(files) if path.is_file()]


def dedupe_paths(items: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


@functools.lru_cache(maxsize=1)
def compute_registration_version() -> str:
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()
    tree = hashlib.sha256()
    tree.update(f"ELT_TASK_IMAGE={ELT_TASK_IMAGE}".encode())
    tree.update(b"\0")
    tree.update(f"ELT_PROFILE={ELT_PROFILE}".encode())
    tree.update(b"\0")
    tree.update(f"K8S_CLUSTER={K8S_CLUSTER}".encode())
    tree.update(b"\0")
    for path in registration_tree_files():
        tree.update(path.relative_to(REPO_ROOT).as_posix().encode("utf-8"))
        tree.update(b"\0")
        tree.update(path.read_bytes())
        tree.update(b"\0")
    return f"{git_sha[:12]}-{tree.hexdigest()[:16]}"


def pyflyte_register_supports_copy_or_fast_flag() -> tuple[bool, bool]:
    help_text = run_cmd(["pyflyte", "register", "--help"], check=False, capture_output=True)
    combined = f"{help_text.stdout}\n{help_text.stderr}"
    return ("--copy" in combined, "--fast" in combined)


def pyflyte_register_supports_service_account() -> bool:
    help_text = run_cmd(["pyflyte", "register", "--help"], check=False, capture_output=True)
    combined = f"{help_text.stdout}\n{help_text.stderr}"
    return "--service-account" in combined


def build_register_env() -> dict[str, str]:
    register_env = os.environ.copy()
    register_env["ELT_TASK_IMAGE"] = ELT_TASK_IMAGE
    existing_pythonpath = register_env.get("PYTHONPATH", "")
    register_env["PYTHONPATH"] = str(SRC_ROOT) + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    return register_env


def build_register_command(registration_version: str) -> list[str]:
    cmd = [
        "pyflyte",
        "register",
        "--project",
        REMOTE_PROJECT,
        "--domain",
        REMOTE_DOMAIN,
        "--image",
        ELT_TASK_IMAGE,
        "--version",
        registration_version,
    ]

    if pyflyte_register_supports_service_account():
        cmd.extend(["--service-account", SPARK_SERVICE_ACCOUNT])

    supports_copy, supports_fast = pyflyte_register_supports_copy_or_fast_flag()
    if supports_copy:
        cmd.extend(["--copy", "none"])
    elif supports_fast:
        cmd.append("--fast=false")
    else:
        fatal("installed pyflyte register does not advertise --copy or --fast; cannot safely disable fast registration")

    if PYFLYTE_REGISTER_EXTRA_ARGS:
        cmd.extend(shlex.split(PYFLYTE_REGISTER_EXTRA_ARGS))

    cmd.append(str(WORKFLOW_SOURCE_REL))
    return cmd


def _activate_launch_plan(lp_name: str, version: str) -> None:
    log(f"Activating launch plan: {lp_name}")
    run_cmd(
        [
            "flytectl",
            "update",
            "launchplan",
            "-p",
            REMOTE_PROJECT,
            "-d",
            REMOTE_DOMAIN,
            lp_name,
            "--version",
            version,
            "--activate",
            "--force",
        ]
    )


def register_entities() -> str:
    require_bin("pyflyte")
    if not WORKFLOW_SOURCE_FILE.is_file():
        fatal(f"workflow source file not found: {WORKFLOW_SOURCE_FILE}")

    registration_version = compute_registration_version()
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()

    log(f"Registering ELT from commit {git_sha}")
    log(f"Workflow import module: {WORKFLOW_IMPORT_MODULE}")
    log(f"Source file: {WORKFLOW_SOURCE_FILE}")
    log(f"Profile: {ELT_PROFILE} | Cluster: {K8S_CLUSTER} | Namespace: {TASK_NAMESPACE}")
    log(f"Task image: {ELT_TASK_IMAGE}")
    log(f"Registration version: {registration_version}")

    register_env = build_register_env()
    cmd = build_register_command(registration_version)
    run_cmd(cmd, cwd=SRC_ROOT, env=register_env)

    if ACTIVATE_SCHEDULED_LAUNCHPLANS:
        _activate_launch_plan("iceberg_maintenance_daily_lp", registration_version)
        _activate_launch_plan("iceberg_maintenance_weekly_lp", registration_version)

    log(f"Registration complete for version {registration_version}")
    return registration_version


def _resolve_launch_plan_name(command: str) -> str:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    candidates = LAUNCH_PLAN_COMMANDS[command]
    return _module_attr_name(mod, candidates)


def fetch_launch_plan_exec_spec(
    launch_plan_name: str,
    exec_spec_file: Path,
    *,
    latest: bool,
    version: str | None,
) -> None:
    exec_spec_file.unlink(missing_ok=True)

    args = [
        "flytectl",
        "get",
        "launchplan",
        "-p",
        REMOTE_PROJECT,
        "-d",
        REMOTE_DOMAIN,
        launch_plan_name,
    ]
    if latest:
        args.append("--latest")
    else:
        if not version:
            fatal("launch-plan version required when latest is disabled")
        args.extend(["--version", version])
    args.extend(["--execFile", str(exec_spec_file)])
    run_cmd(args)


def create_execution_from_spec(exec_spec_file: Path) -> None:
    run_cmd(
        [
            "flytectl",
            "create",
            "execution",
            "-p",
            REMOTE_PROJECT,
            "-d",
            REMOTE_DOMAIN,
            "--execFile",
            str(exec_spec_file),
        ]
    )


def execute_launch_plan(
    command: str,
    *,
    force_immediate_run: bool = False,
    latest: bool | None = None,
    version: str | None = None,
) -> None:
    require_bin("kubectl")
    require_bin("flytectl")

    ensure_namespace_bootstrap_ready()
    start_port_forward()
    init_flytectl()

    launch_plan_name = _resolve_launch_plan_name(command)
    effective_latest = USE_LATEST if latest is None else latest
    effective_version = version if version is not None else (None if effective_latest else compute_registration_version())

    if force_immediate_run:
        log(f"Force immediate run requested for {launch_plan_name}")
    else:
        log(f"Immediate execution for {launch_plan_name}")

    with tempfile.TemporaryDirectory(prefix=f"{launch_plan_name}.") as tmpdir:
        exec_spec_file = Path(tmpdir) / "exec.yaml"

        log(f"Fetching launch plan: {launch_plan_name}")
        fetch_launch_plan_exec_spec(
            launch_plan_name,
            exec_spec_file,
            latest=effective_latest,
            version=effective_version,
        )

        log(f"Creating execution from {exec_spec_file}")
        create_execution_from_spec(exec_spec_file)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="run.py")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("register", help="Register ELT workflows and launch plans")

    def add_launch_plan_command(name: str, help_text: str) -> None:
        p = sub.add_parser(name, help=help_text)
        p.add_argument(
            "--force-immediate-run",
            action="store_true",
            help="Explicitly run now; launch plans already execute immediately with flytectl.",
        )
        p.add_argument(
            "--latest",
            action="store_true",
            help="Fetch the latest registered launch plan version instead of the local version hash.",
        )
        p.add_argument(
            "--version",
            type=str,
            default=None,
            help="Fetch a specific launch plan version.",
        )

    add_launch_plan_command("elt_workflow", "Execute the ELT workflow launch plan")
    add_launch_plan_command("iceberg_maintenance_daily_lp", "Execute the daily Iceberg maintenance launch plan")
    add_launch_plan_command("iceberg_maintenance_weekly_lp", "Execute the weekly Iceberg maintenance launch plan")
    add_launch_plan_command(
        "iceberg_maintenance_workflow",
        "Alias for the weekly Iceberg maintenance launch plan",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    require_bin("kubectl")
    require_bin("git")
    require_bin("python3")

    if args.command == "register":
        lint_sources()
        import_check()
        ensure_namespace_bootstrap_ready()
        start_port_forward()
        init_flytectl()
        register_entities()
        return 0

    if args.command in EXECUTION_COMMANDS:
        execute_launch_plan(
            args.command,
            force_immediate_run=getattr(args, "force_immediate_run", False),
            latest=getattr(args, "latest", None),
            version=getattr(args, "version", None),
        )
        return 0

    fatal(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())