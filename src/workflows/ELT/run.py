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
TASK_NAMESPACE = os.environ.get("TASK_NAMESPACE", f"{REMOTE_PROJECT}-{REMOTE_DOMAIN}").strip() or f"{REMOTE_PROJECT}-{REMOTE_DOMAIN}"

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark").strip() or "spark"

ELT_TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:2026-04-04-19-11--4f0f141@sha256:5aca09c143d6060764711d20d56e3140c5cde29d7b512134df72193ec2545724",
).strip()
if not ELT_TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must not be empty")
os.environ["ELT_TASK_IMAGE"] = ELT_TASK_IMAGE

WORKFLOW_SOURCE_FILE = SRC_ROOT / "workflows" / "ELT" / "launch_plans.py"
WORKFLOW_SOURCE_REL = WORKFLOW_SOURCE_FILE.relative_to(SRC_ROOT)
WORKFLOW_IMPORT_MODULE = os.environ.get("WORKFLOW_IMPORT_MODULE", "workflows.ELT.launch_plans").strip() or "workflows.ELT.launch_plans"

USE_PORT_FORWARD = os.environ.get("USE_PORT_FORWARD", "1").lower() in {"1", "true", "yes", "y", "on"}
FLYTE_ADMIN_NAMESPACE = os.environ.get("FLYTE_ADMIN_NAMESPACE", "flyte").strip() or "flyte"
FLYTE_ADMIN_HOST = os.environ.get("FLYTE_ADMIN_HOST", "127.0.0.1").strip() or "127.0.0.1"
FLYTE_ADMIN_PORT = int(os.environ.get("FLYTE_ADMIN_PORT", "30081"))
PORT_FORWARD_TARGET_PORT = int(os.environ.get("PORT_FORWARD_TARGET_PORT", "81"))
PORT_FORWARD_LOG = Path(os.environ.get("PORT_FORWARD_LOG", "/tmp/flyteadmin-portforward.log"))

PYFLYTE_REGISTER_EXTRA_ARGS = os.environ.get("PYFLYTE_REGISTER_EXTRA_ARGS", "").strip()

RESOURCE_QUOTA_NAME = "spark-workload-quota"

QUOTA_PRESETS: dict[str, dict[str, str]] = {
    "kind": {
        "requests_cpu": "4",
        "requests_memory": "8Gi",
        "limits_cpu": "8",
        "limits_memory": "12Gi",
        "pods": "30",
        "persistentvolumeclaims": "20",
        "services": "40",
    },
    "eks": {
        "requests_cpu": "12",
        "requests_memory": "24Gi",
        "limits_cpu": "24",
        "limits_memory": "48Gi",
        "pods": "100",
        "persistentvolumeclaims": "50",
        "services": "100",
    },
}

PROFILE_PRESETS: dict[str, dict[str, str]] = {
    "kind": {
        "ELT_PROFILE": "staging",
        "ICEBERG_EXPIRE_DAYS": "0",
        "ICEBERG_ORPHAN_DAYS": "0",
        "ICEBERG_RETAIN_LAST": "1",
        "MAINTENANCE_ENABLE_REWRITE": "0",
        "ICEBERG_MAINTENANCE_DAILY_CRON": "*/30 * * * *",
        "ICEBERG_MAINTENANCE_WEEKLY_CRON": "0 */3 * * *,45 */3 * * *",
    },
    "eks": {
        "ELT_PROFILE": "prod",
        "ICEBERG_EXPIRE_DAYS": "7",
        "ICEBERG_ORPHAN_DAYS": "3",
        "ICEBERG_RETAIN_LAST": "2",
        "MAINTENANCE_ENABLE_REWRITE": "1",
        "ICEBERG_MAINTENANCE_DAILY_CRON": "15 */6 * * *",
        "ICEBERG_MAINTENANCE_WEEKLY_CRON": "45 2 * * *",
    },
}

ELT_LAUNCH_PLAN_CANDIDATES = ("ELT_WORKFLOW_LP_NAME", "ELT_WORKFLOW_LP")
MAINTENANCE_DAILY_LP_CANDIDATES = (
    "ICEBERG_MAINTENANCE_DAILY_LP_NAME",
    "ICEBERG_MAINTENANCE_DAILY_LP",
    "ICEBERG_MAINTENANCE_HOURLY_LP_NAME",
    "ICEBERG_MAINTENANCE_HOURLY_LP",
)
MAINTENANCE_WEEKLY_LP_CANDIDATES = (
    "ICEBERG_MAINTENANCE_WEEKLY_LP_NAME",
    "ICEBERG_MAINTENANCE_WEEKLY_LP",
    "ICEBERG_MAINTENANCE_COMPACTION_LP_NAME",
    "ICEBERG_MAINTENANCE_COMPACTION_LP",
)

REGISTRATION_ENV_KEYS = (
    "ELT_PROFILE",
    "ICEBERG_EXPIRE_DAYS",
    "ICEBERG_ORPHAN_DAYS",
    "ICEBERG_RETAIN_LAST",
    "MAINTENANCE_ENABLE_REWRITE",
    "ICEBERG_MAINTENANCE_DAILY_CRON",
    "ICEBERG_MAINTENANCE_WEEKLY_CRON",
)

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

def configure_runtime() -> None:
    preset = PROFILE_PRESETS.get(K8S_CLUSTER)
    if preset is None:
        fatal(f"unsupported K8S_CLUSTER={K8S_CLUSTER!r}; expected one of {sorted(PROFILE_PRESETS)}")

    for key, value in preset.items():
        os.environ[key] = value

    log(
        "Configured runtime "
        f"cluster={K8S_CLUSTER} "
        f"profile={os.environ['ELT_PROFILE']} "
        f"expire_days={os.environ['ICEBERG_EXPIRE_DAYS']} "
        f"orphan_days={os.environ['ICEBERG_ORPHAN_DAYS']} "
        f"retain_last={os.environ['ICEBERG_RETAIN_LAST']} "
        f"rewrite={os.environ['MAINTENANCE_ENABLE_REWRITE']}"
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

def _resolve_launch_plan_name(module: object, candidates: Sequence[str]) -> str:
    return _module_attr_name(module, candidates)

def import_check(*, need_schedule: bool) -> None:
    try:
        mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    except Exception as exc:
        fatal(f"failed to import {WORKFLOW_IMPORT_MODULE}: {exc}")

    try:
        _resolve_launch_plan_name(mod, ELT_LAUNCH_PLAN_CANDIDATES)
    except Exception as exc:
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose ELT launch plan: {exc}")

    if need_schedule:
        try:
            _resolve_launch_plan_name(mod, MAINTENANCE_DAILY_LP_CANDIDATES)
            _resolve_launch_plan_name(mod, MAINTENANCE_WEEKLY_LP_CANDIDATES)
        except Exception as exc:
            fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose maintenance launch plans: {exc}")

    log("import_ok")

def _quota_values() -> dict[str, str]:
    return QUOTA_PRESETS[K8S_CLUSTER]

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

def dedupe_paths(items: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out

def registration_tree_files() -> list[Path]:
    files: list[Path] = [WORKFLOW_SOURCE_FILE]
    for root in (
        SRC_ROOT / "workflows" / "ELT" / "tasks",
        SRC_ROOT / "workflows" / "ELT" / "workflows",
    ):
        if root.is_dir():
            files.extend(sorted(p for p in root.rglob("*.py") if p.is_file()))
    return [path for path in dedupe_paths(files) if path.is_file()]

@functools.lru_cache(maxsize=1)
def compute_registration_version() -> str:
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()
    tree = hashlib.sha256()
    tree.update(f"ELT_TASK_IMAGE={ELT_TASK_IMAGE}".encode())
    tree.update(b"\0")
    tree.update(f"ELT_PROFILE={os.environ.get('ELT_PROFILE', '')}".encode())
    tree.update(b"\0")
    tree.update(f"K8S_CLUSTER={K8S_CLUSTER}".encode())
    tree.update(b"\0")
    for key in REGISTRATION_ENV_KEYS:
        tree.update(f"{key}={os.environ.get(key, '')}".encode())
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

def register_entities() -> str:
    require_bin("pyflyte")
    if not WORKFLOW_SOURCE_FILE.is_file():
        fatal(f"workflow source file not found: {WORKFLOW_SOURCE_FILE}")

    registration_version = compute_registration_version()
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()

    log(f"Registering ELT from commit {git_sha}")
    log(f"Workflow import module: {WORKFLOW_IMPORT_MODULE}")
    log(f"Source file: {WORKFLOW_SOURCE_FILE}")
    log(f"Cluster: {K8S_CLUSTER} | Namespace: {TASK_NAMESPACE}")
    log(f"Task image: {ELT_TASK_IMAGE}")
    log(f"Registration version: {registration_version}")

    register_env = build_register_env()
    cmd = build_register_command(registration_version)
    run_cmd(cmd, cwd=SRC_ROOT, env=register_env)

    log(f"Registration complete for version {registration_version}")
    return registration_version

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

def _activate_launch_plan(launch_plan_name: str, version: str) -> None:
    log(f"Activating launch plan: {launch_plan_name}")
    run_cmd(
        [
            "flytectl",
            "update",
            "launchplan",
            "-p",
            REMOTE_PROJECT,
            "-d",
            REMOTE_DOMAIN,
            launch_plan_name,
            "--version",
            version,
            "--activate",
            "--force",
        ]
    )

def activate_scheduled_launchplans(version: str) -> None:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    daily_lp = _resolve_launch_plan_name(mod, MAINTENANCE_DAILY_LP_CANDIDATES)
    weekly_lp = _resolve_launch_plan_name(mod, MAINTENANCE_WEEKLY_LP_CANDIDATES)
    _activate_launch_plan(daily_lp, version)
    _activate_launch_plan(weekly_lp, version)

def run_elt_workflow(version: str) -> None:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    launch_plan_name = _resolve_launch_plan_name(mod, ELT_LAUNCH_PLAN_CANDIDATES)

    with tempfile.TemporaryDirectory(prefix=f"{launch_plan_name}.") as tmpdir:
        exec_spec_file = Path(tmpdir) / "exec.yaml"
        log(f"Launching: {launch_plan_name}")
        fetch_launch_plan_exec_spec(launch_plan_name, exec_spec_file, latest=False, version=version)
        log(f"Creating execution from {exec_spec_file}")
        create_execution_from_spec(exec_spec_file)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Register ELT workflows and optionally launch ELT and/or scheduled maintenance.",
    )
    parser.add_argument(
        "commands",
        nargs="+",
        choices=("elt", "schedule"),
        help="Use 'elt', 'schedule', or both as 'elt schedule'.",
    )
    return parser

def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    commands = list(dict.fromkeys(args.commands))
    if len(commands) != len(args.commands):
        fatal("duplicate command tokens are not allowed")
    want_elt = "elt" in commands
    want_schedule = "schedule" in commands

    configure_runtime()

    require_bin("kubectl")
    require_bin("git")
    require_bin("python3")

    lint_sources()
    import_check(need_schedule=want_schedule)
    ensure_namespace_bootstrap_ready()

    registration_version = register_entities()

    start_port_forward()
    init_flytectl()

    if want_schedule:
        activate_scheduled_launchplans(registration_version)
    if want_elt:
        run_elt_workflow(registration_version)

    if want_schedule and want_elt:
        log("elt schedule selected; registration, activation, and ELT execution complete")
    elif want_schedule:
        log("schedule selected; registration and scheduled launch-plan activation complete")
    else:
        log("elt selected; registration and ELT execution complete")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())