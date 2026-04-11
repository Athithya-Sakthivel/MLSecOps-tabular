#!/usr/bin/env python3
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
TRAIN_PACKAGE_ROOT = SRC_ROOT / "workflows" / "train"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_bool(name: str, default: str = "0") -> bool:
    return _env_str(name, default).lower() in {"1", "true", "yes", "y", "on"}


REMOTE_PROJECT = _env_str("REMOTE_PROJECT", "flytesnacks")
REMOTE_DOMAIN = _env_str("REMOTE_DOMAIN", "development")
TASK_NAMESPACE = _env_str("TRAIN_NAMESPACE", f"{REMOTE_PROJECT}-{REMOTE_DOMAIN}")

K8S_CLUSTER = _env_str("K8S_CLUSTER", "kind").lower()

TRAIN_PROFILE = _env_str(
    "TRAIN_PROFILE",
    _env_str(
        "ELT_PROFILE",
        "staging" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod",
    ),
).lower()

if TRAIN_PROFILE not in {"staging", "prod"}:
    raise RuntimeError(f"TRAIN_PROFILE must be 'staging' or 'prod', got {TRAIN_PROFILE!r}")

TRAIN_SERVICE_ACCOUNT = _env_str("TRAIN_SERVICE_ACCOUNT", "ray") or "ray"

TRAIN_TASK_IMAGE = _env_str("TRAIN_TASK_IMAGE")
if not TRAIN_TASK_IMAGE:
    raise RuntimeError("TRAIN_TASK_IMAGE environment variable must be set and non-empty")

WORKFLOW_SOURCE_FILE = TRAIN_PACKAGE_ROOT / "launch_plans.py"
WORKFLOW_SOURCE_REL = WORKFLOW_SOURCE_FILE.relative_to(SRC_ROOT)
WORKFLOW_IMPORT_MODULE = _env_str("WORKFLOW_IMPORT_MODULE", "workflows.train.launch_plans")

USE_PORT_FORWARD = _env_bool("USE_PORT_FORWARD", "1")
FLYTE_ADMIN_NAMESPACE = _env_str("FLYTE_ADMIN_NAMESPACE", "flyte")
FLYTE_ADMIN_HOST = _env_str("FLYTE_ADMIN_HOST", "127.0.0.1")
FLYTE_ADMIN_PORT = int(_env_str("FLYTE_ADMIN_PORT", "30081"))
PORT_FORWARD_TARGET_PORT = int(_env_str("PORT_FORWARD_TARGET_PORT", "81"))
PORT_FORWARD_PID_FILE = Path(_env_str("PORT_FORWARD_PID_FILE", "/tmp/flyteadmin-portforward-train.pid"))
PORT_FORWARD_LOG = Path(_env_str("PORT_FORWARD_LOG", "/tmp/flyteadmin-portforward-train.log"))

PORT_FORWARD_PROC: subprocess.Popen[str] | None = None

ACTIVATE_LAUNCHPLANS = _env_bool("ACTIVATE_LAUNCHPLANS", "0")
USE_LATEST = _env_bool("USE_LATEST", "0")

PYFLYTE_REGISTER_EXTRA_ARGS = _env_str("PYFLYTE_REGISTER_EXTRA_ARGS", "")

RESOURCE_QUOTA_NAME = _env_str("TRAIN_RESOURCE_QUOTA_NAME", "ray-workload-quota")

RESOURCE_QUOTA_KIND_REQUESTS_CPU = _env_str("TRAIN_RESOURCE_QUOTA_KIND_REQUESTS_CPU", "8")
RESOURCE_QUOTA_KIND_REQUESTS_MEMORY = _env_str("TRAIN_RESOURCE_QUOTA_KIND_REQUESTS_MEMORY", "4Gi")
RESOURCE_QUOTA_KIND_LIMITS_CPU = _env_str("TRAIN_RESOURCE_QUOTA_KIND_LIMITS_CPU", "16")
RESOURCE_QUOTA_KIND_LIMITS_MEMORY = _env_str("TRAIN_RESOURCE_QUOTA_KIND_LIMITS_MEMORY", "4Gi")
RESOURCE_QUOTA_KIND_PODS = _env_str("TRAIN_RESOURCE_QUOTA_KIND_PODS", "60")
RESOURCE_QUOTA_KIND_PVC = _env_str("TRAIN_RESOURCE_QUOTA_KIND_PVC", "40")
RESOURCE_QUOTA_KIND_SERVICES = _env_str("TRAIN_RESOURCE_QUOTA_KIND_SERVICES", "50")

RESOURCE_QUOTA_EKS_REQUESTS_CPU = _env_str("TRAIN_RESOURCE_QUOTA_EKS_REQUESTS_CPU", "24")
RESOURCE_QUOTA_EKS_REQUESTS_MEMORY = _env_str("TRAIN_RESOURCE_QUOTA_EKS_REQUESTS_MEMORY", "4Gi")
RESOURCE_QUOTA_EKS_LIMITS_CPU = _env_str("TRAIN_RESOURCE_QUOTA_EKS_LIMITS_CPU", "48")
RESOURCE_QUOTA_EKS_LIMITS_MEMORY = _env_str("TRAIN_RESOURCE_QUOTA_EKS_LIMITS_MEMORY", "4Gi")
RESOURCE_QUOTA_EKS_PODS = _env_str("TRAIN_RESOURCE_QUOTA_EKS_PODS", "150")
RESOURCE_QUOTA_EKS_PVC = _env_str("TRAIN_RESOURCE_QUOTA_EKS_PVC", "80")
RESOURCE_QUOTA_EKS_SERVICES = _env_str("TRAIN_RESOURCE_QUOTA_EKS_SERVICES", "150")

TRAIN_TASK_REQUESTS_CPU = _env_str("TRAIN_TASK_REQUESTS_CPU", "2")
TRAIN_TASK_REQUESTS_MEMORY = _env_str("TRAIN_TASK_REQUESTS_MEMORY", "3Gi")
TRAIN_TASK_LIMITS_CPU = _env_str("TRAIN_TASK_LIMITS_CPU", "3")
TRAIN_TASK_LIMITS_MEMORY = _env_str("TRAIN_TASK_LIMITS_MEMORY", "3Gi")
TRAIN_DEFAULT_NUM_THREADS = _env_str("TRAIN_DEFAULT_NUM_THREADS", "2")


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}", file=sys.stderr, flush=True)


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


def stop_port_forward_if_any() -> None:
    global PORT_FORWARD_PROC

    proc = PORT_FORWARD_PROC
    PORT_FORWARD_PROC = None

    pid: int | None = None
    if PORT_FORWARD_PID_FILE.is_file():
        try:
            pid = int(PORT_FORWARD_PID_FILE.read_text().strip())
        except Exception:
            pid = None

    if proc is not None:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)

    if pid is not None and (proc is None or proc.pid != pid):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
        with contextlib.suppress(Exception):
            os.waitpid(pid, 0)

    PORT_FORWARD_PID_FILE.unlink(missing_ok=True)


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

    log_file = PORT_FORWARD_LOG.open("a", encoding="utf-8")
    try:
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
    finally:
        log_file.close()

    PORT_FORWARD_PROC = proc
    PORT_FORWARD_PID_FILE.write_text(str(proc.pid))

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


def _bootstrap_manifest() -> str:
    q = _quota_values()
    return f"""apiVersion: v1
kind: ServiceAccount
metadata:
  name: {TRAIN_SERVICE_ACCOUNT}
  namespace: {TASK_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {TRAIN_SERVICE_ACCOUNT}
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
  name: {TRAIN_SERVICE_ACCOUNT}
  namespace: {TASK_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: {TRAIN_SERVICE_ACCOUNT}
    namespace: {TASK_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {TRAIN_SERVICE_ACCOUNT}
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
            f"--as=system:serviceaccount:{TASK_NAMESPACE}:{TRAIN_SERVICE_ACCOUNT}",
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
        ["kubectl", "get", "serviceaccount", TRAIN_SERVICE_ACCOUNT, "-n", TASK_NAMESPACE],
        check=False,
        capture_output=True,
    )
    if sa.returncode != 0:
        fatal(f"service account {TRAIN_SERVICE_ACCOUNT} missing in {TASK_NAMESPACE}")

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
        fatal("service account lacks required Ray/Flyte permissions: " + ", ".join(missing))

    log(f"Bootstrap verified for {TASK_NAMESPACE}")


def lint_sources() -> None:
    require_bin("ruff")
    log(f"Running ruff on {TRAIN_PACKAGE_ROOT}")
    run_cmd(["ruff", "check", str(TRAIN_PACKAGE_ROOT)])


def _export_resource_env_defaults() -> None:
    os.environ.setdefault("TRAIN_TASK_REQUESTS_CPU", TRAIN_TASK_REQUESTS_CPU)
    os.environ.setdefault("TRAIN_TASK_REQUESTS_MEMORY", TRAIN_TASK_REQUESTS_MEMORY)
    os.environ.setdefault("TRAIN_TASK_LIMITS_CPU", TRAIN_TASK_LIMITS_CPU)
    os.environ.setdefault("TRAIN_TASK_LIMITS_MEMORY", TRAIN_TASK_LIMITS_MEMORY)
    os.environ.setdefault("TRAIN_DEFAULT_NUM_THREADS", TRAIN_DEFAULT_NUM_THREADS)


def import_check() -> None:
    _export_resource_env_defaults()

    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    if not hasattr(mod, "TRAIN_WORKFLOW_LP"):
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose TRAIN_WORKFLOW_LP")
    if not hasattr(mod, "TRAIN_WORKFLOW_LP_NAME"):
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose TRAIN_WORKFLOW_LP_NAME")
    log("import_ok")


def resolve_train_launchplan_name() -> str:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)

    lp_name = getattr(mod, "TRAIN_WORKFLOW_LP_NAME", None)
    if isinstance(lp_name, str) and lp_name.strip():
        return lp_name.strip()

    lp = getattr(mod, "TRAIN_WORKFLOW_LP", None)
    if lp is None:
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose TRAIN_WORKFLOW_LP")
    name = getattr(lp, "name", None)
    if not isinstance(name, str) or not name.strip():
        fatal("could not resolve TRAIN launch plan name")
    return name.strip()


def registration_tree_files() -> list[Path]:
    files: list[Path] = []
    for path in sorted(TRAIN_PACKAGE_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        files.append(path)
    for extra in (
        TRAIN_PACKAGE_ROOT / "requirements.txt",
        TRAIN_PACKAGE_ROOT / "Dockerfile.task_image",
    ):
        if extra.is_file():
            files.append(extra)
    return files


@functools.lru_cache(maxsize=1)
def compute_registration_version() -> str:
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()
    tree = hashlib.sha256()
    for item in (
        f"TRAIN_TASK_IMAGE={TRAIN_TASK_IMAGE}",
        f"TRAIN_PROFILE={TRAIN_PROFILE}",
        f"K8S_CLUSTER={K8S_CLUSTER}",
        f"TRAIN_TASK_REQUESTS_CPU={TRAIN_TASK_REQUESTS_CPU}",
        f"TRAIN_TASK_REQUESTS_MEMORY={TRAIN_TASK_REQUESTS_MEMORY}",
        f"TRAIN_TASK_LIMITS_CPU={TRAIN_TASK_LIMITS_CPU}",
        f"TRAIN_TASK_LIMITS_MEMORY={TRAIN_TASK_LIMITS_MEMORY}",
        f"TRAIN_DEFAULT_NUM_THREADS={TRAIN_DEFAULT_NUM_THREADS}",
    ):
        tree.update(item.encode())
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
    register_env["TRAIN_TASK_IMAGE"] = TRAIN_TASK_IMAGE
    register_env["TRAIN_TASK_REQUESTS_CPU"] = TRAIN_TASK_REQUESTS_CPU
    register_env["TRAIN_TASK_REQUESTS_MEMORY"] = TRAIN_TASK_REQUESTS_MEMORY
    register_env["TRAIN_TASK_LIMITS_CPU"] = TRAIN_TASK_LIMITS_CPU
    register_env["TRAIN_TASK_LIMITS_MEMORY"] = TRAIN_TASK_LIMITS_MEMORY
    register_env["TRAIN_DEFAULT_NUM_THREADS"] = TRAIN_DEFAULT_NUM_THREADS
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
        TRAIN_TASK_IMAGE,
        "--version",
        registration_version,
    ]

    if pyflyte_register_supports_service_account():
        cmd.extend(["--service-account", TRAIN_SERVICE_ACCOUNT])

    supports_copy, supports_fast = pyflyte_register_supports_copy_or_fast_flag()
    if supports_copy:
        cmd.extend(["--copy", "none"])
    elif supports_fast:
        cmd.append("--fast=false")
    else:
        fatal("installed pyflyte register does not advertise --copy or --fast; cannot safely disable fast registration")

    if PYFLYTE_REGISTER_EXTRA_ARGS:
        cmd.extend(shlex.split(PYFLYTE_REGISTER_EXTRA_ARGS))

    if ACTIVATE_LAUNCHPLANS:
        cmd.append("--activate-launchplans")

    cmd.append(str(WORKFLOW_SOURCE_REL))
    return cmd


def register_entities() -> str:
    require_bin("pyflyte")
    if not WORKFLOW_SOURCE_FILE.is_file():
        fatal(f"workflow source file not found: {WORKFLOW_SOURCE_FILE}")

    registration_version = compute_registration_version()
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()

    log(f"Registering TRAIN from commit {git_sha}")
    log(f"Workflow import module: {WORKFLOW_IMPORT_MODULE}")
    log(f"Source file: {WORKFLOW_SOURCE_FILE}")
    log(f"Profile: {TRAIN_PROFILE} | Cluster: {K8S_CLUSTER} | Namespace: {TASK_NAMESPACE}")
    log(f"Task image: {TRAIN_TASK_IMAGE}")
    log(
        "Task resource defaults: "
        f"requests={TRAIN_TASK_REQUESTS_CPU}/{TRAIN_TASK_REQUESTS_MEMORY} "
        f"limits={TRAIN_TASK_LIMITS_CPU}/{TRAIN_TASK_LIMITS_MEMORY}"
    )
    log(f"Registration version: {registration_version}")

    register_env = build_register_env()
    cmd = build_register_command(registration_version)
    run_cmd(cmd, cwd=SRC_ROOT, env=register_env)

    log(f"Registration complete for version {registration_version}")
    return registration_version


def require_preflight_for_execution() -> None:
    ensure_namespace_bootstrap_ready()


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
            fatal("a launch-plan version is required when latest is disabled")
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


def execute_launch_plan(*, latest: bool | None = None, version: str | None = None) -> None:
    require_bin("kubectl")
    require_bin("flytectl")

    require_preflight_for_execution()

    start_port_forward()
    init_flytectl()

    launch_plan_name = resolve_train_launchplan_name()
    effective_latest = USE_LATEST if latest is None else latest
    effective_version = version if version is not None else (None if effective_latest else compute_registration_version())

    if not effective_latest and not effective_version:
        fatal("launch-plan version required when latest is disabled")

    with tempfile.TemporaryDirectory(prefix=f"{launch_plan_name}.") as tmpdir:
        exec_spec_file = Path(tmpdir) / "exec.yaml"
        log(f"Launching: {launch_plan_name}")
        fetch_launch_plan_exec_spec(
            launch_plan_name,
            exec_spec_file,
            latest=effective_latest,
            version=effective_version,
        )
        log(f"Creating execution from {exec_spec_file}")
        create_execution_from_spec(exec_spec_file)


def register_and_run() -> None:
    registration_version = register_entities()
    execute_launch_plan(version=registration_version)


def get_execution_pods(execution_id: str) -> list[str]:
    cp = run_cmd(
        [
            "kubectl",
            "get",
            "pods",
            "-n",
            TASK_NAMESPACE,
            "-l",
            f"execution-id={execution_id}",
            "-o",
            'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
        ],
        check=False,
        capture_output=True,
    )
    pods = [line.strip() for line in cp.stdout.splitlines() if line.strip()]
    if pods:
        return pods

    cp = run_cmd(
        [
            "kubectl",
            "get",
            "pods",
            "-n",
            TASK_NAMESPACE,
            "-o",
            'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
        ],
        check=False,
        capture_output=True,
    )
    return [line.strip() for line in cp.stdout.splitlines() if execution_id in line]


def get_execution_rayjobs(execution_id: str) -> list[str]:
    cp = run_cmd(
        [
            "kubectl",
            "get",
            "rayjobs",
            "-n",
            TASK_NAMESPACE,
            "-l",
            f"execution-id={execution_id}",
            "-o",
            'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
        ],
        check=False,
        capture_output=True,
    )
    jobs = [line.strip() for line in cp.stdout.splitlines() if line.strip()]
    if jobs:
        return jobs

    cp = run_cmd(
        [
            "kubectl",
            "get",
            "rayjobs",
            "-n",
            TASK_NAMESPACE,
            "-o",
            'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
        ],
        check=False,
        capture_output=True,
    )
    return [line.strip() for line in cp.stdout.splitlines() if execution_id in line]


def diagnose_execution(execution_id: str) -> None:
    start_port_forward()
    init_flytectl()

    print("=== EXECUTION ===")
    run_cmd(
        [
            "flytectl",
            "get",
            "execution",
            execution_id,
            "-p",
            REMOTE_PROJECT,
            "-d",
            REMOTE_DOMAIN,
        ],
        check=False,
    )

    print("=== EXECUTION DETAILS ===")
    run_cmd(
        [
            "flytectl",
            "get",
            "execution",
            execution_id,
            "-p",
            REMOTE_PROJECT,
            "-d",
            REMOTE_DOMAIN,
            "--details",
        ],
        check=False,
    )

    pods = get_execution_pods(execution_id)
    if pods:
        print("=== MATCHING PODS ===")
        run_cmd(["kubectl", "get", "pods", "-n", TASK_NAMESPACE, "-o", "wide"], check=False)
        for pod in pods:
            print(f"--- POD {pod} ---")
            run_cmd(["kubectl", "describe", "pod", pod, "-n", TASK_NAMESPACE], check=False)
            run_cmd(
                ["kubectl", "logs", pod, "-n", TASK_NAMESPACE, "--all-containers=true", "--tail=120"],
                check=False,
            )
    else:
        print(f"No pod matched execution {execution_id}")

    jobs = get_execution_rayjobs(execution_id)
    if jobs:
        print("=== MATCHING RAYJOBS ===")
        run_cmd(["kubectl", "get", "rayjobs", "-n", TASK_NAMESPACE, "-o", "wide"], check=False)
        for job in jobs:
            print(f"--- RAYJOB {job} ---")
            run_cmd(["kubectl", "describe", "rayjob", job, "-n", TASK_NAMESPACE], check=False)
    else:
        print(f"No RayJob matched execution {execution_id}")


def delete_execution(execution_id: str) -> None:
    start_port_forward()
    init_flytectl()

    log(f"Deleting execution {execution_id}")
    run_cmd(
        ["flytectl", "delete", "execution", execution_id, "-p", REMOTE_PROJECT, "-d", REMOTE_DOMAIN],
        check=False,
    )

    run_cmd(
        [
            "kubectl",
            "delete",
            "rayjob",
            "-n",
            TASK_NAMESPACE,
            "-l",
            f"execution-id={execution_id}",
            "--ignore-not-found=true",
        ],
        check=False,
    )
    run_cmd(
        [
            "kubectl",
            "delete",
            "pod",
            "-n",
            TASK_NAMESPACE,
            "-l",
            f"execution-id={execution_id}",
            "--ignore-not-found=true",
        ],
        check=False,
    )


def cleanup_stale_resources() -> None:
    run_cmd(
        [
            "kubectl",
            "delete",
            "rayjob",
            "-n",
            TASK_NAMESPACE,
            "-l",
            "execution-id",
            "--ignore-not-found=true",
        ],
        check=False,
    )
    run_cmd(
        [
            "kubectl",
            "delete",
            "pod",
            "-n",
            TASK_NAMESPACE,
            "-l",
            "execution-id",
            "--ignore-not-found=true",
        ],
        check=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Register and execute the train workflow.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("register", help="Register train workflows and launch plans")
    sub.add_parser("train", help="Execute the train workflow")
    sub.add_parser("up", help="Register and then execute train")
    sub.add_parser("diagnose", help="Inspect a Flyte execution and related Kubernetes resources").add_argument("execution_id")
    sub.add_parser("delete", help="Delete a Flyte execution and matching Kubernetes resources").add_argument("execution_id")
    sub.add_parser("reset", help="Delete leftover train Ray / pod resources in the target namespace")
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

    if args.command == "train":
        execute_launch_plan()
        return 0

    if args.command == "up":
        lint_sources()
        import_check()
        ensure_namespace_bootstrap_ready()
        start_port_forward()
        init_flytectl()
        register_and_run()
        return 0

    if args.command == "diagnose":
        diagnose_execution(args.execution_id)
        return 0

    if args.command == "delete":
        delete_execution(args.execution_id)
        return 0

    if args.command == "reset":
        cleanup_stale_resources()
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())