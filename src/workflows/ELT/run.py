from __future__ import annotations

import argparse
import atexit
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
from pathlib import Path
from typing import NoReturn, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
ELT_ROOT = SRC_ROOT / "workflows" / "ELT"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

REMOTE_PROJECT = os.environ.get("REMOTE_PROJECT", "flytesnacks")
REMOTE_DOMAIN = os.environ.get("REMOTE_DOMAIN", "development")
TASK_NAMESPACE = os.environ.get("TASK_NAMESPACE", f"{REMOTE_PROJECT}-{REMOTE_DOMAIN}")

K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "kind").strip().lower()
ELT_PROFILE = (
    os.environ.get("ELT_PROFILE")
    or ("dev" if K8S_CLUSTER in {"kind", "minikube", "docker-desktop", "local"} else "prod")
).strip().lower()

SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
ELT_TASK_IMAGE = os.environ.get(
    "ELT_TASK_IMAGE",
    "ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-29-07-26--4162406@sha256:79ab860f821f3d26a08ab9f4c53e19c5ef63d42e93c4cd2d2b00d4f9b6d160f8",
).strip()
if not ELT_TASK_IMAGE:
    raise RuntimeError("ELT_TASK_IMAGE must not be empty")

WORKFLOW_SOURCE_FILE = SRC_ROOT / "workflows" / "ELT" / "launch_plans.py"
WORKFLOW_SOURCE_REL = WORKFLOW_SOURCE_FILE.relative_to(SRC_ROOT)
WORKFLOW_IMPORT_MODULE = os.environ.get("WORKFLOW_IMPORT_MODULE", "workflows.ELT.launch_plans")

USE_PORT_FORWARD = os.environ.get("USE_PORT_FORWARD", "1").lower() in {"1", "true", "yes", "y", "on"}
FLYTE_ADMIN_NAMESPACE = os.environ.get("FLYTE_ADMIN_NAMESPACE", "flyte")
FLYTE_ADMIN_HOST = os.environ.get("FLYTE_ADMIN_HOST", "127.0.0.1")
FLYTE_ADMIN_PORT = int(os.environ.get("FLYTE_ADMIN_PORT", "30081"))
PORT_FORWARD_TARGET_PORT = int(os.environ.get("PORT_FORWARD_TARGET_PORT", "81"))
PORT_FORWARD_PID_FILE = Path(os.environ.get("PORT_FORWARD_PID_FILE", "/tmp/flyteadmin-portforward.pid"))
PORT_FORWARD_LOG = Path(os.environ.get("PORT_FORWARD_LOG", "/tmp/flyteadmin-portforward.log"))

ACTIVATE_LAUNCHPLANS = os.environ.get("ACTIVATE_LAUNCHPLANS", "0").lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
USE_LATEST = os.environ.get("USE_LATEST", "0").lower() in {"1", "true", "yes", "y", "on"}

# Optional extra args appended to `pyflyte register`.
PYFLYTE_REGISTER_EXTRA_ARGS = os.environ.get("PYFLYTE_REGISTER_EXTRA_ARGS", "").strip()

RESOURCE_QUOTA_NAME = os.environ.get("RESOURCE_QUOTA_NAME", "spark-workload-quota")
RESOURCE_QUOTA_KIND_REQUESTS_CPU = os.environ.get("RESOURCE_QUOTA_KIND_REQUESTS_CPU", "2")
RESOURCE_QUOTA_KIND_REQUESTS_MEMORY = os.environ.get("RESOURCE_QUOTA_KIND_REQUESTS_MEMORY", "1536Mi")
RESOURCE_QUOTA_KIND_LIMITS_CPU = os.environ.get("RESOURCE_QUOTA_KIND_LIMITS_CPU", "4")
RESOURCE_QUOTA_KIND_LIMITS_MEMORY = os.environ.get("RESOURCE_QUOTA_KIND_LIMITS_MEMORY", "3000Mi")
RESOURCE_QUOTA_KIND_PODS = os.environ.get("RESOURCE_QUOTA_KIND_PODS", "20")

RESOURCE_QUOTA_EKS_REQUESTS_CPU = os.environ.get("RESOURCE_QUOTA_EKS_REQUESTS_CPU", "8")
RESOURCE_QUOTA_EKS_REQUESTS_MEMORY = os.environ.get("RESOURCE_QUOTA_EKS_REQUESTS_MEMORY", "16Gi")
RESOURCE_QUOTA_EKS_LIMITS_CPU = os.environ.get("RESOURCE_QUOTA_EKS_LIMITS_CPU", "16")
RESOURCE_QUOTA_EKS_LIMITS_MEMORY = os.environ.get("RESOURCE_QUOTA_EKS_LIMITS_MEMORY", "32Gi")
RESOURCE_QUOTA_EKS_PODS = os.environ.get("RESOURCE_QUOTA_EKS_PODS", "100")


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
    )
    if check and cp.returncode != 0:
        detail = []
        if cp.stdout:
            detail.append(f"stdout:\n{cp.stdout.rstrip()}")
        if cp.stderr:
            detail.append(f"stderr:\n{cp.stderr.rstrip()}")
        suffix = f"\n{'\n'.join(detail)}" if detail else ""
        raise RuntimeError(f"command failed ({cp.returncode}): {' '.join(args)}{suffix}")
    return cp


def stop_port_forward_if_any() -> None:
    if not PORT_FORWARD_PID_FILE.is_file():
        return
    try:
        pid = int(PORT_FORWARD_PID_FILE.read_text().strip())
    except Exception:
        PORT_FORWARD_PID_FILE.unlink(missing_ok=True)
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    PORT_FORWARD_PID_FILE.unlink(missing_ok=True)


def cleanup() -> None:
    if USE_PORT_FORWARD:
        stop_port_forward_if_any()


atexit.register(cleanup)


def port_is_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def start_port_forward() -> None:
    if not USE_PORT_FORWARD:
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
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        PORT_FORWARD_PID_FILE.write_text(str(proc.pid))

        for _ in range(60):
            if port_is_open(FLYTE_ADMIN_HOST, FLYTE_ADMIN_PORT):
                return
            if proc.poll() is not None:
                break
            time.sleep(1)

    tail = ""
    if PORT_FORWARD_LOG.is_file():
        try:
            tail_lines = PORT_FORWARD_LOG.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-20:]
            tail = "\n".join(tail_lines)
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


def import_check() -> None:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    if not hasattr(mod, "ELT_WORKFLOW_LP"):
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose ELT_WORKFLOW_LP")
    log("import_ok")


def resource_quota_expected() -> bool:
    cp = run_cmd(
        ["kubectl", "get", "resourcequota", RESOURCE_QUOTA_NAME, "-n", TASK_NAMESPACE, "-o", "name"],
        check=False,
        capture_output=True,
    )
    return bool(cp.stdout.strip())


def ensure_namespace_bootstrap_ready() -> None:
    require_bin("kubectl")
    log(f"Verifying namespace bootstrap for {TASK_NAMESPACE}")

    ns = run_cmd(
        ["kubectl", "get", "namespace", TASK_NAMESPACE],
        check=False,
        capture_output=True,
    )
    if ns.returncode != 0:
        fatal(
            f"namespace {TASK_NAMESPACE} does not exist. "
            f"Run src/infra/core/spark_operator.sh --rollout first."
        )

    sa = run_cmd(
        ["kubectl", "get", "serviceaccount", SPARK_SERVICE_ACCOUNT, "-n", TASK_NAMESPACE],
        check=False,
        capture_output=True,
    )
    if sa.returncode != 0:
        fatal(
            f"service account {SPARK_SERVICE_ACCOUNT} missing in {TASK_NAMESPACE}. "
            f"Run src/infra/core/spark_operator.sh --rollout first."
        )

    if not resource_quota_expected():
        fatal(
            f"no ResourceQuota named {RESOURCE_QUOTA_NAME} found in {TASK_NAMESPACE}. "
            f"Run src/infra/core/spark_operator.sh --rollout first."
        )

    can_i = run_cmd(
        [
            "kubectl",
            "auth",
            "can-i",
            "create",
            "pods",
            "-n",
            TASK_NAMESPACE,
            f"--as=system:serviceaccount:{TASK_NAMESPACE}:{SPARK_SERVICE_ACCOUNT}",
        ],
        check=False,
        capture_output=True,
    )
    if can_i.returncode != 0 or can_i.stdout.strip() != "yes":
        fatal(
            f"service account {SPARK_SERVICE_ACCOUNT} cannot create pods in {TASK_NAMESPACE}. "
            f"Run src/infra/core/spark_operator.sh --rollout first."
        )

    log(f"Bootstrap verified for {TASK_NAMESPACE}")


def bootstrap_manifest_quota_line() -> str:
    if K8S_CLUSTER == "kind":
        return (
            "requests.cpu: "
            f"{RESOURCE_QUOTA_KIND_REQUESTS_CPU}, "
            "requests.memory: "
            f"{RESOURCE_QUOTA_KIND_REQUESTS_MEMORY}, "
            "limits.cpu: "
            f"{RESOURCE_QUOTA_KIND_LIMITS_CPU}, "
            "limits.memory: "
            f"{RESOURCE_QUOTA_KIND_LIMITS_MEMORY}, "
            "pods: "
            f"{RESOURCE_QUOTA_KIND_PODS}"
        )
    return (
        "requests.cpu: "
        f"{RESOURCE_QUOTA_EKS_REQUESTS_CPU}, "
        "requests.memory: "
        f"{RESOURCE_QUOTA_EKS_REQUESTS_MEMORY}, "
        "limits.cpu: "
        f"{RESOURCE_QUOTA_EKS_LIMITS_CPU}, "
        "limits.memory: "
        f"{RESOURCE_QUOTA_EKS_LIMITS_MEMORY}, "
        "pods: "
        f"{RESOURCE_QUOTA_EKS_PODS}"
    )


def _registration_tree_files() -> list[Path]:
    files: list[Path] = []
    for path in sorted((SRC_ROOT / "workflows" / "ELT").rglob("*.py")):
        if "__pycache__" in path.parts or "ops" in path.parts:
            continue
        files.append(path)
    return files


@functools.lru_cache(maxsize=1)
def compute_registration_version() -> str:
    git_sha = run_cmd(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()
    tree = hashlib.sha256()
    for path in _registration_tree_files():
        tree.update(path.relative_to(REPO_ROOT).as_posix().encode("utf-8"))
        tree.update(b"\0")
        tree.update(path.read_bytes())
        tree.update(b"\0")
    return f"{git_sha[:12]}-{tree.hexdigest()[:16]}"


def resolve_elt_launchplan_name() -> str:
    mod = importlib.import_module(WORKFLOW_IMPORT_MODULE)
    lp_name = getattr(mod, "ELT_WORKFLOW_LP_NAME", None)
    if isinstance(lp_name, str) and lp_name.strip():
        return lp_name.strip()
    lp = getattr(mod, "ELT_WORKFLOW_LP", None)
    if lp is None:
        fatal(f"{WORKFLOW_IMPORT_MODULE} does not expose ELT_WORKFLOW_LP")
    name = getattr(lp, "name", None)
    if not isinstance(name, str) or not name.strip():
        fatal("could not resolve ELT launch plan name")
    return name.strip()


def pyflyte_register_supports_copy_or_fast_flag() -> tuple[bool, bool]:
    help_text = run_cmd(["pyflyte", "register", "--help"], check=False, capture_output=True)
    combined = f"{help_text.stdout}\n{help_text.stderr}"
    return ("--copy" in combined, "--fast" in combined)


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
        "--service-account",
        SPARK_SERVICE_ACCOUNT,
    ]

    supports_copy, supports_fast = pyflyte_register_supports_copy_or_fast_flag()

    if supports_copy:
        cmd.extend(["--copy", "none"])
    elif supports_fast:
        cmd.append("--fast=false")
    else:
        fatal(
            "installed pyflyte register does not advertise --copy or --fast; "
            "cannot safely disable fast registration"
        )

    if PYFLYTE_REGISTER_EXTRA_ARGS:
        cmd.extend(shlex.split(PYFLYTE_REGISTER_EXTRA_ARGS))

    if ACTIVATE_LAUNCHPLANS:
        cmd.append("--activate-launchplans")

    cmd.append(str(WORKFLOW_SOURCE_REL))
    return cmd


def write_helm_values_file(values_file: Path) -> None:
    job_namespaces = [TASK_NAMESPACE]

    if K8S_CLUSTER == "kind":
        metrics = "false"
        webhook = "true"
    else:
        metrics = "true"
        webhook = "true"

    lines = [
        "hook:",
        "  upgradeCrd: true",
        "",
        "webhook:",
        f"  enable: {webhook}",
        "",
        "prometheus:",
        "  metrics:",
        f"    enable: {metrics}",
        "",
        "spark:",
        "  serviceAccount:",
        "    create: false",
        f"    name: {SPARK_SERVICE_ACCOUNT}",
        "  rbac:",
        "    create: false",
        "  jobNamespaces:",
    ]

    for ns in job_namespaces:
        lines.append(f'    - "{ns}"')

    values_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def bootstrap_target_namespace() -> None:
    log(f"Applying namespace and Spark RBAC bootstrap for {TASK_NAMESPACE}")

    run_cmd(
        [
            "kubectl",
            "apply",
            "-f",
            "-",
        ],
        input_text=f"""apiVersion: v1
apiVersion: v1
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
      - events
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
      - deletecollection   # REQUIRED for Spark cleanup
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
""",
    )

    run_cmd(
        ["kubectl", "apply", "-f", "-"],
        input_text=f"""apiVersion: v1
kind: ResourceQuota
metadata:
  name: {RESOURCE_QUOTA_NAME}
  namespace: {TASK_NAMESPACE}
spec:
  hard:
    requests.cpu: "{RESOURCE_QUOTA_KIND_REQUESTS_CPU if K8S_CLUSTER == 'kind' else RESOURCE_QUOTA_EKS_REQUESTS_CPU}"
    requests.memory: "{RESOURCE_QUOTA_KIND_REQUESTS_MEMORY if K8S_CLUSTER == 'kind' else RESOURCE_QUOTA_EKS_REQUESTS_MEMORY}"
    limits.cpu: "{RESOURCE_QUOTA_KIND_LIMITS_CPU if K8S_CLUSTER == 'kind' else RESOURCE_QUOTA_EKS_LIMITS_CPU}"
    limits.memory: "{RESOURCE_QUOTA_KIND_LIMITS_MEMORY if K8S_CLUSTER == 'kind' else RESOURCE_QUOTA_EKS_LIMITS_MEMORY}"
    pods: "{RESOURCE_QUOTA_KIND_PODS if K8S_CLUSTER == 'kind' else RESOURCE_QUOTA_EKS_PODS}"
""",
    )

    log(f"Bootstrap verified for {TASK_NAMESPACE} with quota: {bootstrap_manifest_quota_line()}")


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
    log(f"Registration version: {registration_version}")

    register_env = os.environ.copy()
    existing_pythonpath = register_env.get("PYTHONPATH", "")
    register_env["PYTHONPATH"] = str(SRC_ROOT) + (
        os.pathsep + existing_pythonpath if existing_pythonpath else ""
    )

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


def execute_launch_plan(
    *,
    latest: bool | None = None,
    version: str | None = None,
) -> None:
    require_bin("kubectl")
    require_bin("flytectl")

    require_preflight_for_execution()

    start_port_forward()
    init_flytectl()

    launch_plan_name = resolve_elt_launchplan_name()
    effective_latest = USE_LATEST if latest is None else latest
    effective_version = version if version is not None else (
        None if effective_latest else compute_registration_version()
    )

    if not effective_latest and not effective_version:
        fatal("launch-plan version required when latest is disabled")

    fd, temp_path = tempfile.mkstemp(prefix=f"{launch_plan_name}.", suffix=".yaml")
    os.close(fd)
    exec_spec_file = Path(temp_path)

    try:
        log(f"Launching: {launch_plan_name}")
        fetch_launch_plan_exec_spec(
            launch_plan_name,
            exec_spec_file,
            latest=effective_latest,
            version=effective_version,
        )
        log(f"Creating execution from {exec_spec_file}")
        create_execution_from_spec(exec_spec_file)
    finally:
        exec_spec_file.unlink(missing_ok=True)


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
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
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
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ],
        check=False,
        capture_output=True,
    )
    return [line.strip() for line in cp.stdout.splitlines() if execution_id in line]


def get_execution_sparkapps(execution_id: str) -> list[str]:
    cp = run_cmd(
        [
            "kubectl",
            "get",
            "sparkapplications",
            "-n",
            TASK_NAMESPACE,
            "-l",
            f"execution-id={execution_id}",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ],
        check=False,
        capture_output=True,
    )
    apps = [line.strip() for line in cp.stdout.splitlines() if line.strip()]
    if apps:
        return apps

    cp = run_cmd(
        [
            "kubectl",
            "get",
            "sparkapplications",
            "-n",
            TASK_NAMESPACE,
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
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

    apps = get_execution_sparkapps(execution_id)
    if apps:
        print("=== MATCHING SPARKAPPLICATIONS ===")
        run_cmd(["kubectl", "get", "sparkapplications", "-n", TASK_NAMESPACE, "-o", "wide"], check=False)
        for app in apps:
            print(f"--- SPARKAPPLICATION {app} ---")
            run_cmd(["kubectl", "describe", "sparkapplication", app, "-n", TASK_NAMESPACE], check=False)
    else:
        print(f"No SparkApplication matched execution {execution_id}")


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
            "sparkapplication",
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
            "sparkapplication",
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


def register_and_run() -> None:
    registration_version = register_entities()
    execute_launch_plan(version=registration_version)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="run.py")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("register", help="Register ELT workflows and launch plans")
    sub.add_parser("elt", help="Execute the ELT workflow")
    sub.add_parser("up", help="Register and then execute ELT")

    diag = sub.add_parser("diagnose", help="Inspect a Flyte execution and related Spark resources")
    diag.add_argument("execution_id")

    delete = sub.add_parser("delete", help="Delete a Flyte execution and matching Spark resources")
    delete.add_argument("execution_id")

    sub.add_parser("reset", help="Delete leftover ELT Spark resources in the target namespace")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    require_bin("kubectl")
    require_bin("git")
    require_bin("python")

    if args.command == "register":
        lint_sources()
        import_check()
        ensure_namespace_bootstrap_ready()
        start_port_forward()
        init_flytectl()
        register_entities()
        return 0

    if args.command == "elt":
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