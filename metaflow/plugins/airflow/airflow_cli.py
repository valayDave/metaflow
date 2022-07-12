import os
import re
import sys

from metaflow import current, decorators
from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.package import MetaflowPackage

from metaflow.plugins import KubernetesDecorator
from metaflow.util import get_username

from .airflow import Airflow
from .exception import AirflowException, NotSupportedException

VALID_NAME = re.compile("[^a-zA-Z0-9_\-\.]")

# TODO (Final-comments) : Create CLI checks for resolving production token.


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Airflow.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Airflow DAG name. The flow name is used instead if this option is not "
    "specified",
)
@click.pass_obj
def airflow(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.dag_name = resolve_dag_name(obj, name)


@airflow.command(help="Compile a new version of this flow to Airflow DAG.")
@click.argument("file", required=True)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by Airflow DAG executions "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--is-paused-upon-creation",
    default=False,
    is_flag=True,
    help="Generated Airflow DAG is paused/unpaused upon creation.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    # TODO (savin): Identify the default namespace?
    help="Change the namespace from the default to the given tag. "
    "See run --help for more information.",
)
@click.option(
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of parallel processes.",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Workflow timeout in seconds. Enforced only for scheduled DAGs.",
)
@click.option(
    "--worker-pool",
    default=None,
    show_default=True,
    help="Worker pool for Airflow DAG execution.",
)
@click.pass_obj
def create(
    obj,
    file,
    tags=None,
    is_paused_upon_creation=False,
    user_namespace=None,
    max_workers=None,
    workflow_timeout=None,
    worker_pool=None,
):
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException(
            "Airflow DAG file name cannot be the same as flow file name"
        )

    obj.echo("Compiling *%s* to Airflow DAG..." % obj.dag_name, bold=True)

    flow = make_flow(
        obj,
        obj.dag_name,
        tags,
        is_paused_upon_creation,
        user_namespace,
        max_workers,
        workflow_timeout,
        worker_pool,
        file,
    )
    with open(file, "w") as f:
        f.write(flow.compile())

    obj.echo(
        "DAG *{dag_name}* "
        "for flow *{name}* compiled to "
        "Airflow successfully.\n".format(dag_name=obj.dag_name, name=current.flow_name),
        bold=True,
    )


def make_flow(
    obj,
    dag_name,
    tags,
    is_paused_upon_creation,
    namespace,
    max_workers,
    workflow_timeout,
    worker_pool,
    file,
):
    # Validate if the workflow is correctly parsed.
    _validate_workflow(
        obj.flow, obj.graph, obj.flow_datastore, obj.metadata, workflow_timeout
    )

    # Attach @kubernetes.
    decorators._attach_decorators(obj.flow, [KubernetesDecorator.name])

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )

    # Save the code package in the flow datastore so that both user code and
    # metaflow package can be retrieved during workflow execution.
    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.echo, obj.package_suffixes
    )
    package_url, package_sha = obj.flow_datastore.save_data(
        [obj.package.blob], len_hint=1
    )[0]

    return Airflow(
        dag_name,
        obj.graph,
        obj.flow,
        package_sha,
        package_url,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        tags=tags,
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        worker_pool=worker_pool,
        workflow_timeout=workflow_timeout,
        description=obj.flow.__doc__,
        file_path=file,
        is_paused_upon_creation=is_paused_upon_creation,
    )


def _validate_workflow(flow, graph, flow_datastore, metadata, workflow_timeout):
    no_scheduling = not (
        flow._flow_decorators.get("airflow_schedule_interval")
        or flow._flow_decorators.get("schedule")
    )

    if no_scheduling and workflow_timeout is not None:
        raise AirflowException(
            "Cannot set `--workflow-timeout` for an unscheduled DAG. Add `@schedule` or `@airflow_schedule_interval` to the flow to set `--workflow-timeout`."
        )
    # check for other compute related decorators.
    # supported compute : k8s (v1), local(v2), batch(v3),
    for node in graph:
        # TODO (Final-Comments) : Check if there is an nesting within the foreach and throw and exception if there is a nested foreach within the code.
        if any([d.name == "batch" for d in node.decorators]):
            raise NotSupportedException(
                "Step *%s* is marked for execution on AWS Batch with Airflow which isn't currently supported."
                % node.name
            )

    if flow_datastore.TYPE != "s3":
        raise AirflowException('Datastore of type "s3" required with `airflow create`')


def resolve_dag_name(obj, name):
    project = current.get("project_name")
    if project:
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        dag_name = current.project_flow_name
    else:
        if name and VALID_NAME.search(name):
            raise MetaflowException("Name '%s' contains invalid characters." % name)
        dag_name = name if name else current.flow_name
    return dag_name
