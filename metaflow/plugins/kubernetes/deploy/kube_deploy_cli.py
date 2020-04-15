import os
import sys
import tarfile
import time
import traceback

import click

from metaflow import namespace
from metaflow.util import resolve_identity, decompress_list, write_latest_run_id, get_latest_run_id
from metaflow.plugins.kubernetes.kube import Kube,KubeKilledException, KubeException
from .runtime import KubeDeployRuntime
from metaflow import parameters
from metaflow.package import MetaflowPackage
from metaflow import decorators
from metaflow.graph import FlowGraph
from functools import wraps
from metaflow import util
from metaflow.exception import (
    CommandException,
    METAFLOW_EXIT_DISALLOW_RETRY,
)

@click.group()
def cli():
    pass


def before_deploy(obj, decospecs):
    # This is replica of the cli.before_run command in cli.py
    # Ensures adding Decorators and running a check so graph is properly validated
    if decospecs:
        decorators._attach_decorators(obj.flow, decospecs)
        obj.graph = FlowGraph(obj.flow.__class__)
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    # obj.environment.init_environment(obj.logger)

    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = obj.datastore.get_datastore_root_from_config(
            obj.echo)

    # We are initialising decorators because the flow_init method called within _init_decortors will ensure checks to 
    # things like conda environment etc. before initiating Deployment. This is also done so that right docker images are chosen
    # for deployment.  
    decorators._init_decorators(
        obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)
    
    # We don't persist tags here because they are passed as cli args to the runtime_container.

    # Package the code for deployment as a runtime container.
    # TODO(crk): Capture time taken to package and log to keystone.
    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes)


def common_run_options(func):
    @click.option('--tag',
                  'tags',
                  multiple=True,
                  default=None,
                  help="Annotate this run with the given tag. You can specify "
                       "this option multiple times to attach multiple tags in "
                       "the run.")
    @click.option('--max-workers',
                  default=16,
                  show_default=True,
                  help='Maximum number of parallel processes.')
    @click.option('--max-num-splits',
                  default=100,
                  show_default=True,
                  help='Maximum number of splits allowed in a foreach. This '
                       'is a safety check preventing bugs from triggering '
                       'thousands of steps inadvertently.')
    @click.option('--max-log-size',
                  default=10,
                  show_default=True,
                  help='Maximum size of stdout and stderr captured in '
                       'megabytes. If a step outputs more than this to '
                       'stdout/stderr, its output will be truncated.')
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@cli.group(help='Command related to running entire Metaflow Native runtime in a container on Kubernetes')
def kube_deploy():
    pass

# $ Note : Order of Decorators Matter A LOT!
@parameters.add_custom_parameters
@kube_deploy.command(help='Run the workflow\'s runtime In a container ' 
                          'on Kubernetes which will orchestrate the steps '
                          'as jobs on Kubernetes cluster ')
@common_run_options
@click.option('--dont-exit', 'dont_exit', is_flag=True, help='This will keep running the Deploy for log tailing even after it is done')
@click.option('--kube-namespace', 'kube_namespace', default=None, help='This will use Kube namespace to deploy the runtime and the susequent step based containers for the workflow')
@click.option('--max-runtime-cpu', 'max_runtime_cpu', default=3, help='This is the number of CPUs to allocated to the job that will run the native runtime')
@click.option('--max-runtime-memory', 'max_runtime_memory', default=2000, help='This is the amount of Memory to allocated to the job that will run the native runtime')
@click.pass_context
def run(
        ctx1,
        tags=None,
        max_workers=None,
        max_num_splits=None,
        max_log_size=None,
        run_id_file=None,
        user_namespace=None,
        dont_exit=None,
        kube_namespace=None,
        max_runtime_cpu=None,
        max_runtime_memory=None,
        **kwargs):

    ctx = ctx1.obj

    def echo(batch_id, msg, stream=sys.stdout):
        ctx.logger(head='[%s] '%batch_id,body=msg)
        # ctx.echo_always("[%s] %s" % (batch_id, msg))
    # print(kwargs)
    if namespace is not None:
        namespace(user_namespace or None)
    
    before_deploy(ctx, ctx.environment.decospecs())

    supported_datastore = ['s3']

    if ctx.metadata.TYPE != 'service':
        raise KubeException(
            'Need Service Based Metadata Provider to Make run happen on Kubernetes')

    if ctx.datastore.TYPE not in supported_datastore:
        raise KubeException('Kubernetes Deployment supports {} as Datastores and not {}'.format(
            ' '.join(supported_datastore), ctx.datastore.TYPE))

    if ctx.datastore.datastore_root is None:
        ctx.datastore.datastore_root = ctx.datastore.get_datastore_root_from_config(
            echo)

    executable = 'python' # $ Keeping this No Matter Conda Or Someone Else specifying Env. 
    ctx.entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))
    # Set kube deployment attributes
    attrs = {
        "metaflow.user": util.get_username(),
        "metaflow.flow_name": ctx.flow.name,
        "metaflow.version": ctx.environment.get_environment_info()[
            "metaflow_version"
        ]
    }

    top_args = " ".join(util.dict_to_cli_options(ctx1.parent.parent.params))

    step_args = " ".join(util.dict_to_cli_options(kwargs))

    # $ Send partial because the KubeDeployRuntime will find the parameters on its own and
    # $ using deploy_runtime_util.get_real_param_values
    partial_runtime_cli = u"{entrypoint} {top_args} run".format(
        entrypoint=ctx.entrypoint, top_args=top_args
    )

    """ 
    If the value of an input `Parameter` is 'num_training_examples' or 'num-training-examples' the parsed output from 
    click is num_training_examples. Hence for each step we provide a METAFLOW_INPUT_PATH for retrieving params via metadata provider. 
    # Problem Over here. This is converting '_' into '-' in param conversion via `util.dict_to_cli_options` hence will use My own Method for param extraction.
    # The runtime will Use the `persist_parameters()` to pass the params to the docker image sent. 
    """
    env = {}  # todo : check for env Handling.
    # $ Partial cli is sent because the KubeDeployRuntime will extract any data params in the artifacts so that they can be modeled in the same light on the Deployed image
    deploy_runtime = KubeDeployRuntime(ctx.flow,
                                       ctx.graph,
                                       ctx.datastore,
                                       ctx.metadata,
                                       ctx.environment,
                                       ctx.package,
                                       ctx.logger,
                                       ctx.entrypoint,
                                       ctx.event_logger,
                                       ctx.monitor,
                                       max_workers=max_workers,
                                       max_num_splits=max_num_splits,
                                       max_log_size=max_log_size,
                                       kube_namespace=kube_namespace,
                                       partial_runtime_cli=partial_runtime_cli,
                                       max_runtime_cpu=max_runtime_cpu,
                                       max_runtime_memory=max_runtime_memory,
                                       tags=tags,
                                       **kwargs)

    deploy_runtime.persist_runtime(util.get_username())
    try:
        with ctx.monitor.measure("metaflow.kube-deploy.launch"):
            deploy_runtime.deploy(attrs=attrs, env=env)
            if dont_exit:
                deploy_runtime.wait_to_complete(echo=echo)
            else:
                echo(deploy_runtime.job.name,"Metaflow Runtime Deployed On Kubernetes. Exiting because of --dont_exit not set.")
    except KubeKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)


@click.option('--origin-run-id',
              default=None,
              help="ID of the run that should be resumed. By default, the "
                   "last run executed locally.")
@click.argument('step-to-rerun',
                required=False)
@kube_deploy.command(help='Resume execution of a previous run of this flow.')
@common_run_options
@click.option('--dont-exit', 'dont_exit', is_flag=True, help='This will keep running the Deploy for log tailing even after it is done')
@click.option('--kube-namespace', 'kube_namespace', default=None, help='This will use Kube namespace to deploy the runtime and the susequent step based containers for the workflow')
@click.option('--max-runtime-cpu', 'max_runtime_cpu', default=3, help='This is the number of CPUs to allocated to the job that will run the native runtime')
@click.option('--max-runtime-memory', 'max_runtime_memory', default=2000, help='This is the amount of Memory to allocated to the job that will run the native runtime')
@click.pass_context
def resume(ctx1,
           tags=None,
           step_to_rerun=None,
           origin_run_id=None,
           max_workers=None,
           max_num_splits=None,
           max_log_size=None,
           decospecs=None,
           run_id_file=None,
           dont_exit=None,
           kube_namespace=None,
           max_runtime_cpu=None,
           max_runtime_memory=None):
    
    ctx = ctx1.obj

    def echo(batch_id, msg, stream=sys.stdout):
        ctx.logger(head='[%s] '%batch_id,body=msg)


    before_deploy(ctx, ctx.environment.decospecs())

    if origin_run_id is None:
        origin_run_id = get_latest_run_id(ctx.echo, ctx.flow.name)
        if origin_run_id is None:
            raise CommandException("A previous run id was not found. Specify --origin-run-id.")
    
    supported_datastore = ['s3']

    if ctx.metadata.TYPE != 'service':
        raise KubeException(
            'Need Service Based Metadata Provider to Make run happen on Kubernetes')

    if ctx.datastore.TYPE not in supported_datastore:
        raise KubeException('Kubernetes Deployment supports {} as Datastores and not {}'.format(
            ' '.join(supported_datastore), ctx.datastore.TYPE))

    if ctx.datastore.datastore_root is None:
        ctx.datastore.datastore_root = ctx.datastore.get_datastore_root_from_config(
            echo)

    executable = 'python' # $ Keeping this No Matter Conda Or Someone Else specifying Env. 
    ctx.entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))
    # Set kube deployment attributes
    attrs = {
        "metaflow.user": util.get_username(),
        "metaflow.flow_name": ctx.flow.name,
        "metaflow.version": ctx.environment.get_environment_info()[
            "metaflow_version"
        ]
    }

    top_args = " ".join(util.dict_to_cli_options(ctx1.parent.parent.params))


    # $ Send partial because the KubeDeployRuntime will find the parameters on its own and
    # $ using deploy_runtime_util.get_real_param_values
    partial_runtime_cli = u"{entrypoint} {top_args} resume".format(
        entrypoint=ctx.entrypoint, top_args=top_args
    )

    deploy_runtime = KubeDeployRuntime(ctx.flow,
                            ctx.graph,
                            ctx.datastore,
                            ctx.metadata,
                            ctx.environment,
                            ctx.package,
                            ctx.logger,
                            ctx.entrypoint,
                            ctx.event_logger,
                            ctx.monitor,
                            clone_run_id=origin_run_id,
                            max_workers=max_workers,
                            max_num_splits=max_num_splits,
                            max_log_size=max_log_size,
                            kube_namespace=kube_namespace,
                            partial_runtime_cli=partial_runtime_cli,
                            max_runtime_cpu=max_runtime_cpu,
                            max_runtime_memory=max_runtime_memory,
                            tags=tags,)
    
    deploy_runtime.persist_runtime(util.get_username(),resume_bool=True)
    env = {}  # todo : check for env Handling.
    try:
        with ctx.monitor.measure("metaflow.kube-deploy.launch"):
            deploy_runtime.deploy(attrs=attrs, env=env)
            if dont_exit:
                deploy_runtime.wait_to_complete(echo=echo)
            else:
                echo(deploy_runtime.job.name,"Metaflow Runtime Deployed On Kubernetes. Exiting because of --dont_exit not set.")
    except KubeKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)

    
@kube_deploy.command(help="List the currently running step orchestrators")
@click.option('--user','user',help='List filtered by according to user',default=None)
@click.option('--kube-namespace','kube_namespace',help='Namespace to search while finding this Job')
@click.pass_context
def list(ctx1,user=None,flow=None,kube_namespace=None):
    ctx = ctx1.obj
    deploy_rt = KubeDeployRuntime( ctx.flow,
                                ctx.graph,                                      
                                ctx.datastore,
                                ctx.metadata,
                                ctx.environment,
                                None,
                                ctx.logger,
                                ctx.entrypoint,
                                ctx.event_logger,
                                ctx.monitor,
                                kube_namespace=kube_namespace)
    if user:
        deploy_rt.list_jobs(username=user,echo=ctx.echo)
    else:
        deploy_rt.list_jobs(echo=ctx.echo)
        
    