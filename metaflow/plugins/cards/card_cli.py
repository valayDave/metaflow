from metaflow.client import Task
from metaflow import Flow, JSONType,Step
import webbrowser
import click
import os
import sys
import random
from functools import wraps
from metaflow.exception import MetaflowNotFound
from .card_datastore import CardDatastore,stepname_from_card_id,NUM_SHORT_HASH_CHARS
from .exception import CardClassFoundException,\
                        IncorrectCardArgsException,\
                        UnrenderableCardException,\
                        CardNotPresentException,\
                        IdNotFoundException,\
                        TypeRequiredException

def open_in_browser(card_path):
    url = 'file://' + os.path.abspath(card_path)
    webbrowser.open(url)

def resolve_card(ctx,identifier,id=None,hash=None,type=None,index=0):
    """Resolves the card path based on the arguments provided. We allow identifier to be a pathspec or a id of card. 

    Args:
        ctx : click context object
        identifier : id of card or pathspec
        id ([type], optional): if identifier is pathspec then this will be used. 
        hash ([type], optional): This is to specifically resolve the card via the hash. This is useful when there may be many card with same id or hash. 
        type : type of card 
        index : index of card decorator (Will be useful once we support many @card decorators.)

    Raises:
        IdNotFoundException: the identifier is an ID and metaflow cannot resolve this ID in the datastore. 
        CardNotPresentException: No card could be found for the identifier (may it be pathspec or id)

    Returns:
        (card_paths,card_datastore,taskpathspec) : Tuple[List[str],CardDatastore,str]
    """
    is_path_spec = False
    if "/" in identifier:
        is_path_spec= True
        assert len(identifier.split('/'))  == 3, "Expecting pathspec of form <runid>/<stepname>/<taskid>"
    
    flow_name = ctx.obj.flow.name
    pathspec,run_id,step_name,task_id = None,None,None,None
    # this means that identifier is a pathspec 
    if is_path_spec:
        # what should be the args we expose 
        run_id,step_name,task_id = identifier.split('/')
        pathspec = '/'.join([flow_name,run_id,step_name,task_id])
    else: # If identifier is not a pathspec then it may be referencing the latest run
        # this means the identifier is card-id
        # So we first resolve the id to stepname.
        step_name = stepname_from_card_id(
            identifier,ctx.obj.flow
        )
        # this means that the `id` doesn't match 
        # the one specified in the decorator
        if step_name is None:
            raise IdNotFoundException(
                identifier
            )
        # todo : Should this only resolve cards of the latest runs. 
        run_id = Flow(flow_name).latest_run.id
        step = Step('/'.join([flow_name,run_id,step_name]))
        tasks = list(step)
        # todo : how to resolve cards in a foreach with card-id
        if len(tasks) == 0:
            raise Exception(
                "No Tasks found for Step '%s' and Run '%s'" % (step_name,run_id)
            )
        # todo : What to do when we have multiple tasks to resolve? 
        task = tasks[0]
        
        pathspec = task.pathspec
        task_id = task.id
    
    ctx.obj.echo_always("Resolving card : %s" % pathspec,fg='green')
    # to resolve card_id we first check if the identifier is a pathspec and if it is then we check if the `id` is set or not to resolve card_id
    card_id = None
    if is_path_spec:
        if id is not None:
            card_id = id
    else:
        card_id = identifier
    
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                run_id,\
                                step_name,\
                                task_id,\
                                path_spec=pathspec)
    card_paths_found = card_datastore.extract_card_paths(card_type=type,card_id=card_id,card_index=index,card_hash=hash)
    if len(card_paths_found) == 0 :
            # If there are no files found on the Path then raise an error of 
            raise CardNotPresentException(flow_name,\
                                        run_id,\
                                        step_name,\
                                        card_hash=hash,\
                                        card_index=index,\
                                        card_id=card_id,
                                        card_type=type)
    
    return card_paths_found,card_datastore,pathspec



def list_availble_cards(ctx,path_spec,card_paths,card_datastore,command='view'):
    # todo : create nice response messages on the CLI for cards which were found.
    scriptname = ctx.obj.flow.script_name
    path_tuples = card_datastore.get_card_names(card_paths)
    ctx.obj.echo_always(
        "\nFound %s card matching for your query..."%str(len(path_tuples)),fg='green'
    )
    task_pathspec = '/'.join(path_spec.split('/')[1:])
    
    card_list = []
    for path_tuple in path_tuples:
        card_id,card_type,card_index,card_hash = path_tuple
        card_name = '%s-%s-%s'%(card_type,card_index,card_hash)
        if card_id is not None:
            card_name = '%s-%s-%s-%s'%(card_id,card_type,card_index,card_hash)
        card_list.append(card_name)

    random_idx = 0 if len(path_tuples) ==1 else random.randint(0,len(path_tuples)-1)
    randid, _, _ ,randhash  = path_tuples[random_idx]
    ctx.obj.echo_always(
        '\n\t'.join(['']+card_list),fg='blue'
    )
    ctx.obj.echo_always(
        "\n\tExample access from CLI via: \n\t %s\n" % make_command(
            scriptname,task_pathspec,command=command,hash=randhash[:NUM_SHORT_HASH_CHARS],id=randid
        ),
        fg='yellow'
    )
    
    


def make_command(script_name,taskspec,command='get',hash=None,id=None):
    calling_args = ["--hash",hash]
    if id is not None:
        calling_args = ['--id',id]
    return ' '.join([
        ">>>",
        "python",
        script_name,
        "card",
        command,
        taskspec,
    ]+calling_args)


@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
def card():
    pass



def card_read_options_and_arguments(func):
    @click.option('--id', 
                    default=None,
                    show_default=True,
                    type=str,
                    help="Id of the card")
    @click.option('--hash', 
                    default=None,
                    show_default=True,
                    type=str,
                    help="Hash of the stored HTML")
    @click.option('--type', 
                    default=None,
                    show_default=True,
                    type=str,
                    help="Type of card being created")
    @click.option('--index', 
                    default=0,
                    show_default=True,
                    type=int,
                    help="Index of the card decorator")
    @wraps(func)
    def wrapper(*args,**kwargs):
        return func(*args,**kwargs)
    
    return wrapper


# Finished According to the Memo
@card.command(help='create the HTML card')
@click.argument('pathspec',type=str)
@click.option('--type', 
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--options', 
                default={},
                show_default=True,
                type=JSONType,
                help="arguments of the card being created.")
@click.option('--id', 
                default=None,
                show_default=True,
                type=str,
                help="Unique ID of the card")
@click.option('--index', 
                default=0,
                show_default=True,
                type=int,
                help="Index of the card decorator")
@click.pass_context
def create(ctx,pathspec,type=None,id=None,index=None,options=None):
    assert len(pathspec.split('/'))  == 3, "Expecting pathspec of form <runid>/<stepname>/<taskid>"
    runid,step_name,task_id = pathspec.split('/')
    flowname = ctx.obj.flow.name
    full_pathspec = '/'.join([flowname,runid,step_name,task_id])
    task = Task(full_pathspec)
    from metaflow.plugins import CARDS

    filtered_cards = [CardClass for CardClass in CARDS if CardClass.type == type]
    if len(filtered_cards) == 0:
        raise CardClassFoundException(type)
    
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                runid,\
                                step_name,\
                                task_id,\
                                path_spec=full_pathspec)
    
    filtered_card = filtered_cards[0]
    ctx.obj.echo("Creating new card of type %s" % filtered_card.type, fg='green')
    # save card to datastore
    try:
        mf_card = filtered_card(**options)
    except TypeError as e:
        raise IncorrectCardArgsException(type,options)
    
    try:
        rendered_info = mf_card.render(task)
    except: # TODO : Catch exec trace over here. 
        raise UnrenderableCardException(type,options)
    else:
        card_datastore.save_card(type,id,index,rendered_info)

@card.command()
@click.argument('identifier')
@card_read_options_and_arguments
@click.pass_context
def view(ctx,identifier,id=None,hash=None,type=None,index=None):
    """
    View the HTML card in browser based on the IDENTIFIER.\n
    The IDENTIFIER can be:\n 
        - run path spec : <runid>/<stepname>/<taskid>\n
                    OR\n
        - id given in the @card\n
    """
    available_card_paths,card_datastore,pathspec = resolve_card(ctx,identifier,type=type,index=index,id=id,hash=hash)
    if len(available_card_paths) == 1:
        open_in_browser(
            card_datastore.cache_locally(available_card_paths[0])
        )
    else:
        list_availble_cards(ctx,pathspec,available_card_paths,card_datastore,command='view')

@card.command()
@click.argument('identifier')
@card_read_options_and_arguments
@click.pass_context
def get(ctx,identifier,id=None,hash=None,type=None,index=None):
    available_card_paths,card_datastore,pathspec = resolve_card(ctx,identifier,type=type,index=index,id=id,hash=hash)
    
    
    if len(available_card_paths) == 1:
        with open(card_datastore.cache_locally(available_card_paths[0]),'r') as f:
            print(f.read())
    else:
        list_availble_cards(ctx,pathspec,available_card_paths,card_datastore,command='get')