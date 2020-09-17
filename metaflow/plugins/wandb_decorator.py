from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import WANDB_KEY
from metaflow.exception import MetaflowException

class WeightsAndBiasClientMissing(MetaflowException):
    headline = 'Weights and Bias API Key is missing.'

    def __init__(self,):
        msg = '@wandb decorator requires the WANDB_API_KEY as environment variable'
        super(WeightsAndBiasClientMissing, self).__init__(msg)

class WeightsAndBiasModuleMissing(MetaflowException):
    headline = 'Weights and Bias Module is missing.'

    def __init__(self,):
        msg = '@wandb decorator requires the wandb module to run'
        super(WeightsAndBiasModuleMissing, self).__init__(msg)


class WeightsNBiasDecorator(StepDecorator):
    """
    Step decorator to specify a weights 
    and bias logger for a step. 

    
    To use, annotate your step as follows:
    ```
    @wandb()
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    
    """
    name = 'wandb'
    defaults = {}
    def step_init(self, flow, graph, step_name, decorators, environment, datastore, logger):
        """
        Called when all decorators have been created for this step
        """
        pass

    def step_task_retry_count(self):
        """
        Called to determine the number of times this task should be retried.
        Returns a tuple of (user_code_retries, error_retries). Error retries
        are attempts to run the process after the user code has failed all
        its retries.
        """
        return 0, 0

    def runtime_init(self, flow, graph, package, run_id):
        """
        Top-level initialization before anything gets run in the runtime
        context.
        """
        if WANDB_KEY is None:
            raise WeightsAndBiasClientMissing()
        self._get_wandb()
        
    @staticmethod
    def _get_wandb():
        try: 
            import wandb
            return wandb
        except ModuleNotFoundError as e:
            raise WeightsAndBiasModuleMissing()



    def runtime_finished(self, exception):
        """
        Called when the runtime created task finishes or encounters an interrupt/exception.
        """
        pass

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries):
        """
        Access the command line for a step execution in the runtime context.
        """
        pass


    def task_pre_step(self,
                        step_name,
                        datastore,
                        metadata,
                        run_id,
                        task_id,
                        flow,
                        graph,
                        retry_count,
                        max_user_code_retries):
        """
        Run before the step function in the task context.
        """
        flow.wandb = self._get_wandb()
        api_key = WANDB_KEY
        flow.wandb.login(key=api_key)
        flow.wandb.init(
            name = f'{flow.name}/{run_id}/{step_name}/{task_id}',
            project=f'{flow.name}',
        )
        

    def task_decorate(self,
                        step_func,
                        flow,
                        graph,
                        retry_count,
                        max_user_code_retries):
        return step_func

    def task_post_step(self,
                        step_name,
                        flow,
                        graph,
                        retry_count,
                        max_user_code_retries):
        """
        Run after the step function has finished successfully in the task
        context.
        """
        del flow.wandb

    def task_exception(self,
                        exception,
                        step_name,
                        flow,
                        graph,
                        retry_count,
                        max_user_code_retries):
        """
        Run if the step function raised an exception in the task context.

        If this method returns True, it is assumed that the exception has
        been taken care of and the flow may continue.
        """
        pass

    def task_finished(self,
                        step_name,
                        flow,
                        graph,
                        is_task_ok,
                        retry_count,
                        max_user_code_retries):
        """
        Run after the task context has been finalized.

        is_task_ok is set to False if the user code raised an exception that
        was not handled by any decorator.

        Note that you can't create or modify data artifacts in this method
        since the task has been finalized by the time this method
        is called. Also note that the task may fail after this method has been
        called, so this method may get called multiple times for a task over
        multiple attempts, similar to all task_ methods.
        """
        pass