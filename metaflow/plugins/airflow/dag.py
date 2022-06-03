# Deployed on {{deployed_on}}

# instead of metaflow_workflow_compile_params, just config?
CONFIG = {{{metaflow_workflow_compile_params}}}

# just utils?
{{{AIRFLOW_UTILS}}}

dag = Workflow.from_dict(CONFIG).compile()
with dag:
    pass
