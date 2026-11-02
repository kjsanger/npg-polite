# npg-polite

## Po (rch) Lite

A minimal, task-centric API for interacting with a [Porch](https://github.com/wtsi-npg/npg_porch_cli) server.

# Summary

This library hides the details of sending requests to and receiving responses from a Porch
server, presenting a pipleline- and task-centric API instead.

## Use

### Configuration

This package includes a `Pipeline.Config` dataclass to collect external configuration for
a pipeline in one place where it can be passed to the Pipeline constructor.

The configurable values are:

- url: The base URL of the Porch server.
- pipeline_token: The pipeline token for the Porch server (used to work with an existing pipeline).
- admin_token: The admin token for the Porch server (used to create new pipelines).
- name: The pipeline name (the first part of its identity).
- uri: The pipeline URI (the second part of its identity).
- version: The pipeline version (the third part of its identity).
- task: The fully qualified Python class anme of the pipeline task.

A configuration instance can be created directly and populated with tokens obtained from a secrets
manager. E.g.

    config = Config(
        porch_url="https://example.com/porch",
        admin_token=token1,
        pipeline_token=token2,
        name="Example pipeline",
        uri="htps://example.com/example_pipeline/1",
        task="example.Task",
    )

Alternatively, configuration (and possibly tokens) can be read from a file. As this is a dataclass,
instances can be created from a named section of a configuration INI file using the
[npg-python-lib](https://github.com/wtsi-npg/npg-python-lib) `npg.conf.IniData` class.

An example INI file would look like this:

    [<section name>]
    url = http://localhost:8000
    pipeline_token = 11111111111111111111111111111111
    admin_token = 0000000000000000000000000000000

    [<section name>]
    name = Example pipeline
    uri = htps://example.com/example_pipeline/1
    task = example.Task

The corresponding code to populate the `Config` dataclass would look like this:

    config = IniData(Pipeline.Config).from_file(<file name>, <section name>*)

The token fields are set to not be included in the repr () output to avoid leaking sensitive
information in logs.

### Creating Pipelines and Tasks on a Porch server

A [Porch](https://github.com/wtsi-npg/npg_porch) pipeline is type of pub/sub queue where tasks
are added by one process and later claimed and processed by another.

In this API a `Pipeline` instance represents a kind of pipeline on the Porch server, with the
pipeline name, URI and version as its identity. A `Task` represents an instance of the pipeline
doing some work and consequently changing state.

When a new Pipeline is created, it must be registered with the Porch server before tasks can be
added to it. This is done using the `register` method. Once registered, a pipeline token must be
obtained using the `new_token` method. This token is used to add, claim and update tasks for this
Pipeline.

A Pipeline's `register` and `new_token` methods require an admin token. The other methods require
a pipeline token.

The identity of a Porch Task is defined by the serializable task input; two Task objects with the
same inputs are considered the same task. Porch uses this to try to ensure that each task is created
and processed once.

To use this module for a new Pipeline, you need to create a subclass of `Task` and implement
the `to_serializable` and `from_serializable` methods. See the
[Porch](https://github.com/wtsi-npg/npg_porch) documentation for more information on how the task
attributes and values are serialized as JSON.

For example:

    from npg_polite.porch import Pipeline, Task

    class SumTask(Task):
        """A task to add two integers."""
        
        input1: int
        input2: int

        def __init__(self, input1: int = 0, input2: int = 0):
            super().__init__(Task.Status.PENDING)
            self.input1 = input1
            self.input2 = input2

        def to_serializable(self) -> dict:
            return {
                "input1": self.input1,
                "input2": self.input2,
            }

        @classmethod
        def from_serializable(cls, serializable: dict):
            return cls(**serializable)

When this is done, you can create a new Pipeline and add tasks to it:

    config = Pipeline.Config(
        name="Sum of two integers",
        uri="http://localhost/sum",
        version="1.0.0",
        task=f"{SumTask.__module__}.{SumTask.__qualname__}",
    )
    p = Pipeline[SumTask](config=config, server_config=server_config)
    p = p.register(update_config=True)  # Needs to be done once

    tasks = [
        SumTask(10, 42),
        SumTask(10, 99),
    ]

    for task in tasks:
        p.add(task)

The task class is resolved from the `task`, attribute which must name a class in an importable
module.

If you are using Porch 3.0.0 or later, then registering a new pipeline requires both an admin
and pipeline token. As the latter doesn't yet exist, one is created on the fly, and you can opt
to capture this token into the active in-memory config instance.

    p = Pipeline[SumTask](config=config, server_config=server_config)
    p = p.register(update_config=True)

However, if you want to persist the token beyond the session, it is then up to you to store the
config appropriately.

Once tasks are added, you can claim and update their status as they are processed:

    claimed_task = p.claim()
    try:
        # Submit the task to a worker
        p.run(task)  # Tell the Porch server that the task is running
    except Exception as e:
        p.fail(task)  # Tell the Porch server that the task failed

Porch will ensure that each task is created exactly once and that each task is claimed for processing
once, by only one worker.

A pipeline's identity in Porch is defined by a tuple of name, URI and version. Pipelines sharing the
same values for these properties are considered to be the same logical entity. When a pipeline's
identity changes because its version has been bumped, the default Porch behaviour is that any task
belonging to that pipeline that was completed previously now becomes eligible for re-running.

This might be the desired scenario in some places e.g. the version update fixes a bug that rendered
results invalid and therefore the entire pipline should be re-run on all of its input data. However, it
might not be desired on others e.g. the version update changes some run time parameters that make the
pipeline more efficient without affecting the results.

In order to support different modes of pipeline deployment, this library adds the concept of a
compatibility policy whereby a pipeline reports only some part of its version, or a transformed version,
to the server. For example, in order to support deployment of patch versions of the pipeline (that do not
cause changes in pipeline output), the pipeline can declare a MAJOR_MINOR policy. Only those parts of
the version will be passed to the server, while patch versions can still be trackedin the pipeline code.

The Pipeline class uses the  [Porch](https://github.com/wtsi-npg/npg_porch) REST API. See its documentation
for more information. It has a timeout of 10 seconds and will retry failed requests up to 3 times
with an exponential backoff starting at 15 seconds.

### Included Tasks

### Reusable shell-command tasks

`ShellCommandTask` runs trusted Bash commands that are defined as a command template
plus named string-valued parameters:

```python
from npg_polite.task import ShellCommandTask

task = ShellCommandTask(
        command="process-data --from {:start:} --to {:end:}",
        params={
                "start": "2026-09-24T00:00:00Z",
                "end": "2026-09-25T00:00:00Z",
        },
)
```

Parameters are interpolated into placeholders of the form `{:name:}` where the literal
`name` corresponds to its name. No whitespace is permitted between the `{:` and `:}`
delimiter. Parameter names must match the regular expression `[A-Za-z_][A-Za-z0-9_]*`
and values must be strings. Each value is quoted and then inserted into the template,
replacing the placeholder.

See the ShellCommandTask class docstring for more details.

## Development

If you are making changes to the code, you can run tests against a local Porch server using Docker. The
included `docker-compose.yml` file will start a local Porch server, defaulting to port 8081. You can
either start the server and run tests against it from the host machine:

    docker-compose up -d

    # Create a virtual environment and install dependencies
    python -m venv ./venv
    . ./venv/bin/activate
    pip install -r requirements.txt
    pip install -r test-requirements.txt

    # Install the package
    pip install .
    
    # Run the tests (the `--it` argument is optional and will run the tests using pytest-it explanations)
    pytest --it

Alternatively, you can run the tests in a Docker container built using the included `Dockerfile.dev` which
will automatically install the package and its dependencies in a virtual environment and activate
the environment before running the tests:

    docker compose build
    docker compose run app

    # Install the package
    pip install .
    
    # Run the tests
    pytest --it
