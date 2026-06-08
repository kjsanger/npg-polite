# npg-polite

## Po(rch) Lite

A minimal, task-centric API for interacting with a [Porch](https://github.com/wtsi-npg/npg_porch_cli) server.

# Summary

This library hides the details of sending requests to and receiving responses from a Porch
server, presenting a pipleline- and task-centric API instead.

## Use

### Configuration

This package includes a `Pipeline.ServerConfig` dataclass to collect external configuration for
a pipeline in one place where it can be passed to the Pipeline constructor.

The configurable values are:

- url: The base URL of the Porch server.
- pipeline_token: The pipeline token for the Porch server.
- admin_token: The admin token for the Porch server.

A configuration instance can be created directly and populated with tokens obtained from a secrets
manager. E.g.

    config = ServerConfig(
        porch_url="https://example.com/porch",
        admin_token=token1,
        pipeline_token=token2,
    )

Alternatively, configuration (and possibly tokens) can be read from a file. As this is a dataclass,
instances can be created from a named section of a configuration INI file using the
[npg-python-lib](https://github.com/wtsi-npg/npg-python-lib) `npg.conf.IniData` class.

An example INI file would look like this:

    [<section name>]
    url = http://localhost:8000
    pipeline_token = 11111111111111111111111111111111
    admin_token = 0000000000000000000000000000000

The corresponding code to populate the `ServerConfig` dataclass would look like this:

    server_config = IniData(Pipeline.ServerConfig).from_file(<file name>, <section name>)

The token fields are set to not be included in the repr() output to avoid leaking sensitive
information in logs.


### Creating Piplines and Tasks on a Porch server

A [Porch](https://github.com/wtsi-npg/npg_porch) pipeline is type of pub/sub queue where tasks
are added by one process and later claimed and processed by another.

In this API a `Pipeline` instance represents a kind of pipeline on the Porch server, with the
pipeline name, URI  and version as its identity. A `Task` represents an instance of the pipeline
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

To use this module for a new Pipeline, you need to create a subclass of `Pipeline.Task` and implement
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

        def from_serializable(cls, serializable: dict):
            return cls(**serializable)

When this is done, you can create a new Pipeline and add tasks to it:

    p = Pipeline(SumTask, "Sum of two integers", "http://localhost/sum", "1.0.0")
    p = p.register()  # Needs to be done once

    tasks = [
        SumTask(10, 42),
        SumTask(10, 99),
    ]

    for task in tasks:
        p.add(task)

If you are using Porch 3.0.0 or later, then registering a new pipeline requires both an admin
and pipeline token. As the latter doesn't yet exist, one is created on the fly, and you can opt
to capture this token into the active in-memory config instance.

    p = Pipeline(SumTask, "Sum of two integers", "http://localhost/sum", "1.0.0")
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

The Pipeline class uses the  [Porch](https://github.com/wtsi-npg/npg_porch) REST API. See its documentation
for more information. It has a timeout of 10 seconds and will retry failed requests up to 3 times
with an exponential backoff starting at 15 seconds.


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
will install automatically install the package and its dependencies in a virtual environment and activate
the environment before running the tests:

    docker compose build
    docker compose run app

    # Install the package
    pip install .
    
    # Run the tests
    pytest --it
