# Pipelines for taskiq

Taskiq pipelines is a `fire-and-forget` at its limit.

Imagine you have a really tough functions and you want
to call them sequentially one after one, but you don't want to wait for them
to complete. taskiq-pipeline solves this for you.

## Installation


You can install it from pypi:
```
pip install taskiq-pipelines
```

After you installed it you need to add our super clever middleware
to your broker.

This middleware actually decides what to do next, after current step
is completed.

```python
from taskiq_pipelines.middleware import PipelineMiddleware

my_super_broker = ...


my_super_broker.add_middlewares(
    [
        PipelineMiddleware(),
    ]
)
```

Also we have to admit that your broker MUST use result_backend that
can be read by all your workers. Pipelines work with inmemorybroker,
feel free to use it in local development.


### Example

For this example I'm going to use one single script file.

```python
import asyncio
from typing import Any, List
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq_pipelines import PipelineMiddleware, Pipeline

broker = InMemoryBroker()
broker.add_middlewares([PipelineMiddleware()])


@broker.task
def add_one(value: int) -> int:
    return value + 1


@broker.task
def repeat(value: Any, reps: int) -> List[Any]:
    return [value] * reps


@broker.task
def check(value: int) -> bool:
    return value >= 0


async def main():
    pipe = (
        Pipeline(
            broker,
            add_one,  # First of all we call add_one function.
        )
        # 2
        .call_next(repeat, reps=4)  #  Here we repeat our value 4 times
        # [2, 2, 2, 2]
        .map(add_one)  # Here we execute given function for each value.
        # [3, 3, 3, 3]
        .filter(check)  # Here we filter some values.
        # But sice our filter filters out all numbers less than zero,
        # our value won't change.
        # [3, 3, 3, 3]
    )
    task = await pipe.kiq(1)
    result = await task.wait_result()
    print("Calculated value:", result.return_value)


if __name__ == "__main__":
    asyncio.run(main())

```

If you run this example, it prints this:
```bash
$ python script.py
Calculated value: [3, 3, 3, 3]
```

Let's talk about this example.
Two notable things here:
1. We must add PipelineMiddleware in the list of our middlewares.
2. We can use only tasks as functions we wan to execute in pipeline.
    If you want to execute ordinary python function - you must wrap it in task.

Pipeline itself is just a convinient wrapper over list of steps.
Constructed pipeline has the same semantics as the ordinary task, and you can add steps
manually. But all steps of the pipeline must implement `taskiq_pipelines.abc.AbstractStep` class.

Pipelines can be serialized to strings with `dumps` method, and you can load them back with `Pipeline.loads` method. So you can share pipelines you want to execute as simple strings.

Pipeline assign `task_id` for each task when you call `kiq`, and executes every step with pre-calculated `task_id`,
so you know all task ids after you call kiq method.


## How does it work?

After you call `kiq` method of the pipeline it pre-calculates
all task_ids, serializes itself and adds serialized string to
the labels of the first task in the chain.

All the magic happens in the middleware.
After task is executed and result is saved, you can easily deserialize pipeline
back and calculate pipeline's next move. And that's the trick.
You can get more information from the source code of each pipeline step.

# Available steps

We have a few steps available for chaining calls:
1. Sequential
2. Mapper
3. Filter

### Sequential steps

This type of step is just an ordinary call of the function.
If you haven't specified `param_name` argument, then the result
of the previous step will be passed as the first argument of the function.
If you did specify the `param_name` argument, then the result of the previous
step can be found in key word arguments with the param name you specified.

You can add sequential steps with `.call_next` method of the pipeline.

If you don't want to pass the result of the previous step to the next one,
you can use `.call_after` method of the pipeline.

### Mapper step

This step runs specified task for each item of the previous task's result spawning
multiple tasks.
But I have to admit, that the result of the previous task must be iterable.
Otherwise it will mark the pipeline as failed.

After the execution you'll have mapped list.
You can add mappers by calling `.map` method of the pipeline.

### Filter step

This step runs specified task for each item of the previous task's result.
But I have to admit, that the result of the previous task must be iterable.
Otherwise it will mark the pipeline as failed.

If called tasks returned `True` for some element, this element will be added in the final list.

After the execution you'll get a list with filtered results.
You can add filters by calling `.filter` method of the pipeline.
