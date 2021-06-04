# Clockwork
Project Clockwork’s goal is a big bet to reduce pipeline landing time variance by building a reservation based scheduler for a data warehouse.

## Examples
> python3 main.py
```
DEBUG:asyncio:Using selector: EpollSelector
DEBUG:algorithm.algorithm:Presto Metadata Size 3
DEBUG:algorithm.right_based.algorithm:Scheduled 0/3, 0 accepted
DEBUG:algorithm.algorithm:Presto Plan Size 3
DEBUG:algorithm.algorithm:Spark Metadata Size 3
DEBUG:algorithm.right_based.algorithm:Scheduled 0/3, 0 accepted
DEBUG:algorithm.algorithm:Spark Metadata Size 2
DEBUG:planner.planner:Planning Finished | In Plan: 5 | Missing from Plan: 1
DEBUG:planner.plan_writer:Final Plan: {TaskInstance(task_id='task6', period_id=Timestamp(10)): Timestamp(60), TaskInstance(task_id='task3', period_id=Timestamp(0)): Timestamp(100), TaskInstance(task_id='task5', period_id=Timestamp(10)): Timestamp(35), TaskInstance(task_id='task2', period_id=Timestamp(0)): Timestamp(40), TaskInstance(task_id='task1', period_id=Timestamp(0)): Timestamp(20)}
```


## Requirements

See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License
Clockwork is MIT licensed, as found in the LICENSE file.
