# LocalData Tasks
A background task system for LocalData

### Queue Lists
Example:

```json
[
  ["high", "export","med","low"],
  ["high","med","low"]
]
```

`config.queues` contains an array of queue lists. Each list results in one worker, which will process the specified queues. In the example above, we will have two workers. One will process the `high`, `med`, and `low` queues. The other will process those and also the `export` queue. We might exclude the `export` queue from the second list if we want to avoid having two workers processing export jobs simultaneously, perhaps for memory reasons.


### Running

To run the worker manager, which spins up workers to handle jobs:

```
$ envrun -e local.env node index.js
```

To enqueue a job:

```
$ envrun -e local.env node lib/enqueue.js export csv-exporter '{"survey":"da3a46e0-8be9-11e3-8d76-69d42aceef14","s3Object":"test1","bucket":"localdata-export"}'
```

### TODO
+ Factor out the workers into separate repos.
+ Create a heroku buildpack that lets us install workers based on an environment variable
+ Have worker apps register the job types they handle?
+ Allow other Redis providers
+ Factor so that one fairly lightweight module can provide the worker management stuff and a code interface for enqueueing jobs without bringing along all of the dependencies for executing the jobs?
+ When a client adds some work to the queue, report a max processing time, if applicable.

