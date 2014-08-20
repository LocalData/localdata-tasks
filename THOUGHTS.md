
Worker apps
+ Register job types
+ Have worker manager to allow multiple node.js worker processes per dyno
+ Queues and queue order comes from environment variable
+ Warn if a subset of queues are being processed
+ Must shut down gracefully, in case we change environment variables during an export, for example.

Clients assigning work
+ Should queues come from a predefined list? (to avoid ad hoc queues that might go unprocessed)
+ Should job types come from a predefined list?


