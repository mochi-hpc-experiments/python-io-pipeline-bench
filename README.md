# io-pipeline-bench

This is a simple benchmark to measure the throughput of various I/O
pipelining methods in Python.

## Installation

* create a python venv
* `pip install -r requirements.txt`

## Notes on pipelining methods

Each implementation is embarrassingly parallely; N copies of a pipeline work
to relay data from a mock recv function, to a mock compute function, to a
write function that stores the files on disk.  Each pipeline writes to a
separate file to prevent file contention from skewing the results.

### Sequential

The sequential method is not parallel at all; it only supports
concurrency=1.

### Asyncio

This method uses Asyncio to execute N concurrent pipelines.  Some important
points to know:
* Asyncio uses coroutines to execute tasks in parallel.  This means that
  there is no true concurrency for computation, and only resources with
  asyncio interfaces will be able to proceed concurrently.
* There is no native way to do file I/O in asyncio, really.  We use the
  aiofiles module, which delegates I/O operations to a thread pool so that
  they do not block asynchronous progress.

### Multiprocess

This method uses true processes (analogous to what you would get by forking)
to execute concurrent pipelines.  This introduces a few caveats:
* There isn't a good way to share a file descriptor across processes.  So
  each pipeline has to do it's own file open and file close.  We use a
  barrier to synchronize them before starting the timed part of the
  pipeline.
* In order for processes to share a barrier, the barrier must be passed as
  an argument so that it can be properly pickled.
* Generally speaking, you have to be thoughtful about how to exchange state
  across processes.
