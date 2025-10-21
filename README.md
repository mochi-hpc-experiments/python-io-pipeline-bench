# io-pipeline-bench

This is a simple benchmark to measure the throughput of various I/O
pipelining methods in Python.

## What does the benchmark do?

The core idea of the benchmark is to run N simultaneous concurrent data transfer
pipelines. The pipelines are meant to be embarrassingly parallel; each pipeline
works to relay data from a recv function, to a compute function, to a write
function. They all proceed simultaneously until a specified amount of time has
elapsed.

![Pipeline Diagram](doc/pipeline.png)

Each data transfer pipeline repeatedly operates on a single fixed-size buffer.  The specified level of concurrency therefore dictates both the number of simultaneous pipelines to execute and the number of memory buffers to use.  

The data transfer steps are defined as follows:
* *recv*: this is meant to emulate receiving data into the buffer.  Rather than actually receiving data, however, it delays for a specified amount of time to emulate waiting on new data to arrive.
* *compute*: this is meant to emulate performing some computation on the data buffer.  In this benchmark, this is done be generating random numbers in the buffer.  There is a constant defined at the top of the benchmark called `COMPUTE_SPARSITY` which indicates the stride pattern of bytes to fill in with random numbers.  A value of 1 there for means more intense computation than a value of 8, for example.
* *write*: this appends the buffer to a local file.  The data is written, the Python buffer is flushed, and the file is sync'ed to disk.

Note that each pipeline writes to a separate file.  This is done intentionally to minimize file system overhead in order to better isolate Python's ability to perform concurrent pipeline transfers.

The benchmark supports multiple methods for pipeline concurrency, as defined in [Notes on concurrent pipelining methods](#notes-on-concurrent-pipelining-methods). 

Command line parameters:
* `--method` (required): What pipeline method to use (sequential, asyncio, multiprocess, threading)
* `--duration` (required): How many seconds to execute the benchmark
* `--buffer-size` (required): Size of each transfer buffer in KiB
* `--concurrency` (required): Level of concurrency (number of parallel pipelines)
* `--recv-delay` (required): Seconds to sleep per emulated recv operation
* `--output-file` (required): Base name of output file (each pipeline writes to a separate file)

## Installation

* create a python venv
* `pip install -r requirements.txt`

See notes at the end of this file for tips on installing a free-threaded Python
interpreter if you would like to experiment with that.

## Running the benchmark

You can run a set of benchmarks sweeping across a range of concurrencies by running `./run-all-basic.sh`.  It takes a single (optional) command line argument to specify the Python venv to use.  If no command line argument is given, then it assumes `./.venv`.  The results will be written to a file called `results.<pid>.dat`.

The underlying benchmark code is in `io-pipeline-bench.py`.  You can look at `run-all-basic.sh` to see examples of how to invoke it in different configurations.

## Notes on concurrent pipelining methods

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
* There isn't any way to share a file descriptor across processes.  So
  each pipeline has to do it's own file open and file close.  We use a
  barrier to synchronize them before starting the timed part of the
  pipeline.
* In order for processes to share a barrier, the barrier must be passed as
  an argument so that it can be properly pickled.  It must also be created
  in a multiprocessing manager.
* Generally speaking, you have to be thoughtful about how to exchange state
  across processes.

### Threading

This method is similar to Multiprocess, except that it uses a ThreadPoolExecutor to run the pipelines in threads rather than separate processes.
* This is expected to perform poorly (due to inability to truly execute computational steps in parallel) unless you are using a "free threaded" Python interpreter, that has been built to disable the GIL (Global Interpreter Lock).

## Installing a non-GIL (free-threaded) Python interpretter

* Download an official Python release tar ball (3.14 as of this writing).
* Install dependencies.  For example on Ubuntu you may want the following:
```
sudo apt-get install libffi-dev libgdbm-dev tk-dev uuid-dev libzstd-dev libsqlite3-dev
```
* Run the following to configure and install, adjusting the installation
  path as desired:
```
./configure --prefix=/home/carns/working/install/python3.14.0-nogil --disable-gil --enable-optimizations
make -j 4
make install
```
* You can then use the python3 binary installed in this path to create a
  venv.
* You can toggle the GIL at runtime by setting the environment variable `PYTHON_GIL=1` or `PYTHON_GIL=0`
