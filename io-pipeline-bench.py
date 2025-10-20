#!/usr/bin/env python3

import argparse
import array
import time
import random
import os
import asyncio
import aiofiles
import sys
import multiprocessing
import concurrent.futures
import threading

# --- Module-level Constants ---
# how many bytes to increment when iterating through buffers in compute
# phase.  Set to 1 to process all bytes, 2 to process every 2nd byte, etc.
# This is a crude way to control the compute cost in the pipeline
COMPUTE_SPARSITY = 8

# base class for pipeline
class pipeline:
    """Base class for I/O pipeline implementations with configurable concurrency."""
    
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        """Initialize pipeline with buffer size, concurrency level, receive delay, and output file base name."""
        self.buffer_size_bytes = buffer_size_bytes
        self.concurrency = concurrency
        self.buffers_xferred = 0
        self.elapsed = 0
        self.recv_delay = recv_delay
        self.buffer_list = []
        self.file_ref_list = []
        self.output_file_base = output_file_base
        self.cumul_compute_time = 0

        # create a list of buffers
        for i in range(0, self.concurrency):
            self.buffer_list.append(array.array('B',
                                    bytes(self.buffer_size_bytes)))

    def _compute(self, buffer_idx: int) -> float:
        """Fill buffer with random data and return computation time."""
        start_ts = time.perf_counter()
        # fill the specified buffer with random data, byte by byte
        for i in range(0, self.buffer_size_bytes, COMPUTE_SPARSITY):
            random_byte_int = random.randint(0,255)
            self.buffer_list[buffer_idx][i] = random_byte_int
        return(time.perf_counter() - start_ts)

    def _recv(self):
        """Simulate data reception delay by sleeping for configured time."""
        # don't do anything except wait for configurable time to mimic a
        # delay in receiving data
        time.sleep(self.recv_delay)

    def _write(self, buffer_idx: int, file_ref):
        """Write buffer to file with flush and fsync for durability."""
        # write
        file_ref.write(self.buffer_list[buffer_idx].tobytes())
        # flush Python buffer
        file_ref.flush()
        # sync the write at the FS level
        os.fsync(file_ref.fileno())

    def run(self, duration_s: int):
        """Run the pipeline for specified duration. Override in subclasses."""
        pass

    def report_timing(self, method:str, duration_s:int):
        """Print performance metrics in tabular format."""

        if hasattr(sys, '_is_gil_enabled'):
            is_gil_currently_enabled = sys._is_gil_enabled()
        else:
            is_gil_currently_enabled = True
        major_minor = f"{sys.version_info.major}.{sys.version_info.minor}"
        MiB = self.buffers_xferred * self.buffer_size_bytes/(1024*1024)
        compute_rate = MiB/self.cumul_compute_time;

        print(f"#<Python version>\t<GIL enabled>\t<method>\t<concurrency>\t"
              f"<duration>\t<KiB buffer_size>\t<recv delay>\t<MiB xferred>\t<sequential compute "
              f"throughput (MiB/s)>\t<seconds>\t<aggregate overall throughput (MiB/s)>")
        print(f"{major_minor}\t{is_gil_currently_enabled}\t{method}\t"
              f"{self.concurrency}\t{duration_s}\t{int(self.buffer_size_bytes/1024)}\t{self.recv_delay:.3f}\t"
              f"{MiB:.3f}\t{compute_rate:.3f}\t{self.elapsed:.3f}\t{MiB/self.elapsed:.3f}")

# sequential version of pipeline
class pipeline_sequential(pipeline):
    """Sequential pipeline implementation that processes one buffer at a time."""
    
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        """Initialize sequential pipeline. Concurrency must be 1."""

        if concurrency != 1:
            raise ValueError(f"Invalid 'concurrency' value for pipeline_sequential. Expected 1, but got {concurrency}.")

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    def run(self, duration_s: int):
        """Execute sequential pipeline: recv -> compute -> write in a single loop."""
        my_filename = self.output_file_base + f"0"
        with open(my_filename, 'wb') as f:

            # no concurrency, we just step by step execute the steps in a loop
            start_ts = time.perf_counter()

            while (time.perf_counter() - start_ts) < duration_s:
                # recv data
                self._recv()
                # compute
                self.cumul_compute_time += self._compute(0)
                # write and flush
                self._write(0, f)
                # bookkeeping
                self.buffers_xferred += 1

            self.elapsed = time.perf_counter() - start_ts

        os.unlink(my_filename)


# threading version of pipeline
class pipeline_threading(pipeline):
    """Threading-based pipeline implementation using ThreadPoolExecutor."""
    
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        """Initialize threading pipeline with specified parameters."""

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    def _per_buffer_loop(self, buffer_idx: int, duration_s: int, barrier):
        """Execute pipeline loop for a single buffer in a separate thread."""
        result = {}

        my_buffers_xferred = 0;
        my_cumul_compute_time = 0;
        my_filename = self.output_file_base + f".{buffer_idx}"

        # Note that we can use a context manager in this case since we have
        # combined all of the file operations into a single function so that
        # it is multiprocessing safe (file references cannot be exchanged
        # across processes)
        with open(my_filename, 'wb') as f:

            # synchronize after getting files opened
            barrier.wait()

            # start timer (local)
            my_start_ts = time.perf_counter()

            while (time.perf_counter() - my_start_ts) < duration_s:
                # recv data
                self._recv()
                # compute
                my_cumul_compute_time += self._compute(buffer_idx)
                # write and flush
                self._write(buffer_idx, f)
                # bookkeeping
                my_buffers_xferred += 1

        result['elapsed'] = time.perf_counter() - my_start_ts
        result['buffers_xferred'] = my_buffers_xferred;
        result['cumul_compute_time'] = my_cumul_compute_time

        os.unlink(my_filename)

        return(result)

    def run(self, duration_s: int):
        """Execute concurrent pipeline using ThreadPoolExecutor."""
        # launch concurrent pipelines

        # Note that each function opens and closes its own files, following
        # the pattern used by the multiprocessing example, even though file
        # references could be shared across threads.
        barrier = threading.Barrier(self.concurrency)
        futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            for i in range(0, self.concurrency):
                future = executor.submit(self._per_buffer_loop,
                                         buffer_idx=i,
                                         duration_s=duration_s,
                                         barrier=barrier)
                futures[future] = i

            # Iterate over futures as they complete, in the order of completion
            for future in concurrent.futures.as_completed(futures):
                task_id = futures[future] # Get the original task_id from the future
                try:
                    result = future.result()
                    self.buffers_xferred += result['buffers_xferred']
                    self.cumul_compute_time += result['cumul_compute_time']
                    if(result['elapsed'] > self.elapsed):
                        self.elapsed = result['elapsed']
                except Exception as exc:
                    print(f"Task {task_id} generated an exception: {exc}")



# multiprocess version of pipeline
class pipeline_multiprocess(pipeline):
    """Multiprocess-based pipeline implementation using ProcessPoolExecutor."""
    
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        """Initialize multiprocess pipeline with specified parameters."""

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    def _per_buffer_loop(self, buffer_idx: int, duration_s: int, barrier):
        """Execute pipeline loop for a single buffer in a separate process."""
        result = {}

        my_buffers_xferred = 0
        my_cumul_compute_time = 0
        my_filename = self.output_file_base + f".{buffer_idx}"

        # Note that we can use a context manager in this case since we have
        # combined all of the file operations into a single function so that
        # it is multiprocessing safe (file references cannot be exchanged
        # across processes)
        with open(my_filename, 'wb') as f:

            # synchronize after getting files opened
            barrier.wait()

            # start timer (local)
            my_start_ts = time.perf_counter()

            while (time.perf_counter() - my_start_ts) < duration_s:
                # recv data
                self._recv()
                # compute
                my_cumul_compute_time += self._compute(buffer_idx)
                # write and flush
                self._write(buffer_idx, f)
                # bookkeeping
                my_buffers_xferred += 1

        result['elapsed'] = time.perf_counter() - my_start_ts
        result['buffers_xferred'] = my_buffers_xferred;
        result['cumul_compute_time'] = my_cumul_compute_time

        os.unlink(my_filename)

        return(result)

    def run(self, duration_s: int):
        """Execute concurrent pipeline using ProcessPoolExecutor with multiprocessing Manager."""
        # launch concurrent pipelines
        with multiprocessing.Manager() as manager:

            # Note that in this version, each concurrent pipeline has to open
            # and close its own file, because there is no way to share a file
            # descriptor across multiple processes.  We use a barrier to make
            # sure that the open cost is not counted in the pipeline time.
            barrier = manager.Barrier(self.concurrency)
            futures = {}
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.concurrency) as executor:
                for i in range(0, self.concurrency):
                    future = executor.submit(self._per_buffer_loop,
                                             buffer_idx=i,
                                             duration_s=duration_s,
                                             barrier=barrier)
                    futures[future] = i

                # Iterate over futures as they complete, in the order of completion
                for future in concurrent.futures.as_completed(futures):
                    task_id = futures[future] # Get the original task_id from the future
                    try:
                        result = future.result()
                        self.buffers_xferred += result['buffers_xferred']
                        self.cumul_compute_time += result['cumul_compute_time']
                        if(result['elapsed'] > self.elapsed):
                            self.elapsed = result['elapsed']
                    except Exception as exc:
                        print(f"Task {task_id} generated an exception: {exc}")


# asyncio version of pipeline
class pipeline_asyncio(pipeline):
    """Asyncio-based pipeline implementation using async/await and aiofiles."""
    
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        """Initialize asyncio pipeline with specified parameters."""

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    async def _openfiles(self, filename: str):
        """Open all output files asynchronously using aiofiles."""
        # open files
        for i in range(0, self.concurrency):
            filename = self.output_file_base + f".{i}"
            self.file_ref_list.append(await aiofiles.open(filename, 'wb'))

    async def _close_files(self):
        """Close all files and unlink them."""
        # close and unlink files
        for i in range(0, self.concurrency):
            await self.file_ref_list[i].close()
            filename = self.output_file_base + f".{i}"
            os.unlink(filename)

    async def _recv(self):
        """Async version of recv that uses asyncio.sleep for non-blocking delay."""
        # note that we override this function for asyncio so that we can use
        # an async-aware sleep function

        # don't do anything except wait for configurable time to mimic a
        # delay in receiving data
        await asyncio.sleep(self.recv_delay)

    async def _write(self, buffer_idx: int, file_ref):
        """Async version of write with flush and fsync using aiofiles and asyncio.to_thread."""
        buffer_bytes = self.buffer_list[buffer_idx].tobytes()
        # write
        await file_ref.write(buffer_bytes)
        # flush Python buffer
        await file_ref.flush()
        # sync the write at the FS level
        # note that there is no true fsync exposed by aiofiles, we have to
        # use the to_thread asyncio option to call the os function
        await asyncio.to_thread(os.fsync, file_ref.fileno())

    async def _per_buffer_loop(self, buffer_idx: int, duration_s: int) -> int:
        """Execute async pipeline loop for a single buffer."""
        my_cumul_compute_time = 0
        my_buffers_xferred = 0
        result = {}

        while (time.perf_counter() - self.start_ts) < duration_s:
            # recv data
            await self._recv()
            # compute
            my_cumul_compute_time += self._compute(buffer_idx)
            # write and flush
            await self._write(buffer_idx, self.file_ref_list[buffer_idx])

            # bookkeeping
            my_buffers_xferred += 1

        result['elapsed'] = time.perf_counter() - self.start_ts
        result['buffers_xferred'] = my_buffers_xferred
        result['cumul_compute_time'] = my_cumul_compute_time

        return(result)

    async def _concurrent_run(self, duration_s: int):
        """Execute concurrent async pipeline using asyncio.gather for task coordination."""
        await self._openfiles(self.output_file_base)

        # create an array of N tasks for desired concurrency; each will
        # essentially operate an independent loop on it's corresponding
        # buffer/file
        tasks = [self._per_buffer_loop(idx, duration_s) for idx in range(0,
                                                                         self.concurrency)]

        # start timer
        self.start_ts = time.perf_counter()

        # run tasks concurrently; each will continue until time is elapsed
        results = await asyncio.gather(*tasks)

        # sum buffers xferred and find the max elapsed time
        for result in results:
            self.buffers_xferred += result['buffers_xferred']
            self.cumul_compute_time += result['cumul_compute_time']
            if(result['elapsed'] > self.elapsed):
                self.elapsed = result['elapsed']

        await self._close_files()

    def run(self, duration_s: int):
        """Run async pipeline by executing asyncio.run on the concurrent implementation."""
        # this function is really just a wrapper around _concurrent loop so
        # that we can launch it and await it's completion from a non-async
        # function
        asyncio.run(self._concurrent_run(duration_s))


def main():
    """Parse command line arguments and execute the specified pipeline benchmark."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', type=str, required=True,
                        help='What pipeline method to use.')
    parser.add_argument('--duration', type=int, required=True,
                        help='How many seconds to execute.')
    parser.add_argument('--buffer-size', type=int, required=True,
                        help='Size of each transfer buffer in KiB.')
    parser.add_argument('--concurrency', type=int, required=True,
                        help='Level of concurrency.')
    parser.add_argument('--recv-delay', type=float, required=True,
                        help='(float) seconds to sleep per emulated recv')
    parser.add_argument('--output-file', type=str, required=True,
                        help='base name of output file')
    args = parser.parse_args()

    if args.method == "sequential":
        my_pipeline = pipeline_sequential(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency,
                                          recv_delay=args.recv_delay,
                                          output_file_base=args.output_file);
    elif args.method == "asyncio":
        my_pipeline = pipeline_asyncio(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency,
                                          recv_delay=args.recv_delay,
                                          output_file_base=args.output_file);
    elif args.method == "multiprocess":
        my_pipeline = pipeline_multiprocess(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency,
                                          recv_delay=args.recv_delay,
                                          output_file_base=args.output_file);
    elif args.method == "threading":
        my_pipeline = pipeline_threading(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency,
                                          recv_delay=args.recv_delay,
                                          output_file_base=args.output_file);
    else:
        raise ValueError(f"Invalid method: {args.method}")

    my_pipeline.run(duration_s=args.duration)

    my_pipeline.report_timing(method=args.method, duration_s=args.duration)


if __name__ == "__main__":
    main()
