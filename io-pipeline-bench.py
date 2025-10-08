#!/usr/bin/env python3

import argparse
import array
import time
import random
import os
import asyncio

# base class for pipeline
class pipeline:
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):
        self.buffer_size_bytes = buffer_size_bytes
        self.concurrency = concurrency
        self.buffers_xferred = 0
        self.elapsed = 0
        self.recv_delay = recv_delay
        self.buffer_list = []
        self.file_ref_list = []
        self.output_file_base = output_file_base

        # create a list of buffers, and also open a file for each one
        for i in range(0, self.concurrency):
            filename = self.output_file_base + f".{i}"
            self.buffer_list.append(array.array('B',
                                    bytes(self.buffer_size_bytes)))
            self.file_ref_list.append(open(filename, 'wb'))

    def close(self):
        for i in range(0, self.concurrency):
            self.file_ref_list[i].close()
            filename = self.output_file_base + f".{i}"
            os.unlink(filename)

        self.buffer_list = []

    def run(self, duration_s: int):
        pass

    def report_timing(self):

        MiB = self.buffers_xferred * self.buffer_size_bytes/(1024*1024)
        print(f"Transferred {MiB} MiB in {self.elapsed} seconds.")
        print(f"{MiB/self.elapsed} MiB/s")

# sequential version of pipeline
class pipeline_sequential(pipeline):
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):

        if concurrency != 1:
            raise ValueError(f"Invalid 'concurrency' value for pipeline_sequential. Expected 1, but got {concurrency}.")

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    def _recv(self):
        # don't do anything except wait for configurable time to mimic a
        # delay in receiving data
        time.sleep(self.recv_delay)

    def _compute(self, buffer_idx: int):
        # fill the specified buffer with random data, byte by byte
        for i in range(0, self.buffer_size_bytes):
            random_byte_int = random.randint(0,255)
            self.buffer_list[buffer_idx][i] = random_byte_int

    def _write(self, buffer_idx: int):
        # write
        self.file_ref_list[buffer_idx].write(self.buffer_list[buffer_idx].tobytes())
        # flush Python buffer
        self.file_ref_list[buffer_idx].flush()
        # sync the write at the FS level
        os.fsync(self.file_ref_list[buffer_idx].fileno())

    def run(self, duration_s: int):

        # no concurrency, we just step by step execute the steps in a loop
        start_ts = time.perf_counter()

        while (time.perf_counter() - start_ts) < duration_s:
            # 3 steps per pipeline:

            # recv data
            self._recv()

            # compute
            self._compute(0)

            # write and flush
            self._write(0)

            # bookkeeping
            self.buffers_xferred += 1

        self.elapsed = time.perf_counter() - start_ts

# asyncio version of pipeline
class pipeline_asyncio(pipeline):
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float, output_file_base: str):

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay,
                         output_file_base=output_file_base)

    def _recv(self):
        # don't do anything except wait for configurable time to mimic a
        # delay in receiving data
        time.sleep(self.recv_delay)

    def _compute(self, buffer_idx: int):
        # fill the specified buffer with random data, byte by byte
        for i in range(0, self.buffer_size_bytes):
            random_byte_int = random.randint(0,255)
            self.buffer_list[buffer_idx][i] = random_byte_int

    def _write(self, buffer_idx: int):
        # write
        self.file_ref_list[buffer_idx].write(self.buffer_list[buffer_idx].tobytes())
        # flush Python buffer
        self.file_ref_list[buffer_idx].flush()
        # sync the write at the FS level
        os.fsync(self.file_ref_list[buffer_idx].fileno())

    async def _per_buffer_loop(self, buffer_idx: int, duration_s: int):

        while (time.perf_counter() - self.start_ts) < duration_s:
            # recv data
            self._recv()
            # compute
            self._compute(buffer_idx)
            # write and flush
            self._write(buffer_idx)

            # bookkeeping
            self.buffers_xferred += 1

    async def _concurrent_loop(self, duration_s: int):

        # create an array of N tasks for desired concurrency; each will
        # essentially operate an independent loop on it's corresponding
        # buffer/file
        tasks = [self._per_buffer_loop(idx, duration_s) for idx in range(0,
                                                                         self.concurrency)]

        # start timer
        self.start_ts = time.perf_counter()

        # run tasks concurrently; each will continue until time is elapsed
        results = await asyncio.gather(*tasks)
        # TODO: check results?

        # TODO: think about this: is this what we want?
        # don't count end time until we get back to this point
        self.elapsed = time.perf_counter() - self.start_ts

    def run(self, duration_s: int):

        # this function is really just a wrapper around _concurrent loop so
        # that we can launch it and await it's completion from a non-async
        # function

        asyncio.run(self._concurrent_loop(duration_s))
        # TODO: check result?

def main():
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
    else:
        raise ValueError(f"Invalid method: {args.method}")

    my_pipeline.run(duration_s=args.duration)
    my_pipeline.report_timing()
    # TODO: could refactor this to use a context manager I guess
    my_pipeline.close()


if __name__ == "__main__":
    main()
