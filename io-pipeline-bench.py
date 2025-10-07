#!/usr/bin/env python3

import argparse
import array
import time
import random

# base class for pipeline
class pipeline:
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float):
        self.buffer_size_bytes = buffer_size_bytes
        self.concurrency = concurrency
        self.buffers_xferred = 0
        self.elapsed = 0
        self.recv_delay = recv_delay
        self.buffer_list = []

        # create a list of buffers
        for i in range(0, self.concurrency):
            self.buffer_list.append(array.array('B',
                                    bytes(self.buffer_size_bytes)))

    def run(self, duration_s: int):
        pass

    def report_timing(self):

        MiB = self.buffers_xferred * self.buffer_size_bytes/(1024*1024)
        print(f"Transferred {MiB} MiB in {self.elapsed} seconds.")
        print(f"{MiB/self.elapsed} MiB/s")

# sequential version of pipeline
class pipeline_sequential(pipeline):
    def __init__(self, buffer_size_bytes: int, concurrency: int, recv_delay:
                 float):

        if concurrency != 1:
            raise ValueError(f"Invalid 'concurrency' value for pipeline_sequential. Expected 1, but got {concurrency}.")

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency, recv_delay=recv_delay)

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
        pass

    def run(self, duration_s: int):

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
    args = parser.parse_args()

    if args.method == "sequential":
        my_pipeline = pipeline_sequential(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency,
                                          recv_delay=args.recv_delay);
    else:
        raise ValueError(f"Invalid method: {args.method}")

    my_pipeline.run(duration_s=args.duration)
    my_pipeline.report_timing()


if __name__ == "__main__":
    main()
