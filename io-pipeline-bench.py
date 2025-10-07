#!/usr/bin/env python3

import argparse
import array
import time

# base class for pipeline
class pipeline:
    def __init__(self, buffer_size_bytes: int, concurrency: int):
        self.buffer_size_bytes = buffer_size_bytes
        self.concurrency = concurrency

        self._buffer = array.array('B', bytes(self.buffer_size_bytes))

    def run(self, duration_s: int):
        pass

# sequential version of pipeline
class pipeline_sequential(pipeline):
    def __init__(self, buffer_size_bytes: int, concurrency: int):

        if concurrency != 1:
            raise ValueError(f"Invalid 'concurrency' value for pipeline_sequential. Expected 1, but got {concurrency}.")

        super().__init__(buffer_size_bytes=buffer_size_bytes,
                         concurrency=concurrency)

    def run(self, duration_s: int):

        start_ts = time.perf_counter()

        while (time.perf_counter() - start_ts) < duration_s:
            time.sleep(.1)

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
    args = parser.parse_args()

    if args.method == "sequential":
        my_pipeline = pipeline_sequential(buffer_size_bytes=
                                          (args.buffer_size*1024),
                                          concurrency=args.concurrency);
    else:
        raise ValueError(f"Invalid method: {args.method}")

    my_pipeline.run(duration_s=args.duration)


if __name__ == "__main__":
    main()
