import sys
import argparse
import numpy as np

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--scheduler-address", required=True, type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", type=str, help="the objstore's address")
parser.add_argument("--redis-host", type=str, default='localhost', help="Redis host")
parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")

if __name__ == "__main__":
  args = parser.parse_args()
  ray.worker.connect(args.node_ip_address, args.scheduler_address, redis_host=args.redis_host, redis_port=args.redis_port)

  ray.worker.main_loop()
