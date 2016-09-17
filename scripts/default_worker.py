import sys
import argparse
import numpy as np

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--scheduler-address", required=True, type=str, help="the scheduler's address")
parser.add_argument("--redis-address", required=True, type=str, help="the port to use for Redis")
parser.add_argument("--object-store-name", type=str, help="the object store's name")
parser.add_argument("--object-store-manager-port", type=int, help="the objstore manager's port")

if __name__ == "__main__":
  args = parser.parse_args()
  ray.worker.connect(args.node_ip_address, args.scheduler_address, args.redis_address, args.object_store_name, args.object_store_manager_port)

  ray.worker.main_loop()
