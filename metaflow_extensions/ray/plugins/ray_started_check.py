# The purpose of this file is to ensure that we call `ray.init` outside the metaflow step process and in
# turn extract all the nodes that are a part of the ray cluster without having to mess up the user's
# runtime environment by calling `ray.init` inside the metaflow step process.
import json
import sys


def check_ray_started(main_node_ip, main_port):
    import ray

    # Connect to the existing Ray cluster
    ray.init(address=f"{main_node_ip}:{main_port}")
    ray_nodes = ray.nodes()
    print(json.dumps(ray_nodes))


if __name__ == "__main__":
    main_ip = sys.argv[1]
    main_port = sys.argv[2] if len(sys.argv) > 2 else "6379"
    check_ray_started(main_ip, main_port)
