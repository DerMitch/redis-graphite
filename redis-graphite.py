"""
    Redis Graphite Publisher
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Publishes stats from a redis server to a carbon server.
    These stats include:
    - Generic server stats (INFO command)
    - Length of lists (useful for monitoring queues)

    Requires redis-py:
    https://pypi.python.org/pypi/redis

    Example for a carbon storage schema:
    [redis]
    pattern = ^redis\.
    retentions = 10s:24d,1m:30d,10m:1y

    :license: MIT License
    :author: Michael Mayr <michael@michfrm.net>
"""

import time
import socket
import logging
import sys
from argparse import ArgumentParser

from redis import Redis

log = logging.getLogger("redis-graphite")

stats_keys = [
    # Clients
    ('connected_clients', int),
    ('client_longest_output_list', int),
    ('client_biggest_input_buf', int),
    ('blocked_clients', int),

    # Memory
    ('used_memory', int),
    ('used_memory_rss', int),
    ('used_memory_peak', int),
    ('used_memory_lua', int),
    ('mem_fragmentation_ratio', lambda x: int(float(x) * 100)),

    # Persistence
    ('rdb_bgsave_in_progress', int), # Nice for graphites render 0 as inf
    ('aof_rewrite_in_progress', int), # Nice for graphites render 0 as inf
    ('aof_base_size', int),
    ('aof_current_size', int),

    # Stats
    ('total_connections_received', int),
    ('total_commands_processed', int),
]

parser = ArgumentParser()
# Connections
parser.add_argument('--redis-server', default="localhost")
parser.add_argument('--redis-ports', nargs='+', type=int, default=6379)
parser.add_argument('--carbon-server', default="localhost")
parser.add_argument('--carbon-port', type=int, default=2003)
# Options
parser.add_argument('--no-server-stats', '-s', help="Disable graphing of server stats", action="store_true")
parser.add_argument('--lists', '-l', help="Watch the length of one or more lists", nargs="+")
parser.add_argument('--once', '-o', help="Run only once, then quit", action="store_true")
parser.add_argument('--interval', '-i', help="Check interval in seconds", type=int, default=10)
parser.add_argument('--verbose', '-v', help="Debug output", action="store_true")

def main():
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if isinstance(args.redis_ports, basestring):
        args.redis_ports = [ args.redis_ports ]
    try:
        iter(args.redis_ports)
    except TypeError:
        args.redis_ports = [ args.redis_ports ]

    log.debug("Starting mainloop")

    while True:
        sock = socket.socket()
        sock.settimeout(5.0)
        try:
            sock.connect((args.carbon_server, args.carbon_port))
        except:
            log.debug("Could not connect to graphite on {}:{}".format(args.carbon_server, args.carbon_port))
            time.sleep(2)
            continue

        while True:
            for redis_port in args.redis_ports:
                base_key = "redis.{}:{}.".format(args.redis_server, redis_port)
                log.debug("Base key:{}".format(base_key))

                log.debug("Connecting to redis")
                try:
                    client = Redis(args.redis_server, redis_port)
                    info = client.info()
                    log.debug("Got {} info keys from redis".format(len(info)))
                except:
                    log.debug("Could not connect to {}:{}".format(args.redis_server, redis_port))
                    continue

                if not args.no_server_stats:
                    for key, keytype in stats_keys:
                        if key not in info:
                            log.debug("WARN:Key not supported by redis: {}".format(key))
                            continue
                        value = keytype(info[key])
                        log.debug("gauge {}{} -> {}".format(base_key, key, value))
                        cmd = "{} {} {}\n".format(base_key + key, value, int(time.time()))
                        try:
                            sock.sendall(cmd)
                        except:
                            log.debug("Could not send metrics...")
                            break
                    else:
                        continue
                    break

                if args.lists:
                    lists_key = base_key + "list."
                    for key in args.lists:
                        length = client.llen(key)
                        log.debug("Length of list {}: {}".format(key, length))
                        cmd = "{} {} {}\n".format(base_key + key, length, int(time.time()))
                        try:
                            sock.sendall(cmd)
                        except:
                            log.debug("Could not send metrics...")
                            break
                    else:
                        continue
                    break

            else:
                log.debug("Sleeping {} seconds".format(args.interval))
                time.sleep(args.interval)
                continue
            break

            if args.once:
                sys.exit()

        try:
            sock.close()
        except:
            pass

if __name__ == '__main__':
    main()
