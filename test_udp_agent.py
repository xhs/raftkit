#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""test_udp_agent.py

Usage:
  test_udp_agent.py <advertise_address> [--peer-addresses=<a0:p0,a1:p1,...>]
  test_udp_agent.py (-h | --help)
  test_udp_agent.py (-v | --version)

Options:
  -h --help             Show this screen.
  -v --version          Show version.
  --join=<router_ip>    Shoutd router IP address
"""
from docopt import docopt
from raftkit import RaftUDPAgent

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)

    args = docopt(__doc__, version='test_udp_agent.py 0.1.0')

    if args.get('--peer-addresses'):
        peer_addresses = args.get('--peer-addresses')
    else:
        peer_addresses = None

    raft = RaftUDPAgent(args.get('<advertise_address>'), peer_addresses=peer_addresses)
    raft.run_forever()
