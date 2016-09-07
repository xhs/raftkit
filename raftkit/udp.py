#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from .raftkit import RaftMessage, RaftTransportProtocol, RaftProtocol
import asyncio
import socket
import json
import os

RAFTKIT_ADVERTISE_PORT = int(os.environ.get('RAFTKIT_ADVERTISE_PORT', 7789))


class RaftUDPTransport(RaftTransportProtocol):
    def __init__(self, sock):
        self._sock = sock

    def sendto(self, payload, address):
        ip, port = address.split(':')
        self._sock.sendto(payload, (ip, int(port)))


class RaftUDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, agent):
        self._agent = agent

    def datagram_received(self, payload, address):
        message = self._agent.decode_message(payload)
        self._agent.handle_message(message, message.address, self._agent.transport)


class RaftUDPAgent(RaftProtocol):
    def __init__(self, advertise_address, peer_addresses=None, event_loop=None):
        super().__init__(advertise_address, peer_addresses, event_loop)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._transport = RaftUDPTransport(sock)

    @property
    def transport(self):
        return self._transport

    def service_coroutine(self):
        listening_addr = ('0.0.0.0', RAFTKIT_ADVERTISE_PORT)
        return self.loop.create_datagram_endpoint(lambda: RaftUDPProtocol(self), local_addr=listening_addr,
                                                  reuse_address=True, reuse_port=True)

    def broadcast(self, payload, addresses):
        for address in addresses:
            self._transport.sendto(payload, address)

    def encode_message(self, message):
        return json.dumps(message).encode('utf-8')

    def decode_message(self, payload):
        message = json.loads(payload.decode('utf-8'))
        return RaftMessage(message)
