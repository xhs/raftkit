#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import random
import hashlib
import copy
import os

import structlog
structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())
logger = structlog.get_logger('raftkit')

RAFTKIT_HEARTBEAT_INTERVAL = int(os.environ.get('RAFTKIT_HEARTBEAT_INTERVAL', 1000))

HEARTBEAT_INTERVAL = RAFTKIT_HEARTBEAT_INTERVAL / 1000.0
CHECK_INTERVAL = HEARTBEAT_INTERVAL * 1.3
ELECTION_TIMEOUT_LOW = HEARTBEAT_INTERVAL * 2.3
ELECTION_TIMEOUT_HIGH = HEARTBEAT_INTERVAL * 2.9


class RaftMessage(object):
    def __init__(self, message=None):
        if message is None:
            self.raw_dict = {}
        else:
            assert type(message) is dict
            self.raw_dict = message

    def __str__(self):
        return '<RaftMessage %s>' % str(self.raw_dict)

    def __getattr__(self, name):
        return self.raw_dict.get(name)

    def __getitem__(self, key):
        return self.raw_dict.get(key)


class RaftTransportProtocol(object):
    def sendto(self, payload, address):
        raise NotImplementedError


class RaftProtocol(object):
    def __init__(self, advertise_address, peer_addresses=None, event_loop=None):
        self._advertise_address = advertise_address

        if peer_addresses is None:
            self._peers = {}
        else:
            self._peers = {
                hashlib.md5(address.encode('utf-8')).hexdigest(): {
                    'address': address,
                    'retried_times': 0
                } for address in peer_addresses
            }

        if event_loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = event_loop

        if len(self._peers) == 0:
            self._role = 'leader'
        else:
            self._role = 'follower'

        self._id = hashlib.md5(self._advertise_address.encode('utf-8')).hexdigest()
        self._term = 0
        self._voted_for = None
        self._voters = 0
        self._leader = None
        self._election_timeout = asyncio.Queue()

    @property
    def loop(self):
        return self._loop

    @property
    def is_winner(self):
        half = (len(self._peers) + 1) / 2
        return self._voters > half

    @property
    def timeout_seconds(self):
        if self._role == 'leader':
            seconds = HEARTBEAT_INTERVAL
        else:
            seconds = random.uniform(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH)
        return seconds

    def service_coroutine(self):
        raise NotImplementedError

    def broadcast(self, payload, addresses):
        raise NotImplementedError

    def encode_message(self, message):
        raise NotImplementedError

    def decode_message(self, payload):
        raise NotImplementedError

    def decorate_message(self, message):
        return {
            'id': self._id,
            'term': self._term,
            'address': self._advertise_address,
            **message
        }

    def do_broadcast(self, message):
        payload = self.encode_message(self.decorate_message(message))
        addresses = [v['address'] for k, v in self._peers.items()]
        try:
            self.broadcast(payload, addresses)
        except Exception as e:
            logger.error('error.unhandled', exception=e)

    def hello(self):
        for peer in self._peers.values():
            peer['retried_times'] += 1
        self.do_broadcast({'command': 'hello'})

    def farewell(self):
        self.do_broadcast({'command': 'farewell'})

    def campaign(self):
        if len(self._peers) == 0:
            self.become_leader()
        self.do_broadcast({'command': 'vote-for-me'})

    def ping(self):
        self.do_broadcast({'command': 'ping'})

    def become_follower(self, term, leader):
        if self._role != 'follower':
            logger.info('role.changed', id=self._id, old_role=self._role, new_role='follower', term=term, leader=leader)
        self._term = term
        self._leader = leader
        self._role = 'follower'
        self._voted_for = None
        self._voters = 0

    def become_candidate(self):
        self._term += 1
        self._leader = None
        if self._role == 'follower':
            logger.info('role.changed', id=self._id, old_role=self._role, new_role='candidate', term=self._term)
            logger.debug('campaign.started', id=self._id, term=self._term)
            self._role = 'candidate'
        elif self._role == 'candidate':
            logger.debug('campaign.again', id=self._id, term=self._term)
        self._voted_for = self._id
        self._voters = 1

    def become_leader(self):
        logger.info('role.changed', id=self._id, old_role=self._role, new_role='leader', term=self._term)
        self._leader = self._id
        self._role = 'leader'

    def handle_message(self, message, address, transport):
        if message.id == self._id:
            return

        logger.debug('message.received', id=self._id, message=str(message))

        if self._peers.get(message.id) is None and message.id != self._id:
            logger.info('peer.found', id=self._id, peer_id=message.id, peer_address=message.address)
            self._peers[message.id] = {
                'address': message.address,
                'retried_times': 0
            }
            logger.info('peers', id=self._id, peers=self._peers)

        if message.term > self._term:
            self.become_follower(message.term, message.id)

        command = message.command
        if command == 'hello':
            transport.sendto(self.encode_message(self.decorate_message({
                'command': 'hello-ack',
                'known_peers': self._peers
            })), address)
        elif command == 'farewell':
            logger.info('peer.lost', id=self._id, peer_id=message.id, peer_address=message.address)
            del self._peers[message.id]
            logger.info('peers', id=self._id, peers=self._peers)
        elif command == 'vote-for-me':
            if message.term < self._term:
                logger.info('vote.rejected', id=self._id, candidate_id=message.id, peer_term=message.term, own_term=self._term)
                transport.sendto(self.encode_message(self.decorate_message({
                    'command': 'vote-ack',
                    'granted': False
                })), address)
            elif self._role == 'follower' and self._voted_for is None:
                logger.info('vote.granted', id=self._id, candidate_id=message.id, peer_term=message.term, own_term=self._term)
                self._election_timeout.put_nowait(None)
                self._voted_for = message.id
                transport.sendto(self.encode_message(self.decorate_message({
                    'command': 'vote-ack',
                    'granted': True
                })), address)
        elif command == 'ping':
            if self._role == 'follower' and message.term >= self._term:
                self._leader = message.id
                self._election_timeout.put_nowait(None)
                transport.sendto(self.encode_message(self.decorate_message({
                    'command': 'pong'
                })), address)
        elif command == 'vote-ack':
            if message.granted:
                logger.info('vote.got', id=self._id, voter_id=message.id, voters=self._voters)
                self._voters += 1
                if self.is_winner:
                    self.become_leader()
                    self.ping()
            else:
                self._election_timeout.put_nowait(None)
        elif command == 'hello-ack':
            self._peers[message.id]['retried_times'] = 0
            for peer_id, peer in message.known_peers.items():
                if self._peers.get(peer_id) is None and peer_id != self._id:
                    logger.info('peer.found', id=self._id, peer_id=peer_id, peer_address=peer['address'])
                    self._peers[peer_id] = {
                        'address': peer['address'],
                        'retried_times': 0
                    }
                    logger.info('peers', id=self._id, peers=self._peers)
        elif command == 'pong':
            pass

    async def heartbeat(self):
        while True:
            seconds = self.timeout_seconds
            logger.debug('timer.started', id=self._id, timeout_seconds=seconds)
            try:
                await asyncio.wait_for(self._election_timeout.get(), seconds)
                logger.debug('timer.canceled', id=self._id)
            except asyncio.TimeoutError:
                logger.debug('timer.timeout', id=self._id)
                if self._role != 'leader':
                    self.become_candidate()

            if self._role == 'candidate':
                self.campaign()
            elif self._role == 'leader':
                self.ping()

    async def check_health(self):
        while True:
            self.hello()
            await asyncio.sleep(CHECK_INTERVAL)
            peers = copy.deepcopy(self._peers)
            for peer_id, peer in peers.items():
                if peer['retried_times'] > 3:
                    logger.info('peer.lost', id=self._id, peer_id=peer_id, peer_address=peer['address'])
                    del self._peers[peer_id]
                    logger.info('peers', id=self._id, peers=self._peers)

    def run_forever(self):
        logger.info('started', id=self._id)
        logger.info('peers', id=self._id, peers=self._peers)
        try:
            tasks = asyncio.gather(
                asyncio.ensure_future(self.service_coroutine(), loop=self._loop),
                asyncio.ensure_future(self.heartbeat(), loop=self._loop),
                asyncio.ensure_future(self.check_health(), loop=self._loop)
            )
            self._loop.run_until_complete(tasks)
        except KeyboardInterrupt:
            self.farewell()
            tasks.cancel()
            logger.info('stopped', id=self._id)
            self._loop.run_forever()
            tasks.exception()
        finally:
            self._loop.close()
