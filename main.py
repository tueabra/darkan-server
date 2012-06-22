from datetime import datetime
from uuid import uuid4
from types import StringType
import logging

from gevent_zeromq import zmq
import gevent

import psycopg2.extras
from sqlalchemy import MetaData, create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, backref, relationship

from actions import Action

### LOGGING SET UP ###

logger = logging.getLogger('darkan.server')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)

### SQLALCHEMY SET UP ###

engine = create_engine("postgresql://darkan@localhost/darkan", echo=False)
Base = declarative_base()
Session = scoped_session(sessionmaker(bind=engine))
Base.metadata = MetaData()
Base.metadata.bind = engine

psycopg2.extras.register_uuid()

### DATABASES ###

class HostStatus:
    New = 0
    Accepted = 1
    Declined = 2

class Host(Base):
    __tablename__ = 'hosts'

    # metadata
    id       = Column(Integer, primary_key=True)
    added    = Column(DateTime, default=lambda: datetime.now())
    last_report = Column(DateTime, default=None)

    # host information
    hostname = Column(String(50))
    interval = Column(Integer)

    acknowledged = Column(Boolean, default=False)
    key = Column(String(50))

    @property
    def status(self):
        # acknowledged + key = host is accepted
        # acknowledged + no key = host is declined
        # not acknowledged + key = this should not be possible
        # not acknowledged + no key = host is new
        if self.acknowledged:
            if self.key:
                return HostStatus.Accepted
            else:
                return HostStatus.Declined
        return HostStatus.New

    def to_json(self):
        return {
            'id': self.id,
            'key': self.key,
            'hostname': self.hostname,
            'added': self.added.strftime("%Y-%m-%d %H:%M:%S"),
        }

class Report(Base):
    __tablename__ = 'report'

    # metadata
    id      = Column(Integer, primary_key=True)
    added   = Column(DateTime, default=lambda: datetime.now())
    host_id = Column(Integer, ForeignKey(Host.id))

    # relationships
    host = relationship(Host, backref=backref('reports', lazy='dynamic'))

class Value(Base):
    __tablename__ = 'value'

    # metadata
    id        = Column(Integer, primary_key=True)
    added     = Column(DateTime, default=lambda: datetime.now())
    report_id = Column(Integer, ForeignKey(Report.id))

    # identifier
    key = Column(String(100))
    arg = Column(String(100))

    # values
    string  = Column(String(300))
    integer = Column(Integer)
    float   = Column(Float)

    # relationships
    report = relationship(Report, backref=backref('values', lazy='dynamic'))

    def to_json(self):
        return {
            'key': self.key,
            'arg': self.arg,
            'value': self.string or self.integer or self.float,
        }

class Trigger(Base):
    __tablename__ = 'trigger'

    # metadata
    id      = Column(Integer, primary_key=True)
    name    = Column(String(100))
    description = Column(String(300))
    added   = Column(DateTime, default=lambda: datetime.now())
    host_id = Column(Integer, ForeignKey(Host.id))

    # configuration
    expression = Column(String(300))
    action     = Column(String(300))

    # relationships
    host = relationship(Host, backref=backref('triggers', lazy='dynamic'))

    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'added': self.added.strftime("%Y-%m-%d %H:%M:%S"),
            'host_id': self.host_id,
            'expression': self.expression,
            'action': self.action,
        }

#Base.metadata.drop_all()
Base.metadata.create_all()

### MAIN ###

class PackageError(Exception): pass

class BaseServer(object):
    def __init__(self, context, address):
        self.context = context
        self.server = self.context.socket(zmq.REP)
        self.server.bind(address)

    def listen(self):
        self.ready()
        while True:
            msg = self.server.recv_json()
            if not msg:
                break # Interrupted

            try:
                ret = self.on_receive(msg)
            except PackageError, e:
                self.server.send_json({'status': 'ERROR', 'error': e.args[0]})
            else:
                ret.update({'status': 'OK'})
                self.server.send_json(ret)

class PackageServer(BaseServer):

    def ready(self):
        logger.info("PackageServer started!")

    def on_receive(self, msg):
        host = Session.query(Host).filter(Host.hostname==msg['hostname']).first()

        # 1. Is host declined? Then exit
        if host and host.status == HostStatus.Declined:
            raise PackageError("Host declined")

        # 2. Does host exist? If not, add it
        is_new = False
        if not msg['key']:

            if not host:
                host = Host(hostname=msg['hostname'], interval=msg['interval'])
                Session.add(host)
                Session.commit()
                is_new = True

            elif host.status == HostStatus.Accepted:
                raise PackageError("Hostname already registered")

        elif not host or msg['key'] != host.key:
            raise PackageError("Unknown host/key combination")

        # 3. If host is acknowledged or new, save values
        if host.acknowledged or is_new:
            report = Report(host=host)
            Session.add(report)

            for row in msg['values']:
                val = Value(report=report, key=row['key'], arg=row['arg'])
                if isinstance(row['val'], StringType):
                    val.string = row['val']
                elif isinstance(row['val'], int):
                    val.integer = row['val']
                elif isinstance(row['val'], float):
                    val.float = row['val']
                else:
                    raise PackageError('Unknown value type %s for value %s' % (type(row['val']), row['val']))
                Session.add(val)
            Session.commit()

        logger.debug("Received package: %s" % msg)

        return {}

class AdministrationServer(BaseServer):

    def ready(self):
        logger.info("AdministrationServer started!")

    def on_receive(self, msg):
        logger.debug("Received admin command: %s" % msg)

        cmd = msg.get('command', '')
        if cmd.count('.') == 1:
            subject, action = cmd.split('.')

            # hosts
            if subject == 'hosts':

                # .list
                if action == 'list':
                    return {'hosts': [n.to_json() for n in Session.query(Host).filter(Host.acknowledged==True)]}

                # .details
                elif action == 'details':
                    host = Session.query(Host).filter(Host.id==msg['args'][0]).first()
                    return {'host': host.to_json()}

            # autohosts
            elif subject == 'autohosts':

                # .list
                if action == 'list':
                    return {'hosts': [n.to_json() for n in Session.query(Host).filter(Host.acknowledged==False)]}

                # .add
                elif action == 'add':
                    host = Session.query(Host).filter(Host.id==msg['args'][0]).first()
                    host.acknowledged = True
                    host.key = uuid4()
                    Session.add(host)
                    Session.commit()
                    return {'key': host.key}

                # .decline
                elif action == 'decline':
                    host = Session.query(Host).filter(Host.id==msg['args'][0]).first()
                    Session.delete(host)
                    Session.commit()
                    return {}

            # values
            elif subject == 'values':

                # .latest
                if action == 'latest':
                    host = Session.query(Host).filter(Host.id==msg['args'][0]).first()
                    return {'values': [v.to_json() for v in host.reports[-1].values.all()]}

            # triggers
            elif subject == 'triggers':

                # .list
                if action == 'list':
                    return {'triggers': [t.to_json() for t in Session.query(Trigger).all()]}

                # .add
                if action == 'add':
                    trigger = Trigger(
                        host_id     = msg['args'][0]['host'],
                        name        = msg['args'][0]['name'],
                        description = msg['args'][0]['description'],
                        expression  = msg['args'][0]['expression'],
                        action      = msg['args'][0]['action']
                    )
                    Session.add(trigger)
                    Session.commit()
                    return {'id': trigger.id}

            # actions
            elif subject == 'actions':

                # .list
                if action == 'list':
                    return {'actions': Action.get_actions()}

        else:
            raise PackageError('Missing or invalid command')

class TriggerCheck(object):

    def loop(self):
        logger.info("TriggerCheck started!")

        while True:
            logger.debug("Checking triggers...")

            for trigger in Session.query(Trigger).all():
                pass

            gevent.sleep(30) # should probably be like ~5

def main():
    context = zmq.Context()

    pack_serv = PackageServer(context, "tcp://*:12345")
    adm_serv  = AdministrationServer(context, "ipc:///tmp/darkan.sck")
    t_check   = TriggerCheck()

    gevent.joinall([
        gevent.spawn(pack_serv.listen),
        gevent.spawn(adm_serv.listen),
        gevent.spawn(t_check.loop),
    ])

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
