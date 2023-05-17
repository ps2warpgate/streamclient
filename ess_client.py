import asyncio
import json
import logging.handlers
import os
from typing import Dict

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv
from prometheus_client import Counter, Enum, Gauge, Info, start_http_server

from constants.typings import UniqueEventId
from constants.utils import CustomFormatter, is_docker
from services import Alert, Rabbit

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()
# TODO: use getenv 'default' parameter
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
APP_VERSION = os.getenv('APP_VERSION') or 'dev'
API_KEY = os.getenv('API_KEY') or 's:example'
RABBITMQ_ENABLED = os.getenv('RABBITMQ_ENABLED') or 'True'
RABBITMQ_URL = os.getenv('RABBITMQ_URL') or None
MONGODB_URL = os.getenv('MONGODB_URL') or None
MONGODB_DB = os.getenv('MONGODB_DB') or 'warpgate'
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION') or 'alerts'
METRICS_PORT = os.getenv('METRICS_PORT') or 8000

log = logging.getLogger('ess')
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)

WORLD_NAMES: Dict[int, str] = {
    1: 'connery',
    10: 'miller',
    13: 'cobalt',
    17: 'emerald',
    19: 'jaeger',
    40: 'soltech'
}

ZONE_NAMES: Dict[int, str] = {
    2: 'indar',
    4: 'hossin',
    6: 'amerish',
    8: 'esamir',
    344: 'oshur',
}

METAGAME_STATES: Dict[int, str] = {
    135: 'started',
    136: 'restarted',
    137: 'cancelled',
    138: 'ended',
    139: 'xp_bonus_changed'
}

rabbit = Rabbit()
alert = Alert()

alert_service = Enum(
    name='alert_service_state',
    documentation='state of the alert service',
    states=['starting', 'running', 'stopped'])
rabbit_service = Enum(
    name='rabbit_service_state',
    documentation='state of the rabbitmq service',
    states=['starting', 'running', 'stopped']
)
total_events = Counter(
    name='total_events',
    documentation='total number of received events'
)
in_progress_alerts = Gauge(
    name='in_progress_alerts',
    documentation='number of in-progress alerts'
)
last_event_time = Gauge(
    name='last_alert_time',
    documentation='timestamp of the last received event',
)
census_status = Info(
    name='census_status',
    documentation='status of the census endpoint'
)


async def start_services():
    log.info('Starting Services...')

    if RABBITMQ_ENABLED == 'True':
        if not rabbit.is_ready:
            rabbit_service.state('starting')
            await rabbit.setup(RABBITMQ_URL)

        log.info('RabbitMQ Service ready!')
        rabbit_service.state('running')
    else:
        log.info('RabbitMQ Service disabled by user')
        rabbit_service.state('stopped')

    alert_service.state('stopped')

    if not alert.is_ready:
        alert_service.state('starting')
        await alert.setup(
            mongodb_url=MONGODB_URL,
            db='warpgate_dev',
            collection='alerts',
        )

    log.info('Alert Service ready!')
    alert_service.state('running')


async def main() -> None:
    log.info(f'Starting ESS client version: {APP_VERSION}')

    await start_services()

    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        log.info('Listening for Census Events...')

        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            unique_id = str(UniqueEventId(evt.world_id, evt.instance_id))

            log.info(f'Received {evt.event_name} id: {unique_id}')

            total_events.inc(1)
            last_event_time.set(evt.timestamp.timestamp())

            # event data as json object
            event_data = {
                '_id': unique_id,
                'event_id': evt.metagame_event_id,
                'state': evt.metagame_event_state_name,
                'world_id': evt.world_id,
                'zone_id': evt.zone_id,
                'nc': evt.faction_nc,
                'tr': evt.faction_tr,
                'vs': evt.faction_vs,
                'xp': evt.experience_bonus,
                'timestamp': evt.timestamp.timestamp()
            }

            log.debug(event_data)

            # Convert to string
            json_event = json.dumps(event_data)

            # Publish to RabbitMQ
            if RABBITMQ_ENABLED == 'True':
                await rabbit.publish(bytes(json_event, encoding='utf-8'))
                log.info(f'Event {unique_id} published')

            # Add or remove from database
            if evt.metagame_event_state_name == 'started':
                result = await alert.create(event_data)

                log.info(f'Created alert {result}')
            elif evt.metagame_event_state_name == 'ended' or 'cancelled':
                await alert.remove(event_id=unique_id)

                log.info(f'Removed alert {unique_id}')

    _ = on_metagame_event


if __name__ == '__main__':
    try:
        start_http_server(port=int(METRICS_PORT))
        log.info(f'Metrics service listening on port {METRICS_PORT}')
        loop = asyncio.new_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
