import asyncio
import json
import logging.handlers
import os
from dataclasses import asdict
from datetime import datetime

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv
from prometheus_client import Counter, Enum, Gauge, Info, start_http_server
from pydantic.json import pydantic_encoder

from constants import models
from constants.typings import UniqueEventId
from constants.utils import CustomFormatter, is_docker
from services import Alert, Rabbit

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
APP_VERSION = os.getenv('APP_VERSION', 'dev')
API_KEY = os.getenv('API_KEY', 's:example')
RABBITMQ_ENABLED = os.getenv('RABBITMQ_ENABLED', 'True')
RABBITMQ_URL = os.getenv('RABBITMQ_URL', None)
MONGODB_URL = os.getenv('MONGODB_URL', None)
MONGODB_DB = os.getenv('MONGODB_DB', 'warpgate')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'alerts')
METRICS_PORT = os.getenv('METRICS_PORT', 8000)
PURGE_STALE_ALERTS = os.getenv('PURGE_STALE_ALERTS', 'True')

log = logging.getLogger('ess')
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


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


async def start_services() -> None:
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


async def purge_stale_alerts() -> None:
    """Removes alerts from the database that are older than 5400s (1h30m)"""
    max_age = datetime.utcnow().timestamp() - 5400  # Current POSIX timestamp minus 1h30m
    # List of alerts where timestamp < max_age
    stale_alerts = await alert.read_many(length=30, query={'timestamp': {'$lt': max_age}})
    if len(stale_alerts) > 0:
        log.info(f'Found {len(stale_alerts)} stale alert(s) in the database. Removing...')
        deleted_count = 0
        for i in stale_alerts:
            deleted_count += await alert.remove(event_id=i['id'])
        log.info(f'Stale alerts removed: {deleted_count}')


async def main() -> None:
    log.info(f'Starting ESS client version: {APP_VERSION}')

    await start_services()

    if PURGE_STALE_ALERTS == 'True':
        await purge_stale_alerts()

    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        log.info('Listening for Census Events...')

        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            unique_id = str(UniqueEventId(evt.world_id, evt.instance_id))

            log.info(f'Received {evt.event_name} id: {unique_id}')

            total_events.inc(1)
            last_event_time.set(evt.timestamp.timestamp())

            event_data = models.MetagameEvent(
                id=unique_id,
                event_id=evt.metagame_event_id,
                state=evt.metagame_event_state_name,
                world_id=evt.world_id,
                zone_id=evt.zone_id,
                nc=evt.faction_nc,
                tr=evt.faction_tr,
                vs=evt.faction_vs,
                xp=evt.experience_bonus,
                timestamp=evt.timestamp.timestamp()
            )

            # Convert to dictionary and json
            dict_event = asdict(event_data)
            json_event = json.dumps(event_data, indent=4, default=pydantic_encoder)

            log.debug(dict_event)

            # Publish to RabbitMQ
            if RABBITMQ_ENABLED == 'True':
                await rabbit.publish(bytes(json_event, encoding='utf-8'))
                log.info(f'Event {unique_id} published')
            # Add or remove from database
            if evt.metagame_event_state_name == 'started':
                result = await alert.create(dict_event)

                log.info(f'Created alert {unique_id}')
                log.debug(result)
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
