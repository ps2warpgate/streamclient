import asyncio
import json
import logging.handlers
import os
from dataclasses import asdict
from datetime import datetime, timedelta

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv
from motor import motor_asyncio
from prometheus_client import Counter, Enum, Gauge, Info, start_http_server
from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder
from pymongo.errors import CollectionInvalid

from constants import models
from constants.typings import UniqueEventId
from constants.utils import CustomFormatter, is_docker
from services import Rabbit

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
METRICS_PORT = os.getenv('METRICS_PORT', 8000)
PURGE_STALE_ALERTS = os.getenv('PURGE_STALE_ALERTS', 'True')

log = logging.getLogger('ess')
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


rabbit = Rabbit()


rabbit_service = Enum(
    name='rabbit_service_state',
    documentation='state of the rabbitmq service',
    states=['starting', 'running', 'stopped']
)
events_total = Counter(
    name='events_total',
    documentation='total number of received events',
    labelnames=['event_name', 'world_id', 'zone_id']
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


@dataclass
class EventMetadata:
    event_name: str
    world_id: int
    zone_id: int


@dataclass
class CensusEvent:
    metadata: EventMetadata
    timestamp: datetime
    attacker_id: int
    victim_id: int


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


async def setup_timeseries(mongo: motor_asyncio.AsyncIOMotorClient) -> None:
    """Creates the timeseries collection for storing population-related events"""
    db = mongo[MONGODB_DB]
    try:
        await db.create_collection(
            name='realtime',
            timeseries={
                'metadata': {
                    'event_name': str,
                    'world_id': int,
                    'zone_id': int
                },
                # TODO: Fix 'bson.errors.InvalidDocument: cannot encode object: <class 'str'>, of type: <class 'type'>'
                'timestamp': datetime,
                'attacker_id': int,
                'victim_id': int
            },
            expireAfterSeconds=1800
        )
    except CollectionInvalid:
        log.info('Collection already exists!')


async def purge_stale_alerts(mongo: motor_asyncio.AsyncIOMotorClient) -> None:
    """Removes alerts from the database that are older than 1h30m"""
    db = mongo[MONGODB_DB]
    max_age = datetime.utcnow() - timedelta(hours=1, minutes=30)
    # List of alerts where timestamp < max_age
    stale_alerts: list[dict] = []
    cursor = db.alerts.find({'timestamp': {'$lt': max_age}})
    for document in await cursor.to_list(length=30):
        stale_alerts.append(document)
    if len(stale_alerts) > 0:
        deleted_count = 0
        for i in stale_alerts:
            result = await db.alerts.delete_one({"id": i['id']})
            deleted_count += result.deleted_count
        log.info(f'Found and removed {deleted_count} stale alerts')


async def main() -> None:
    log.info(f'Starting ESS client version: {APP_VERSION}')

    await start_services()

    mongo = motor_asyncio.AsyncIOMotorClient(MONGODB_URL)
    db = mongo[MONGODB_DB]
    await setup_timeseries(mongo)

    if PURGE_STALE_ALERTS == 'True':
        await purge_stale_alerts(mongo)

    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        log.info('Listening for Census Events...')

        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            unique_id = UniqueEventId(evt.world_id, evt.instance_id)

            log.info(f'Received {evt.event_name} id: {unique_id}')

            events_total.labels(evt.event_name, evt.world_id, evt.zone_id).inc()
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
                timestamp=evt.timestamp
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
                await db.alerts.insert_one(dict_event)

                log.info(f'Created alert {unique_id}')
            elif evt.metagame_event_state_name == 'ended' or 'cancelled':
                await db.alerts.delete_one({"id": unique_id})

                log.info(f'Removed alert {unique_id}')

        @client.trigger(event.Death)
        async def on_death(evt: event.Death) -> None:
            event_data = CensusEvent(
                metadata=EventMetadata(
                    event_name=evt.event_name,
                    world_id=evt.world_id,
                    zone_id=evt.zone_id
                ),
                timestamp=evt.timestamp,
                attacker_id=evt.attacker_character_id,
                victim_id=evt.character_id
            )
            events_total.labels(evt.event_name, evt.world_id, evt.zone_id).inc()

            dict_event = asdict(event_data)
            result = await db.realtime.insert_one(dict_event)
            log.debug(result.inserted_id)

        @client.trigger(event.VehicleDestroy)
        async def on_vehicle_destroy(evt: event.VehicleDestroy) -> None:
            event_data = CensusEvent(
                metadata=EventMetadata(
                    event_name=evt.event_name,
                    world_id=evt.world_id,
                    zone_id=evt.zone_id
                ),
                timestamp=evt.timestamp,
                attacker_id=evt.attacker_character_id,
                victim_id=evt.character_id
            )
            events_total.labels(evt.event_name, evt.world_id, evt.zone_id).inc()

            dict_event = asdict(event_data)
            result = await db.realtime.insert_one(dict_event)
            log.debug(result.inserted_id)


if __name__ == '__main__':
    try:
        start_http_server(port=int(METRICS_PORT))
        log.info(f'Metrics service listening on port {METRICS_PORT}')
        loop = asyncio.new_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
