import asyncio
import json
import logging
import logging.handlers
import os
from typing import Dict

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv

from constants.typings import UniqueEventId
from constants.utils import CustomFormatter, is_docker
from services import Alert, Rabbit

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()

LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
APP_VERSION = os.getenv('APP_VERSION') or 'undefined'
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


async def main() -> None:
    log.info(f'Starting ESS client version: {APP_VERSION}')
    log.info('Starting Services...')
    if RABBITMQ_ENABLED == 'True':
        if not rabbit.is_ready:
            await rabbit.setup(RABBITMQ_URL)
        
        log.info('RabbitMQ Service ready!')
    else:
        log.info('RabbitMQ Service disabled by user')

    if not alert.is_ready:
        # await alert.setup(REDIS_URL)
        await alert.setup(
            mongodb_url=MONGODB_URL,
            db='warpgate_dev',
            collection='alerts',
        )
    
    log.info('Alert Service ready!')

    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        log.info('Listening for Census Events...')
        
        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            unique_id = str(UniqueEventId(evt.world_id, evt.instance_id))

            log.info(
                f'Received {evt.event_name} id: {unique_id}'
            )

            log.debug(f"""
            ESS Data:
            Instance ID: {evt.instance_id}
            Event ID: {evt.metagame_event_id}
            State: {evt.metagame_event_state} ({evt.metagame_event_state_name})
            World: {evt.world_id} ({WORLD_NAMES[evt.world_id]})
            Zone: {evt.zone_id} ({ZONE_NAMES[evt.zone_id]})
            NC: {evt.faction_nc}
            TR: {evt.faction_tr}
            VS: {evt.faction_vs}
            XP Bonus: {evt.experience_bonus}
            Timestamp: {evt.timestamp}
            """)

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
            # Convert to string
            json_event = json.dumps(event_data)
            
            # Publish to RabbitMQ
            if RABBITMQ_ENABLED == 'True':
                await rabbit.publish(bytes(json_event, encoding='utf-8'))
                log.info(f'Event {unique_id} published')

            # Add or remove from database
            if evt.metagame_event_state_name == 'started':
                # await alert.create(
                #     world=WORLD_NAMES[evt.world_id], 
                #     instance_id=evt.instance_id, 
                #     event_json=json_event
                # )
                result = await alert.create(event_data)

                log.info(f'Created alert with id: {result}')
            if evt.metagame_event_state_name == 'ended' or 'cancelled':
                # await alert.remove(
                #     world=WORLD_NAMES[evt.world_id],
                #     instance_id=evt.instance_id,
                # )
                await alert.remove(id=unique_id)

                log.info(f'Removed alert {unique_id}')

    
    _ = on_metagame_event


loop = asyncio.new_event_loop()
loop.create_task(main())
try:
    loop.run_forever()
except asyncio.exceptions.CancelledError:
    loop.stop()
except KeyboardInterrupt:
    loop.stop()


if __name__ == '__main__':
    try:
        loop = asyncio.new_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
