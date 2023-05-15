import asyncio
import logging
import os
from datetime import datetime

from constants.utils import CustomFormatter
from services import Alert2
from constants.typings import UniqueEventId

MONGODB_URL = os.getenv('MONGODB_URL') or None


log = logging.getLogger('test')
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


alert = Alert2()


async def create_and_remove_alert():
    if not alert.is_ready:
        await alert.setup(
            mongodb_url=MONGODB_URL,
            db='warpgate_dev',
            collection='alerts',
        )

    log.info('Ready!')

    id = str(UniqueEventId(17, 123456))

    event_data = {
        '_id': id,
        'event_id': 123,
        'state': 'started',
        'world_id': 17,
        'zone_id': 1,
        'nc': 40,
        'tr': 30,
        'vs': 20,
        'xp': 25,
        'timestamp': datetime.utcnow().timestamp()
    }

    # json_event = json.dumps(event_data)

    c_result = await alert.create(event_data)
    log.info(f'Created alert {c_result}')

    count = await alert.count()
    log.info(f'Alerts in database: {count}')

    await asyncio.sleep(20)

    r_result = await alert.remove(id)
    log.info(f'Removed {r_result} alert(s)')

    count = await alert.count()
    log.info(f'Alerts in database: {count}')


if __name__ == '__main__':
    asyncio.run(create_and_remove_alert())


