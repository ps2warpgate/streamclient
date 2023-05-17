from motor import motor_asyncio
from constants.typings import UniqueEventId


class Alert():
    """Handles database operations for MetagameEvents"""
    def __init__(self) -> None:
        self.is_ready = False

    async def setup(self, mongodb_url: str, db: str, collection: str):
        """Creates a MongoDB client

        Args:
            - mongodb_url: `str` MongoDB connection URL
            - db: `str` Mongo database name
            - collection: `str` Mongo collection name
        """
        self.client = motor_asyncio.AsyncIOMotorClient(mongodb_url)
        self.db = self.client[db]
        self.alert_collection = self.db[collection]

        self.is_ready = True

    async def create(self, event_data: dict):
        """Creates a new MetagameEvent instance in the database

        Args:
            event_data: `dict` Dictionary containing event data

        Returns:
            inserted_id: `Any` ID of the created document
        """
        result = await self.alert_collection.insert_one(event_data)
        return result.inserted_id

    async def read_one(self, id: UniqueEventId):
        """Reads one alert from the database

        Args:
            id: `UniqueEventId` event/document ID

        Returns:
            A single document
        """
        result = await self.alert_collection.find_one({"_id": id})
        return result

    async def read_many(self):
        # TODO: Get all alerts or alerts matching a pattern
        raise NotImplementedError

    async def count(self) -> int:
        """Get the number of alerts currently in the database

        Returns:
            result: `int` number of documents in alert collection
        """
        result = await self.alert_collection.count_documents({})
        return result

    async def count_world(self):
        # TODO: Get number of alerts for a given world
        raise NotImplementedError

    async def remove(self, id: UniqueEventId):
        """Remove a MetagameEvent instance from the database

        Args:
            id: `UniqueEventId` ID of the event being removed

        Returns:
            deleted_count: `int` number of documents deleted
        """
        result = await self.alert_collection.delete_one({"_id": id})
        return result.deleted_count
