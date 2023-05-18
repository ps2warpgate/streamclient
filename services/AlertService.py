from motor import motor_asyncio


class Alert:
    """Handles database operations for MetagameEvents"""
    def __init__(self) -> None:
        self._client = None
        self._db = None
        self._alert_collection = None
        self.is_ready = False

    async def setup(self, mongodb_url: str, db: str, collection: str):
        """Creates a MongoDB client

        Args:
            - mongodb_url: `str` MongoDB connection URL
            - db: `str` Mongo database name
            - collection: `str` Mongo collection name
        """
        self._client = motor_asyncio.AsyncIOMotorClient(mongodb_url)
        self._db = self._client[db]
        self._alert_collection = self._db[collection]

        self.is_ready = True

    async def create(self, event_data: dict):
        """Creates a new MetagameEvent instance in the database

        Args:
            event_data: `dict` Dictionary containing event data

        Returns:
            inserted_id: `Any` ID of the created document
        """
        result = await self._alert_collection.insert_one(event_data)
        return result.inserted_id

    async def read_one(self, event_id: str) -> dict:
        """Reads one alert from the database

        Args:
            event_id: `str` event/document ID

        Returns:
            `dict` document
        """
        result = await self._alert_collection.find_one({"_id": event_id})
        return result

    async def read_many(self, length: int, query: dict = None) -> list[dict]:
        """Reads many alerts from the database

        Args:
            length: `int` number of documents to return
            query: `dict` (optional) a Mongo query

        Returns:
            `list[dict]` list of documents
        """
        cursor = self._alert_collection.find(query)
        found_documents = []
        for document in await cursor.to_list(length=length):
            found_documents.append(document)
        return found_documents

    async def count(self, query: dict = None) -> int:
        """Get the number of alerts currently in the database

        Args:
            query: `dict` (optional) a Mongo query

        Returns:
            `int` number of documents in alert collection
        """
        result = await self._alert_collection.count_documents(query)
        return result

    async def remove(self, event_id: str) -> int:
        """Remove a MetagameEvent instance from the database

        Args:
            event_id: `str` ID of the event being removed

        Returns:
            `int` number of documents deleted
        """
        result = await self._alert_collection.delete_one({"_id": event_id})
        return result.deleted_count

    async def remove_many(self, query: dict) -> int:
        """Remove many MetagameEvents from the database

        Args:
            query: `dict` a Mongo query

        Returns:
            `int` number of documents deleted
        """
        result = await self._alert_collection.delete_many(query)
        return result.deleted_count
