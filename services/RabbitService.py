from aio_pika import DeliveryMode, ExchangeType, Message, connect

class Rabbit:
    """Handles connecting and publishing events to RabbitMQ"""
    def __init__(self) -> None:
        self.is_ready = False

    async def setup(self, rabbitmq_url: str):
        """Creates a RabbitMQ connection

        Args:
            rabbitmq_url: :class:`str` RabbitMQ broker connection URL
        """
        self.rmq_conn = await connect(
            url=rabbitmq_url,
        )
        self.channel = await self.rmq_conn.channel()
        self.exchange = await self.channel.declare_exchange(
            name='events',
            type=ExchangeType.DIRECT,
        )
        self.is_ready = True
    
    async def publish(self, message: bytes):
        """Publishes a message to the exchange

        Args:
            message: :class:`bytes` message in bytes
        """
        rmq_message = Message(
            body=message,
            content_encoding='application/json',
            delivery_mode=DeliveryMode.PERSISTENT,
        )
        await self.exchange.publish(message=rmq_message, routing_key='metagame')
