from aioprometheus import Counter, Gauge
from aioprometheus.service import Service
from prometheus_client import Enum


class Metrics:
    def __init__(self) -> None:
        self._service = Service()
        self.events_counter = Counter(
            name = 'events', 
            doc = 'total number of census events',
        )
        self.in_progress_alerts = Gauge(
            name = 'in-progress alerts',
            doc = 'number of alerts currently in the database',
        )
        self.alert_service_state = Enum(
            name = 'alert service state',
            documentation = 'state of the alert service',
            states = ['starting', 'running', 'stopped']
        )
        self.rabbit_service_state = Enum(
            name = 'rabbitmq service state',
            documentation = 'state of the RabbitMQ service',
            states = ['starting', 'running', 'stopped']
        )


