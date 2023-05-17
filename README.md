# streamclient
Collects events from the Census Event Streaming Service.
</br>
Upon receiving a MetagameEvent, this publishes it to a RabbitMQ queue and saves it in a Mongo database.

## Cloning
Make sure to run `git submodule update --init --recursive` after cloning to install the submodule.
