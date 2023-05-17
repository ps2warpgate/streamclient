# streamclient
Collects events from the Census Event Streaming Service.  
Upon receiving a MetagameEvent, this publishes it to a RabbitMQ queue and saves it in a Mongo database.

### Development
After cloning the repository, make sure to install the submodule with `git submodule update --init --recursive`  
</br>
This project uses [Poetry](https://python-poetry.org/docs/) for package management. Use `poetry install` to install dependencies. The `requirements.txt` does not contain dev-dependencies and is used only for deployment.
