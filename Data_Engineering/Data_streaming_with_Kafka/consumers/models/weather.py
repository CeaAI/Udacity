"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        if "org.chicago.cta.weather.v1" in message.topic(): 
            try:
                value = json.loads(message.value())
                self.temperature = value['temperature']
                self.status = value['status'].name
            except Exception as e:
                logger.fatal("no weather info? %s, %s", value, e)
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic
            )
        logger.info("weather process_message is incomplete - skipping")
    
