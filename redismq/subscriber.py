class Subscriber(object):
    """
    Easy wrapper around subscribing
    """
    def __init__(self, client, channel, workerCallback, die_after_message=False):
        """
        default constructor
        """
        self._client = client
        self._channel = channel
        self._workerCallback = workerCallback
        self._die_after_message = die_after_message
    
    def exit(self):
        """
        quits subscribing
        """
        pass
        pass