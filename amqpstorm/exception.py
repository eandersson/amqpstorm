"""AMQP-Storm Exception."""
__author__ = 'eandersson'


class AMQPError(IOError):
    """General AMQP Error."""
    pass


class AMQPConnectionError(AMQPError):
    """AMQP Connection Error."""
    pass


class AMQPChannelError(AMQPError):
    """AMQP Channel Error."""
    pass


class AMQPMessageError(AMQPChannelError):
    """AMQP Message Error."""
    pass

class AMQPInvalidArgument(AMQPError):
    """AMQP Argument Error."""
