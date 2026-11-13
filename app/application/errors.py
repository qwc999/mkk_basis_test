class StorageError(Exception):
    pass


class PublicationError(Exception):
    pass


class PublicationNotConfirmed(PublicationError):
    pass


class WebhookDeliveryError(Exception):
    pass
