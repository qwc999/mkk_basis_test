class PaymentNotFound(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


class InvalidPaymentState(Exception):
    pass
