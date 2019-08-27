from tap_kit import main_method
from .client import EloquaClient
from .executor import EloquaExecutor
from .contacts import ContactsStream
from .bounces import BouncesStream
from .clicks import ClicksStream
from .opens import OpensStream
from .sends import SendsStream
from .subscribes import SubscribesStream
from .unsubscribes import UnsubscribesStream

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "sitename",
    "username",
    "password",
    "contacts_export_fields",
    "activities_export_fields"
]

STREAMS = [
	ContactsStream,
    BouncesStream,
    ClicksStream,
    OpensStream,
    SendsStream,
    SubscribesStream,
    UnsubscribesStream,
]


def main():
    main_method(
        config_keys=REQUIRED_CONFIG_KEYS,
        tap=EloquaExecutor,
        client=EloquaClient,
        streams=STREAMS
	)


if __name__ == '__main__':
	main()