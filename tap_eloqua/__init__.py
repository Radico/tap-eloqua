from tap_kit import main_method
from .client import EloquaClient
from .executor import EloquaExecutor
from .contacts import ContactsStream


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "sitename",
    "username",
    "password",
    "export_fields"
]

STREAMS = [
	ContactsStream,
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