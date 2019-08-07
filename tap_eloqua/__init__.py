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
		REQUIRED_CONFIG_KEYS,
		EloquaClient,
		EloquaExecutor,
		STREAMS
	)


if __name__ == '__main__':
	main()