from tap_kit import main_method
from .client import EloquaClient
from .executor import EloquaExecutor


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "sitename",
    "username",
    "password"
]

def main():
    main_method(
        config_keys=REQUIRED_CONFIG_KEYS,
        tap=EloquaExecutor,
        client=EloquaClient,
        streams=None
	)


if __name__ == '__main__':
	main()