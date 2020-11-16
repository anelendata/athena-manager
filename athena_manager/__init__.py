import argparse, datetime, json, logging, os, sys

from athena_manager import impl


logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    # level=logging.INFO,
                    level=logging.DEBUG
                    )
logger = logging.getLogger(__name__)


def _load_data_params(arg_list):
    data = {}
    for a in arg_list:
        pos = a.find("=")
        if pos < 0:
            raise ValueError("data argument list format error")
        key = a[0:pos].strip()
        value = a[pos + 1:].strip()
        if not key or not value:
            raise ValueError("data argument list format error")
        data[key] = value
    return data


def run(command, config, data):
    """
    """
    # Search in impl.py for available commands
    commands = dict()
    impl_obj = dir(impl)
    for name in impl_obj:
        if name[0] == "_":
            continue
        obj = getattr(impl, name)
        if callable(obj):
            commands[name] = obj

    if command not in commands:
        raise ValueError("Invalid command: %s\nAvailable commands are %s" %
                         (command, [x for x in commands.keys()]))

    logger.info("Running " + command)

    start = datetime.datetime.utcnow()
    logger.info("Job started at " + str(start))

    # Run the command
    ret = commands[command](config, **data)
    if ret:
        logger.debug(ret)

    end = datetime.datetime.utcnow()
    logger.info("Job ended at " + str(end))
    duration = end - start
    logger.info("Processed in " + str(duration))

    logger.debug(ret)


def main():
    parser = argparse.ArgumentParser(description="Run Athena queries.")
    parser.add_argument("command", type=str, help="command")
    parser.add_argument("-c", "--config", type=str, required=True,
                        help="config file")
    parser.add_argument("-d", "--data", type=str, nargs="*", default="",
                        help="Data required for the command a='x' b='y' ...")
    args = parser.parse_args()

    command = args.command
    with open(args.config, "r") as f:
        config = json.load(f)
    data = _load_data_params(args.data)

    # Overwrite the config when argv is present
    for key in data.keys():
        if config.get(key) is not None:
            config.pop(key)

    logger.info("Running " + command + " data:" + str(data))
    run(command, config, data)


if __name__ == "__main__":
    main()
