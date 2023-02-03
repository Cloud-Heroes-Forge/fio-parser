import configparser


def get_file(_filename):
    """
    Safely read a file.
    Taken from https://fossies.org/linux/fio/t/run-fio-tests.py
    """
    try:
        with open(_filename, "r") as output_file:
            file_data = output_file.read()
        return file_data
    except OSError:
        return False


def parse_fio_config(_filename) -> object or bool:
    conf = configparser.ConfigParser()
    try:
        conf.read(_filename)
        return dict(conf.items('global', raw=True))     # return all of the items in the global section as a dict
    except configparser.Erorr:
        raise configparser.ParsingError(f"Unable to parse {_filename}")
