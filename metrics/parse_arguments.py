import argparse

def parse_arguments():
    """Extend the default parser with the local needs"""
    parser = argparse.ArgumentParser(prog=__name__)

    # Add the locally defined parser extensions
    parser.add_argument(
        "log_file_path",
        help="Path of the log file to parse."
    )

    return parser.parse_args()
