import logging
from hera_utils import parser

def parse_arguments(logger=logging.getLogger(__name__)):
    logger = logging.getLogger(__name__)
    experience_parser = parser(logger=logger)

    # Add the k8s parser extensions
    experience_parser.parser.add(
        "--k8s_config_file",
        help="Path to the k8s config file",
        type=str,
    )

    # Add the locally defined parser extensions
    experience_parser.parser.add(
        "--k8s_dataset_volume_claim_name",
        help="Name of the k8s volume claim to be used by numerical experiment to store the dataset.",
        type=str,
    )

    return experience_parser.parser.parse_args()
