import argparse

import libs.log as log
from http_ops import Initialize
from utils.test_desc import AddTestInfo
from utils.utils import get_calamari_config


class Test(Initialize):
    def __init__(self, **config):

        super(Test, self).__init__(**config)

        self.url = self.http_request.base_url + "info"


def exec_test(config_data):

    add_test_info = AddTestInfo(8, "api/v2/info")
    add_test_info.started_info()

    try:
        test = Test(**config_data)

        test.get(test.url)

        add_test_info.success("test ok")

    except AssertionError, e:
        log.error(e)
        add_test_info.failed("test error")

    return add_test_info.completed_info(config_data["log_copy_location"])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Calamari API Automation")

    parser.add_argument(
        "-c",
        dest="config",
        default="config.yaml",
        help="calamari config file: yaml file",
    )

    args = parser.parse_args()

    calamari_config = get_calamari_config(args.config)

    exec_test(calamari_config)
