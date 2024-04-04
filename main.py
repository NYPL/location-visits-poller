import os

from lib.pipeline_controller import PipelineController
from nypl_py_utils.functions.config_helper import load_env_file


def main():
    load_env_file(os.environ["ENVIRONMENT"], "config/{}.yaml")
    controller = PipelineController()
    controller.run()


if __name__ == "__main__":
    main()
