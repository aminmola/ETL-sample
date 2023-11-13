import os

import etl.extract as extract
import etl.load as load
from etl.transform import Transform
from utils.logger import Logger

log = Logger("ServiceName")
os.environ['TZ'] = 'UTC'


def run(data):
    """
    Extract, transform and load Data
    """
    if data:
        transform = Transform()
        parsed_data = transform.run(data)

        load.run(data=parsed_data)

    else:
        log.error(f"Extract data failed")


if __name__ == '__main__':
    data = extract.run()
    run(data=data)
