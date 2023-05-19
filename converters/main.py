import logging
import multiprocessing as mp
# from queue import Queue

from workers import ECGWatcher, HeartRateCalculator
from datamodel import HeartRate, ECG, ECGBulk

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def main():
    ecg_queue: mp.Queue[list[ECG]] = mp.Queue()
    hr_queue: mp.Queue[HeartRate] = mp.Queue()

    watcher = ECGWatcher(ecg_queue)
    watcher.start()

    calculator = HeartRateCalculator(incoming_queue=ecg_queue, outgoing_queue=hr_queue)
    calculator.start()

    while True:
        pass
        # manager.start()
        # server = manager.get_server()
        # server.serve_forever()


if __name__ == "__main__":
    main()


