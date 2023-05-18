import multiprocessing as mp
from traceback import print_exc

from workers import ECGWatcher, HRCalculator


def main():
    ecg_queue = mp.Queue()
    watcher = ECGWatcher(ecg_queue)
    watcher.start()

    calculator = HRCalculator(ecg_queue)
    calculator.start()

    while True:
        pass


if __name__ == "__main__":
    main()
