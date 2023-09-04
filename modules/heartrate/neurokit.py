import neurokit2 as nk
import numpy as np


def get_rpeaks(ecg_signal: list[float], sampling_rate: int = 250) -> np.ndarray[int]:
    """It returns R Peaks Indexes of the input ECG Signal"""
    ecg_signal = nk.signal_sanitize(ecg_signal)
    ecg_cleaned = nk.ecg_clean(ecg_signal, sampling_rate=sampling_rate)
    instant_peaks, rpeaks = nk.ecg_peaks(ecg_cleaned, sampling_rate=sampling_rate)
    return rpeaks["ECG_R_Peaks"]
