import numpy as np

seconds_in_one_hour = np.float64(60.0 * 60.0)

# we ignore the rare case of leap seconds
seconds_in_one_day = seconds_in_one_hour * 24.0

seconds_in_one_week = seconds_in_one_day * 7.0
