import obspy
from .response import delta_trace_plot
from icecream import ic


def check_resample(src_file, sac_file):
    ic("comprasion between resample results:")
    ic("1. sac resample result")
    sac_st = obspy.read(sac_file)
    delta = sac_st[0].stats.sampling_rate
    ic(delta)
    sac_st.plot()

    ic("2. obspy resample result")
    st = obspy.read(src_file)
    delta1 = st[0].stats.sampling_rate
    st.resample(1.0)
    delta2 = st[0].stats.sampling_rate
    ic(delta1, delta2)
    st.plot()

    ic("3. delta trace plot")
    delta_trace_plot(sac_st, st)


if __name__ == "__main__":
    check_resample(
        "data/response_contract/NZ37.BHZ.2024.001.rmpz.sac",
        "data/response_contract/NZ37.BHZ.2024.001.1Hz.sac",
    )
