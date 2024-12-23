import obspy
from icecream import ic

import seispy


def check_deconv_result(src_file, deconv_file):
    for ifile in [src_file, deconv_file]:
        ic(ifile)
        st = obspy.read(ifile)
        st.plot()


def check_deconv_prior(sac_file, src_file, resp_file):
    ic("source stream")
    st = obspy.read(src_file)
    st.plot()

    ic("deconvolution")
    ic("1. sac result")
    sac_st = obspy.read(sac_file)
    sac_st.plot()

    ic("2.1 obspy result without water_level")
    inv = obspy.read_inventory(resp_file)
    st_se = seispy.response.stream_removed_response(src_file, inv)
    st_se.plot()

    ic("2.2 obspy result with default water_level=60")
    for tr in st:
        tr.remove_response(
            inventory=inv,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
        )
        tr.data *= 1e9
    st.plot()
