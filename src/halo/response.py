from pathlib import Path

import obspy
from icecream import ic


def diff_rmt_deconv(dir, per=True):
    dir = Path(dir)
    # rmt
    st = obspy.read(dir / "NZ37.BHZ.2024.001.sac")
    st.plot()

    # 1. check `rmean`
    st.detrend("demean")
    sac_st = obspy.read(dir / "NZ37.BHZ.2024.001.r.sac")
    ic("rmean")
    delta_trace_plot(sac_st, st)

    # 2. check `rtr`
    if per:
        st = sac_st.copy()
    st.detrend("linear")
    sac_st = obspy.read(dir / "NZ37.BHZ.2024.001.rt.sac")
    ic("rtr")
    delta_trace_plot(sac_st, st)

    # 3. check `taper`
    if per:
        st = sac_st.copy()
    st.taper(type="hann", max_percentage=0.05)
    sac_st = obspy.read(dir / "NZ37.BHZ.2024.001.rmt.sac")
    ic("taper")
    delta_trace_plot(sac_st, st)

    # check `remove response`
    inv = obspy.read_inventory(dir / "response/NZ37.xml")
    if per:
        st = sac_st.copy()
    st = _rm_resp(st, inv)
    sac_st = obspy.read(dir / "NZ37.BHZ.2024.001.rmpz.sac")
    ic("rm resposne")
    delta_trace_plot(sac_st, st)


def check_deconv_result(file1, file2):
    ic(file1)
    st1 = obspy.read(file1)
    st1.plot()
    ic(file2)
    st2 = obspy.read(file2)
    st1.plot()
    ic("delta")
    delta_trace_plot(st1, st2)


def check_rmt_prior(src_file, rmt_file):
    ic("source stream")
    st = obspy.read(src_file)
    st.plot()

    ic("1. rmt result")
    ic("1.1 sac result")
    sac_st = obspy.read(rmt_file)
    sac_st.plot()
    ic("1.2 obspy result")
    st = _hard_rmt(st)
    st.plot()

    ic("2. delta of result (sac - obspy)")
    delta_trace_plot(sac_st, st)


def check_deconv_prior(sac_file, src_file, resp_file):
    ic("source stream")
    st = obspy.read(src_file)
    st.plot()

    ic("deconvolution")
    ic("1. sac result")
    sac_st = obspy.read(sac_file)
    sac_st.plot()

    ic("2.1 obspy default result without water_level")
    inv = obspy.read_inventory(resp_file)
    st_se = st.copy()
    for tr in st_se:
        tr.remove_response(
            inventory=inv,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
            water_level=None,
        )
        tr.data *= 1e9
    st_se.plot()

    ic("2.2 obspy default result with water_level=60")
    st_wl = st.copy()
    for tr in st_wl:
        tr.remove_response(
            inventory=inv,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
        )
        tr.data *= 1e9
    st_wl.plot()

    ic("3. different taper method")
    ic("3.1.1 obspy default result (water_level=None, taper=cosine)")
    st_se.plot()
    ic("3.1.2 delta")
    delta_trace_plot(sac_st, st_se)
    _check_deconv_hard_delta(sac_st, _hard_rmt(st.copy()), inv)


def _check_deconv_hard_delta(sac_st, st, inv):
    st = _rm_resp(st, inv)
    ic("3.2 obspy hard result (water_level=None, taper=hann)")
    st.plot()
    ic("3.2.2 delta")
    delta_trace_plot(sac_st, st)


def _hard_rmt(st):
    st.detrend("demean")
    st.detrend("linear")
    st.taper(max_percentage=0.05, type="hann")
    return st


def delta_trace_plot(st1, st2):
    delta_tr = st1[0].copy()
    tr2 = st2[0]
    delta_tr.data -= tr2.data
    delta_tr.plot()


def _rm_resp(st, inv):
    for tr in st:
        tr.remove_response(
            inventory=inv,
            water_level=None,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
            zero_mean=False,
            taper=False,
        )
        tr.data *= 1e9
    return st
