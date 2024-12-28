from pathlib import Path

import obspy
from icecream import ic


def check_merge_prior(sacs_dir):
    text_path = Path(sacs_dir)
    sacs = sorted(text_path.iterdir())

    st_combined = obspy.Stream()

    for sac in sacs[:3]:
        st = obspy.read(sac)
        ic(sac.name)
        st.plot()
        st_combined += st

    # st_combined
    st_combined.sort()
    ic("combined stream:")
    st_combined.plot()
    # need to merge to output one file
    ic("merged stream:")
    st_combined.merge(method=1, fill_value="interpolate")
    st_combined.plot()


def check_merge_result(src_dir, dest_file):
    src_path = Path(src_dir)
    src_st = obspy.Stream()
    for zsac in src_path.glob("*BHZ*.SAC"):
        src_st += obspy.read(zsac)
    src_st.plot()
    dest_st = obspy.read(dest_file)
    dest_st.plot()
