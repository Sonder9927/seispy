from pathlib import Path

import obspy
from icecream import ic


def check_merge_prior(sacs_dir, specials):
    text_path = Path(sacs_dir)
    sacs = sorted(text_path.iterdir())
    ic("sacs:")
    show_isac_and_merged_sac(sacs[:3])
    ic("specials:")
    st_merged = show_isac_and_merged_sac([Path(i) for i in specials])
    # st0 = obspy.read(specials[0])
    # tr0 = st0[0]
    # tr0.data -= st_merged[0].data
    # ic("delta (special0 - merged):")
    # tr0.plot()


def show_isac_and_merged_sac(sacs: list[Path]):
    st_combined = obspy.Stream()

    for sac in sacs:
        ic(sac.name)
        st = obspy.read(sac)
        st.plot()
        st_combined += st

    # st_combined
    st_combined.sort()
    # need to merge to output one file
    ic("merged stream:")
    st_combined.merge(method=1, fill_value="interpolate")
    st_combined.plot()
    return st_combined


def check_merge_result(src_dir, dest_file):
    src_path = Path(src_dir)
    src_st = obspy.Stream()
    for zsac in src_path.glob("*BHZ*.SAC"):
        src_st += obspy.read(zsac)
    src_st.plot()
    dest_st = obspy.read(dest_file)
    dest_st.plot()
