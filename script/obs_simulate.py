from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from tqdm import tqdm

src_dir = Path("/path/to/nz_obs/")
dest_dir = Path("/path/to/nz_obs_deconv")


def process_per_channel(pattern, paz):
    total = 0
    for sac in src_dir.rglob(pattern):
        # st = _st_deconv(sac, inv)
        st = _st_simulated(sac, paz)
        target = dest_dir / sac.relative_to(src_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        target = target.with_suffix(".deconv.sac")
        st.write(str(target), format="SAC")
        total += 1
    return total


def _st_simulated(sac, paz):
    st = obspy.read(sac)
    st.merge(method=1, fill_value="interpolate")
    for tr in st:
        tr.detrend("demean")
        tr.detrend("linear")
        tr.taper(max_percentage=0.05, type="hann")
        tr.simulate(paz_remove=paz, pre_filt=[0.003, 0.006, 1, 2])
        # tr.data *= 1e9
    return st


def _st_deconv(sac, inv):
    st = obspy.read(sac)
    st.merge(method=1, fill_value="interpolate")
    for tr in st:
        tr.detrend("demean")
        tr.detrend("linear")
        tr.taper(max_percentage=0.05, type="hann")
        tr.remove_response(
            inventory=inv,
            water_level=None,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
            zero_mean=False,
            taper=False,
        )
        tr.date *= 1e9
    return st


def _pp_gen():
    # displacement
    paz_seis = {
        "gain": 4,
        "poles": [
            -3e-02 + 3-02j,
            -3e-02 - 3-02j,
            -3e02 + 0e00j,
            -3e02 + 4e02j,
            -3e02 - 4e02j,
            -8e02 + 1e03j,
            -8e02 - 1e03j,
            -4e03 + 4e03j,
            -4e03 - 4e03j,
            -6e03 + 0e00j,
            -1e04 + 0e00j,
        ],
        "zeros": [
            0.000000e00 + 0.000000e00j,
            0.000000e00 + 0.000000e00j,
            0.000000e00 + 0.000000e00j,
            -3e02 + 0e00j,
            -1e03 + 0e00j,
            -1e03 + 1e03j,
            -1e03 - 1e03j,
        ],
        "sensitivity": 2e08,
    }

    # Instrument response with transfer function = 1
    # for BHH
    paz_pres = {
        "gain": 1,
        "poles": [1 + 0j],
        "sensitivity": 1,
        "zeros": [1 + 0j],
    }
    patterns = ["*BHZ*.sac", "*BHN*.sac", "*BHE*.sac", "*BHH*.sac"]
    for pattern, paz in zip(patterns, [paz_seis, paz_seis, paz_seis, paz_pres]):
        yield pattern, paz


def deconv():
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(process_per_channel, pattern, paz)
            for pattern, paz in _pp_gen()
        }
        with tqdm(desc="Deconvolutioning ...") as pbar:
            total = 0
            for future in as_completed(futures):
                batch_total = future.result()
                total += batch_total
        print(f"All {total} sac files deconvolutioned.")


if __name__ == "__main__":
    deconv()
