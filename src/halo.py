import marimo

__generated_with = "0.10.6"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        # SeisPy
        SeisPy: seismic data processing with ObsPy.
        data: Seismographic staions deployed on the North island of New Zealand.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import seispy
    from pathlib import Path
    from icecream import ic

    seispy.hello()
    return Path, ic, mo, seispy


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Sort Data
        ### Sort Directory
        以新西兰数据为例。使用 `seispy.sort_to` 函数，复制并整理原始数据文件夹 `data/sac_src` 到 `data/dest`。
        本示例目标文件名如 `NZ.NZ01..BHE.D.2023.308.230000.SAC`，故函数参数的搜索匹配模式为 `pattern='*.SAC'`。
        通过 "." 拆分文件名内容，若文件名有异，需要修改 `src/seispy/collate.py` 文件的 `_copy_targets` 函数。
        ### Merge Data

        利用 `seispy.merge_by_day` 按照 `channel` 合并每一天的数据，输出到当前文件夹，并**删除原文件**。

        为了验证 `merge` 的效果，进行测试。

        1. 首先查看 `data/sac_unprocess/NZ/NZ37/2024/001` 文件夹中 `BHE` 分量前三个小时的地震图。
        2. 将这三个文件合并，再查看。

        若符合预期，可以利用 `merge_by_day`，对每一天数据进行合并(merge)，合并后将**删除原文件**，若不想删除则将参数 `remove_src` 参数设置为 `False`。

        **Bug:**

        后缀命名无效：如果已经存在 "example.SAC"，保存新的 "example.sac"，会保存成 "example.SAC" 并覆盖原文件。

        原因：某些文件系统（如 Windows 的 NTFS）对文件名的大小写不敏感。
        """
    )
    return


@app.cell
def _(Path, ic, seispy):
    import obspy


    def check_merge_prior():
        text_path = Path("data/sac_unprocess/NZ/NZ37/2024/001")
        text_sac = Path("text.sac")
        sacs = sorted(text_path.iterdir())
        
        st_combined = obspy.Stream()
        
        for sac in sacs[:3]:
            st = obspy.read(sac)
            ic(sac.name)
            st.plot()
            st_combined += st
        
        # st_combined
        st_combined.sort()
        ic("combine")
        st_combined.plot()
        # need to merge to output one file
        ic("combine-merge-write-check")
        st_combined.merge(method=1, fill_value="interpolate")
        st_combined.write(str(text_sac), format="SAC")
        st_text = obspy.read(text_sac)
        st_text.plot()
        text_sac.unlink()

    seispy.sort_to("data/sac_src", "data/sac_unprocess", "*.SAC")
    check_merge_prior()
    seispy.merge_by_day("data/sac_unprocess/")
    return check_merge_prior, obspy


@app.cell
def _(mo):
    mo.md(r"""检查 `data/sac_src/NZ37/2024/01/01/` 下的 `BHZ` 分量多文件，与 `data/sac_unpress/NZ/NZ37/2024/001/` 下的 `BHZ` 分量单个文件。""")
    return


@app.cell
def _(Path, obspy):
    def check_merge_result():
        src_path = Path("data/sac_src/NZ37/2024/01/01/")
        dest_sac = Path("data/sac_unprocess/NZ/NZ37/2024/001/NZ.NZ37..BHZ.D.2024.001.merged.sac")
        src_st = obspy.Stream()
        for zsac in src_path.glob("*BHZ*.SAC"):
            src_st += obspy.read(zsac)
        src_st.plot()
        dest_st = obspy.read(dest_sac)
        dest_st.plot()
    check_merge_result()
    return (check_merge_result,)


if __name__ == "__main__":
    app.run()
