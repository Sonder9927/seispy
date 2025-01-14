import marimo

__generated_with = "0.10.12"
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


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import seispy
    import halo

    halo.hello()
    return halo, mo, seispy


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Sort Data
        ### Sort to New Directory
        以新西兰数据为例。使用 `seispy.sort_to` 函数，复制并整理原始数据文件夹 `data/sac_src` 到 `data/sac_dest`。
        本示例目标文件名如 `NZ.NZ01..BHE.D.2023.308.230000.SAC`，故函数参数的搜索匹配模式为 `pattern='*.SAC'`。
        通过 "." 拆分文件名内容，若文件名有异，需要修改 `src/seispy/collate.py` 文件的 `_copy_targets` 函数。
        """
    )
    return


@app.cell(hide_code=True)
def _(seispy):
    # sort to a new directory
    seispy.sort_to("data/sac_src", "data/sac_dest", "*.SAC")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ### Merge Data by Days

        利用 `seispy.merge_by_day` 按照 `channel` 合并每一天的数据，输出到当前文件夹，并**删除原文件**。

        为了验证 `merge` 的效果，进行测试。

        1. 首先查看 `data/sac_dest/NZ/NZ37/2024/001` 文件夹中 `BHE` 分量前三个小时的地震图。
        2. 将这三个文件合并，再查看。
        """
    )
    return


@app.cell(hide_code=True)
def _(halo):
    # check traces before `merge`
    halo.check_merge_prior(
        "data/sac_dest/NZ/NZ37/2024/001",
        specials=[
            "data/sac_src/NZ01/NZ.NZ01..BHE.D.2023.308.204358.SAC",
            "data/sac_src/NZ01/NZ.NZ01..BHE.D.2023.308.204358-1.SAC",
        ],
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        3. 若符合预期，可以利用 `merge_by_day`，对每一天数据进行合并(merge)，合并后将**删除原文件**，若不想删除则将参数 `remove_src` 参数设置为 `False`。

            **Bug:**

            后缀命名无效：如果已经存在 "example.SAC"，保存新的 "example.sac"，会保存成 "example.SAC" 并覆盖原文件。

            原因：某些文件系统（如 Windows 的 NTFS）对文件名的大小写不敏感。
        """
    )
    return


@app.cell(hide_code=True)
def _(seispy):
    # run merging
    seispy.merge_by_day("data/sac_dest/")
    return


@app.cell
def _(mo):
    mo.md(r"""4. 检查 `data/sac_src/NZ37/2024/01/01/` 下的 `BHZ` 分量多个文件，与 `data/sac_dest/NZ/NZ37/2024/001/` 下的 `BHZ` 分量单个文件。""")
    return


@app.cell(hide_code=True)
def _(halo):
    # check merged result
    halo.check_merge_result(
        src_dir="data/sac_src/NZ37/2024/01/01/", 
        dest_file="data/sac_dest/NZ/NZ37/2024/001/NZ.NZ37..BHZ.D.2024.001.merged.sac",
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Response Files
        ### Download Response
        利用 `seispy.response.download_response` 下载。

        参考 [GeoNet FDSN webservice with Obspy - Station Service](https://github.com/GeoNet/data-tutorials/blob/main/FDSN/FDSN_station.ipynb)

        ### Combine Response
        利用 `seispy.response.combine_response` 合并 response 文件。

        ## Data Process
        ### Preprocess & deconvolution
        #### Comparison between SAC and ObsPy

        以 "NZ37.BHZ.2024.001.sac" 为例，sac 预处理得到 “NZ37.BHZ.2024.001.{r,rt,rmt,rmpz}.sac”等文件，sac 操作如下：

        ```sac
        r NZ37.BHZ.2024.001.sac
        rmean
        w NZ37.BHZ.2024.001.r.sac
        rtr
        w NZ37.BHZ.2024.001.rt.sac
        taper
        w NZ37.BHZ.2024.001.rmt.sac
        trans from polezero subtype ./NZ37.pz to none freq 0.003 0.006 1 2
        mul 1.0e9
        w NZ37.BHZ.2024.001.rmpz.sac
        q
        ```

        与 obspy 对比，obspy 每一步操作都是基于上一步 sac 的结果，因此差异只是当前步骤的差异，不是累积的。
        """
    )
    return


@app.cell(hide_code=True)
def _(halo):
    halo.diff_rmt_deconv("data/diff_data", per=True)
    return


@app.cell
def _(mo):
    mo.md(r"""而累积的误差结果是：""")
    return


@app.cell(hide_code=True)
def _(halo):
    halo.diff_rmt_deconv("data/diff_data", per=False)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        #### conclusion

        一些参数设置：

        1. 预处理差异

            obspy 默认的 `remove_response` 会去均值，做余弦窗 taper，与 sac 略有不同。目前最好结果是额外先去均值、去线性趋势、做 hann 窗的 taper。

        2. 去仪器响应的 `water_level` 参数

            obspy 的 `remove_response` 参数 `water_level` 会影响仪器响应去除过程中的频率响应，特别是在处理极点和零点时。这个参数在 obspy 中用于避免数值不稳定问题，尤其是在处理极点时，防止极点接近零时导致的数值爆炸。设置一个适当的 `water_level` 值可以确保去响应操作的稳定性。然而，当 `water_level` 设置为 `None` 时，obspy 会采用默认行为，去掉水位平衡的影响，这样就避免了与 SAC 可能产生的差异，得到一致的结果。


        因此，obspy 的操作如下，**得到上述累积误差**：

        ```python
        st.detrend("demean")
        st.detrend("linear")
        st.taper(max_percentage=0.05, type="hann")
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
        ```

        **结论：**

        `rmean; rtr; taper` 每一步都有所差异，反而去仪器响应只有数值精度的差异。总的来看误差主要由去线性趋势这一步造成。误差相比原数据很小，应该是可以接受的。

        不过我们也提供了调用 sac 的方法，通过修改参数 `method=“sac”`。由于 sac 需要另外安装，所以默认方法是 obspy。

        ### resample
        #### Comparison between SAC and ObsPy

        原始数据是 100Hz，目标降采样到 1Hz，以上述文件 `NZ37.BHZ.2024.001.rmpz.sac` 为例，sac 降采样得到 `NZ37.BHZ.2024.001.1Hz.sac`。sac 命令为：

        ```sac
        r NZ37.BHZ.2024.001.rmpz.sac
        decimate 5; decimate 5; decimate 4
        w NZ37.BHZ.2024.001.1Hz.sac
        q
        ```
        与 obspy 的 `resample` 处理对比：
        """
    )
    return


@app.cell(hide_code=True)
def _(halo):
    halo.check_resample(
        "data/diff_data/NZ37.BHZ.2024.001.rmpz.sac", 
        "data/diff_data/NZ37.BHZ.2024.001.1Hz.sac",
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ### Remove Response by Days

        除了函数 `resample_by_day` 还可以直接利用在 `deconvolution_by_day` 去仪器响应时设置参数 `resample`。

        完成后将**删除原文件**，若不想删除则将参数 `remove_src` 参数设置为 `False`。也可以提前复制保存一份数据。
        """
    )
    return


@app.cell(hide_code=True)
def _(seispy):
    # run deconvolution
    seispy.response.deconvolution_last_subdirs(
        "data/sac_dest", "data/diff_data/response/NZ_example.xml"
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        检查结果：

        1. 前文所述 sac 去仪器响应的文件 `data/diff_data/NZ37.BHZ.2024.001.rmpz.sac`
        2. 对应 obspy 去仪器响应结果`data/sac_dest/NZ/NZ37/2024/001/NZ.NZ37..BHZ.D.2024.001.merged.deconv.sac`

        二者都没做降采样。
        """
    )
    return


@app.cell(hide_code=True)
def _(halo):
    # check deconvolution result
    halo.check_deconv_result(
        "data/diff_data/NZ37.BHZ.2024.001.rmpz.sac", 
        "data/sac_dest/NZ/NZ37/2024/001/NZ.NZ37..BHZ.D.2024.001.merged.deconv.sac"
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Conclusion

        执行以下操作：

        ```python
        # sort to a new directory
        seispy.sort_to("data/sac_src", "data/sac_dest", "*.SAC")
        # run merging
        seispy.merge_by_day("data/sac_dest/")
        # run deconvolution
        seispy.response.deconvolution_by_day(
            "data/sac_dest", resp="data/response_contract/NZ_example.xml", resample=1.0
        )
        ```

        - **使用 sac 需要设置 `method`，默认是 `obspy`。**
        - **降采样需要指定采样率 `resample`，默认是 `None`。**
        - **不删除原文件需设置 `remove_src`，默认是 `False`。**
        """
    )
    return


if __name__ == "__main__":
    app.run()
