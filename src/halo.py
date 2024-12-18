import marimo

__generated_with = "0.10.2"
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


    seispy.hello()
    return mo, seispy


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Seismogram

        Show seismogram.
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
