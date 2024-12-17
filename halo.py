import marimo

__generated_with = "0.10.2"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""# SeisPy""")
    return


@app.cell
def _(mo):
    mo.md("""SeisPy: seismic data processing with ObsPy.""")
    return


@app.cell
def _():
    import marimo as mo
    import seispy

    seispy.hello()
    return mo, seispy


if __name__ == "__main__":
    app.run()
