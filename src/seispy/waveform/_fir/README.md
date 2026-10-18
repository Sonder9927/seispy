# Bundled SAC decimation filters

These unmodified FIR coefficient files (`dec2` through `dec7`) are from
SAC Community Edition, distributed under Apache-2.0. See the accompanying
`LICENSE` for the full license. They are third-party data; the project's MIT
license does not replace their Apache-2.0 license.

Upstream: https://github.com/EarthScope/sac-community
Pinned commit: `796fd8f06909f8515c1b1f6c2b582ac8ee22fdc6`
Source directory: https://github.com/EarthScope/sac-community/tree/796fd8f06909f8515c1b1f6c2b582ac8ee22fdc6/aux/fir
Upstream license: https://github.com/EarthScope/sac-community/blob/796fd8f06909f8515c1b1f6c2b582ac8ee22fdc6/License

No upstream NOTICE file was present at this commit. The coefficient files
contain no separate copyright notices. Their contents are unchanged.

The first coefficient is the center tap, followed by one half of an odd-length
symmetric FIR filter. SeisPy mirrors that half to obtain the full filter and
uses it directly with SciPy polyphase resampling. Coefficients are not
redesigned or normalized. Pinning them makes results independent of a local
SAC installation, environment variables, and future upstream changes.

Updating these files requires checking the license, updating the pinned commit
and checksum tests, and rerunning the SAC numerical integration comparisons.
