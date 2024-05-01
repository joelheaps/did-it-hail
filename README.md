# ⛈️ Did It Hail?

This Python application is designed to process and animate hail data based on the NEXRAD Level 3 hydrometeor classification product. It uses a mix of things like MetPy, Matplot, Xarray, etc., to manipulate data available from NOAA's near-realtime radar data feeds.

Example total hail map generated from several hours of radar data around Omaha, NE, US:

![example hail sum](assets/example_sum.png)

## What is the "hail index"?

The NEXRAD hydrometeor classification product specifies 3 levels of hail:

- Hail (<1 in.)
- Large Hail (1-2 in.)
- Giant Hail (≥ 2in)

(For more info, see [Hydrometeor Classification Algorithm 2 (HCA2) Overview](https://www.nssl.noaa.gov/about/events/review2015/science/files/Schuur_NSSLReview2015.pdf) and [RDA/RPG Build 17.0 Training](https://training.weather.gov/wdtd/buildTraining/build17/documents/build17-deploy.pdf).)

This application simply 1) uses a values of 1, 2, or 3 to represent the corresponding hail severity in each radar scan, and 2) adds the new value when subsequent scans indicate hail at the same location.  The index is not very determinstic in that, for example, an index of 9 could indicate 9 scans of small hail, or 3 scans of giant hail, but it is a useful comparison of relative hail activity in an area.

A future version may map these three categories separately to show a more granular report.

### Limitations

Hail is only sampled at given radar scan intervals (about 5 minutes).  Without some sort of frame interpolation, unfortunately, gaps or stripes can be observed in depictions of small or fast-moving storms.  The patterns are usually obvious at least, and you can infer that similar hail fell between the stripes.

# Animations

### Radar Hail Depiction
<video src="https://github.com/joelheaps/did-it-hail/assets/13434824/0f6cf4ec-f3e4-4265-acfc-41f8f6bed3b7" width="320" height="240" controls></video>

### Running Hail Sum
<video src="https://github.com/joelheaps/did-it-hail/assets/13434824/cdf7c7d4-826c-4640-8e45-5cd1da14bb8a" width="320" height="240" controls></video>


