# SATDA-VegStress
Scripts for developing parsimonious Surface-Air Temperature Difference Anomaly (SATDA) method for tracking vegetation drought stress using sub-diurnal geostationary satellites and meteorological grids.

Surface-Air Temperature Difference Anomaly (SATDA) is a metric for tracking vegetation drought stress using the cumulative sub-diurnal difference between land surface temperature (Ts) from the Himawari-8 geostationary satellite and hourly air temperature (Ta) from meteorological grids.
The major codes for the workflow to compute SATDA are included in the GitHub repository and are available at the Zenodo repository: https://doi.org/10.5281/zenodo.15273488.

As SATDA is parsimonious with small data inputs and computationally efficient formulations, this workflow has the potential to be extended to other regions covered by Himawari satellite’s observation area (e.g., East and Southeast Asia) or to other new-generation geostationary satellites (e.g., GOES-R series over the Americas, Meteosat Second Generation series over Europe and Africa).   

For full details about the background, methodology and results for SATDA in the early detection of vegetation drought stress, check the publication at the Remote Sensing of Environment:

Cai, D., McVicar, T. R., Van Niel, T. G., Donohue, R. J., Yamamoto, Y., Stewart, S. B., Ichii, K. & Stenson, M. P. 2025. Using sub-diurnal surface-air temperature difference anomaly derived from Himawari-8 geostationary satellite and meteorological grids for early detection of vegetation drought stress: Application to Australia's 2017–2019 Tinderbox Drought. Remote Sensing of Environment, 327, 114768. https://doi.org/10.1016/j.rse.2025.114768