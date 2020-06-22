# Code for streaming crime data from Halifax Regional Municipality's open data API

Only the previous 7 days of crime data are made available via [Halifax Open Data](http://catalogue-hrm.opendata.arcgis.com/).

When scheduled to run daily (or weekly at a minimum) this script connects to the Halifax Open Data API, and appends the new crime records to your dataset based on the event date. 

### Note: 

When running for the first time, you will need uncomment the line of code that saves the data to a csv to form your baseline dataset. 
