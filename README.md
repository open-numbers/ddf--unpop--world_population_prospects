# UN World Population Prospects

This dataset contains all indicators from [UN World Population Prospects][1].

Check [the concepts file][2] for a list of all indicators.

[1]: https://population.un.org/wpp/
[2]: https://github.com/open-numbers/ddf--unpop--world_population_prospects/blob/master/ddf--concepts.csv

## Data sources summary

We use CSV files from the [download page][3]. Locations comes from the [Location Metadata File][4].

### Notes

- there are different methods for projections for datapoints for future years. We only keep the Medium projection.
- We compute total dependency ratio and sex ratio by age groups in `ratios` folder. 

[3]: https://population.un.org/wpp/Download/Standard/CSV/
[4]: https://population.un.org/wpp/Download/Documentation/Documentation/

## How to run the script

You should download all csv files listed in `etl/scripts/urls.txt` from WPP and put them into the `etl/source` directory. Then you can run the etl.py to create the dataset.

