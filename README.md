# Analyzing Climate Change in Canada Over 6 Decades
- [Basant Singh](https://www.linkedin.com/in/basantsingh1000/) & [Hasan Zaheer](https://www.linkedin.com/in/muhammad-hasan-zaheer-096282217/)
  
This project involves analyzing Canadian climate data over six decades, leveraging weather station information from various sources. Below is a summary of the data sources, APIs, and website descriptions for the relevant datasets.

1. Climate Data - Station Selection and Download:
source- [link](https://api.weather.gc.ca/collections/climate-stations/items)  
This portal provides comprehensive climate data for Canada. It allows users to download datasets for various weather stations, including information such as the Station ID and Station Name. It serves as a great resource for selecting weather stations based on your analysis needs, focusing on climate resilience and adaptation.

2. Government of Canada API for Climate Stations:
source- [link](http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=155&Year=2024&timeframe=2)  
This API from the Government of Canada provides programmatic access to information about Canadian climate stations. Users can fetch metadata about different weather stations, including location, station ID, operational dates, and more, making it a valuable resource for integrating real-time or historical climate data in analysis.

- An ETL job with 8,000 csv file readings from the 70 odd weather station was set up, and a QuickSight dashboard was used. Data filtering was performed using Spark Glue.


# Video

https://github.com/user-attachments/assets/d0328029-6af7-41e6-a51e-c0fa59e1c613


# Observations-
1. Climate change is a reality, as there has been an increase in maximum, minimum, and average temperatures after 2000 at majority of stations.
2. There is a decrease in snowfall, rainfall after year 2000 at the majority of stations


# Architecture

![Mind map (2)](https://github.com/user-attachments/assets/085aae2c-4ad0-4adb-8f20-ffdbfdcc13ad)

1. Create an EventBridge rule to trigger an EC2 instance.
2. Store 8,000 CSV files in an S3 bucket.
3. Trigger an AWS Glue job after storing the files.
4. Aggregate the 8,000 CSV files into a single CSV file.
5. Filter the aggregated CSV file.
6. Use Amazon QuickSight to create a dashboard.



Kaggle- [output dataset link](https://www.kaggle.com/datasets/basantsingh1000/canada-climate-data-set/data)



