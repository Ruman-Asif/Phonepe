# Phonepe project by Ruman Asif
Python,sql,streamlit and plotly used for data visualization
Nested json files are stored classified according to quarter, year, states, country and transaction or user
The data is also divided into data fro transactions, data for hover over the map and for top transactions and users.
Python pulls the data from these nested json files by using normalize function.
As the files are distributed in various folders like quarters(1-4), years(2018-2022), states of India, etc. I have used os library in python to dynamically reference these files and extract data from them.
I have also created addition columns like states, year and quarter for easy data classification and visualization
The data from various files has been concatenated in a dataframe to create a single dataframe.
Then the data columns are rearranged and renamed according to our needs.
The states column values have been corrected for looking up states in our polygon json file. For example: andaman-&-nicobar has been correctly name "Andaman and Nicobar according to proper names given by the Indian code of standards.
This data is then stored in the form of tables in a sql database called phonepe1 so that data can be retrieved from here whenever we want.
Next comes the data visualization part. I have used plotly via streamlit for creating geo spatial data, pie charts and bar charts.
Multiple multiselect dropdowns have been created so that data can be selected according to users needs for visualization.
The dataframes for these plots are created by pulling data from the sql tables stored in our database.
The groupby and value selections are used to filter the data in the dataframe.
I have tried to make the streamlit app as beautiful as possible by choosing various colours, phonepe icons and layouts.
Hope you like my project. Thanks
