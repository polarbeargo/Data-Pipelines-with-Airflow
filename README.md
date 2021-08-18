# Data-Pipelines-with-Airflow  
[//]: # (Image References)

[image1]: ./images/Dag.png
[image2]: ./images/treeView.png

![][image1]   
![][image2]  
## Project Datasets
* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data'  
### Song Dataset  
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset  
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset will be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
## Fact Table  
1. songplays - records in log data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
## Dimension Tables  

2. users - users in the app  
   * user_id (string), first_name (string), last_name (string), gender (string), level (string)  
3. songs - songs in music database    
   * song_id (long), title (string), artist_id (string), year (integer), duration (double)    
4. artists - artists in music database   
   * artist_id (string), name (string), location (string), latitude (double), longitude (double)    
5. time - timestamps of records in songplays broken down into specific units  
   * start_time (timestamp), hour (integer), day (integer), week (integer), month (integer), year (integer), weekday (string)   

## Operate Airflow web UI  
Click on the Admin tab and select Connections. On the create connection page, enter the following values:

   - Conn Id: Enter `aws_credentials`.  
   - Conn Type: Enter `Amazon Web Services`.  
   - Login: Enter your `Access key ID` from the IAM User credentials you downloaded earlier.  
   - Password: Enter your `Secret access key` from the IAM User credentials you downloaded earlier.  

On the next create connection page, enter the following values:

   - Conn Id: Enter `redshift`.  
   - Conn Type: Enter `Postgres`.  
   - Host: Enter `the endpoint of your Redshift cluster`, excluding the port at the end.  
   - Login: Enter `awsuser`.  
   - Password: Enter the password you created when launching your Redshift cluster.  
   - Port: Enter `5439`.  
 
