<h1> Read Me <\h1>
This is the read me document for the Udacity course "Data Engineering Nano Degree", "Data Modelling with Postgres"
The aim of this project is to demonstrate a sufficient understanding of concepts of data modelling through modelling a star diagram and building an ETL pipeline using python.
    
<h2> Purpose of Database <h2>
The purpose of this database design is to 
    1. Provide a database where JSON data can be safely and securely transferred to.
    2. To enable Sparkify to access this data in a straightforward and timely fashion to perform relevant analytical queries (eg "how many people are listening per day?", "How many songs are being played per day?", "What are the top 50 songs played in a given period?")
    
<h2> Database schema design.<\h2>
<p>A standard star schema was chosen to allow the users to perform quick queries on indvidual areas of interest (eg, users, songs) without having to perform expensive joins operations over multiple tables.  There was a possibilty of joining songplays to songs and then artists to songs to create a snowflake diagram but this was rejected as requiring a join table to handle the many-to-many relationship that would result.</p>

The schema diagram is given below:
![Schema diagram](/'schema diagram v_1_0.jpg')

<p>Note that the "song_play_id" is a serial datatype which adds an incremented integer value each time a row is added to the table.  Alternatively, a composite key from song_id,user_id, artist_id could have been generated, but given data quality issues in the data supplied, this would not be practical.  For a similar reason no formal relationships between the fact and dimension tables with the necessary constraints to catch missing foreign keys were created.</p>
    
<p>The insert statements have the ON CONFLICT DO NOTHING clause added to them.  Although redundant as no relationship constraints were created, they were added for a future time when these relationship between tables might be defined.  </p>
    
<p>Use of data types.  Generally varchar was used as a default, unless the data was numeric.  Integer was generally indicatied, apart from duration, which required numeric as it contains decimals.  The only other datatype used was "datetime" to capture the start_time information in the time and songplays tables.</p>

<h2>ELT Design</h2>
<p>The ETL code creates a connection to the existing database.  It then opens and parses the json files from the song_data folder into a dataframe which has relevant columns inserted via hard-coded SQL queries into the relevant fields of the songs and artists tables.  The json files from the log_files folder is then read and parsed.  Irrelvant data is filtered out and the time and user tables are populated in the same fashion.  There are further steps to extract song and artist keys and to use them to populate the fact table songplays.  There is also a check to relace NULL results with the python None datatype before populating the table<p/>
    
<h2>Inventory of files in this repository<h2>
1. README.md.           A introductory and explantory file
2. create_tables.py     A python file containing the code to drop and create the 'sparkify' database and the necessary tables, as specified in 'sql_queries.py'
3. sql_queries.py       A python file containing various string containing SQL queries to drop, create tables, or insert data into tables
4. test.ipynb           A jypiter notebook used to test the results of various operations on the sparkify database
5. etl.py               A python file containing to code used to create the etl pipeline to move the JSON data into the new database

<h2>How to run the full script<\h2>
1. Open the console window and type "python create_tables.py"
2. Once the command line returns, type "python elt.py".  The console screen should then indicate each row being submitted to the database.
    
<h2>Examples of queries</h2>
**How many users are free, how many are paying?**
`SELECT level, count(level) 
 FROM users GROUP BY level`
Free: 76 Paid 20
    
**How many songs are being listened to by each level??**
`SELECT level, count(level) 
 FROM songplays 
 GROUP BY level`
Free: 1,229 Paid 5,591
    
**What is the gender split between free and paid users?**
`SELECT u.gender,sp.level, count(sp.level) 
 FROM songplays as sp 
    JOIN users as u 
    ON sp.user_id = u.user_id 
 GROUP BY u.gender, sp.level `