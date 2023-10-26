create database spotify_project;

use spotify_project;

create schema spotify_data;

use schema spotify_data;

CREATE or replace TABLE tblsongs (
    todays_date date,
    song_name STRING,
    popularity NUMBER,
    song_id STRING,
    song_duration_ms NUMBER,
    song_urls STRING,
    album_id STRING
);

CREATE or replace TABLE tblartists (
    todays_date date,
    release_date DATE,
    artists_name STRING,
    artists_id STRING,
    song STRING,
    album_name STRING
);

CREATE or replace TABLE tblalbum (
    todays_date date,
    album_added_date TIMESTAMP,
    album_name STRING,
    album_url STRING,
    total_tracks NUMBER,
    album_id STRING
);


CREATE OR REPLACE STORAGE INTEGRATION s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::154508655573:role/snowflake-s3-connection'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-dataengineering-projects/transformed_data/')
  COMMENT = 'This is my storage integration to the S3 bucket for Spotify data';


Desc integration s3_init;

CREATE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
null_if = ('Null','null')
empty_field_as_null = True;



create or replace stage aws_spotify_data
url = 's3://snowflake-dataengineering-projects/transformed_data/'
storage_integration = s3_init
file_format = my_csv_format;


list @aws_spotify_data/album_data;
list @aws_spotify_data/artists_data;
list @aws_spotify_data/songs_data;


create or replace pipe album_pipe
auto_ingest = True
as 
copy into tblalbum
from @aws_spotify_data/album_data;

select * from tblalbum;

desc pipe album_pipe;


create or replace pipe songs_pipe
auto_ingest = True
as 
copy into tblsongs
from @aws_spotify_data/songs_data;

create or replace pipe artist_pipe
auto_ingest = True
as 
copy into tblartists
from @aws_spotify_data/artists_data;

desc pipe artist_pipe;

show pipes;

truncate table tblalbum;

select * from tblalbum;
select * from tblartists;
select * from tblsongs;

truncate table tblalbum;
truncate table tblartists;
truncate table tblsongs;

create or replace view tblsongs_views as(
select *,
ROW_NUMBER() OVER (partition by todays_date order by song_rank) AS trend_no
from
(select *,
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS song_rank
from tblsongs
) inner_query);



create or replace view tblsongs_views as
(select *,
ROW_NUMBER() OVER (partition by todays_date ORDER BY (SELECT NULL)) AS song_rank
from tblsongs
);

select * from tblsongs_views;




