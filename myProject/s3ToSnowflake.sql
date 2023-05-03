use practice_snowflake;


create or replace table result(
  col1 varchar(1000),
  col2 varchar(1000),
  col3 varchar(1000),
  col4 varchar(1000),
  col5 varchar(1000)
);


CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
TYPE=CSV
FIELD_DELIMITER=',' SKIP_HEADER=1
NULL_IF=('NULL','null')
EMPTY_FIELD_AS_NULL=true;

CREATE OR REPLACE STAGE MY_STAGE
url='s3://{bucket-name}/sample.csv'
credentials=(aws_key_id='<access-key>' aws_secret_key='<secret-access-key')
file_format=my_csv_format;

show stages;

LIST @PRACTICE_SNOWFLAKE.PUBLIC.MY_STAGE;

COPY INTO RESULT 
FROM @PRACTICE_SNOWFLAKE.PUBLIC.MY_STAGE
FILE_FORMAT=(FORMAT_NAME=MY_CSV_FORMAT);

truncate table result;

SELECT * FROM "PRACTICE_SNOWFLAKE"."PUBLIC"."RESULT" limit 5;
