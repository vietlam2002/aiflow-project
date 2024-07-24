from snowflake import connector

conn = connector.connect(
    user='',
    password='',
    account=''
)

access_key=''
secret_key=''

conn.cursor().execute(f"""
COPY INTO MYAIRFLOW_DATABASE.PUBLIC.TITLE_RATINGS from s3://mynvl-aws-bucket/title.ratings.tsv.gz
    CREDENTIALS = (
        AWS_KEY_ID='{access_key}', AWS_SECRET_KEY='{secret_key}')
    FILE_FORMAT = (field_delimiter='\t', skip_header=1)   
""")

