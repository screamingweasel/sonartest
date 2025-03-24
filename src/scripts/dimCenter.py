from prefect import flow, task
from prefect.logging import get_run_logger
from textwrap import dedent
from prefect_data_platform.applications.data_and_analytics_homegrid.scripts.utils.snowflake_db_util import (
    execute_nonquery,
    execute_query_fetchone,
    get_snowflake_connection,

)
from prefect_data_platform.applications.data_and_analytics_homegrid.scripts.utils.env import (
    settings)

import asyncio

config = settings()

# This is jim's edit!
@task
async def Package_SQL___Truncate_Staging_Table_Center(snowflake_conn):
    execute_nonquery(snowflake_conn, "TRUNCATE TABLE ONEHOME_OHSDWSTAGE_ONECARE.DimCenter;")
    print("Package_SQL___Truncate_Staging_Table_Center executed --------------------------------------------")


@task
async def Package_SQL___Get_Last_Loaded_DW_Modified_Date(snowflake_conn, sql_FetchLastModifiedDatetime: str):
    return execute_query_fetchone(snowflake_conn, dedent(sql_FetchLastModifiedDatetime))


@task
async def Package_DFT___Stage_Center(snowflake_conn, LastModifiedDatetime: str, ETLCutoffDatetime: str) -> None:
    logger = get_run_logger()
    logger.info(
        f"Calling stored procedure with parameters - LastModifiedDatetime: {LastModifiedDatetime}, ETLCutoffDatetime: {ETLCutoffDatetime}")
    sql1 = dedent("""WITH populate AS PROCEDURE (LastModifiedDatetime TIMESTAMP_NTZ(3),ETLCutoffDatetime TIMESTAMP_NTZ(3))
        RETURNS INTEGER
        AS
        $$
        BEGIN
        CREATE OR REPLACE TRANSIENT TABLE Trans_DimCenter (
            CenterId NUMBER(38,0) ,
            CenterNumber VARCHAR (50),
            CenterName VARCHAR (150),
            CenterFacilityName VARCHAR (150),
            CenterType VARCHAR (50),
            Address VARCHAR (50),
            Address1 VARCHAR (50),
            City VARCHAR (65),
            County VARCHAR (65),
            Code VARCHAR (5),
            State VARCHAR (65),
            StateInitials VARCHAR (2),
            FullAddress VARCHAR (500),
            Fax NVARCHAR (15),
            CreatedByUser VARCHAR (50),
            CreatedByUserName VARCHAR (101),
            ModifiedByUser VARCHAR (50),
            ModifiedByUserName VARCHAR (101),
            CreatedDateTime TIMESTAMP ,
            LastModifiedDateTime TIMESTAMP );

            CALL DAW_DEV.ONEHOME_OHSDWSTAGE_ONECARE.spStageDimCenter(:LastModifiedDatetime, :ETLCutoffDatetime);
            INSERT INTO Trans_DimCenter SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            RETURN 0;
            END
        $$
        CALL populate(?,?);
    """)

    sql2 = dedent("""
        INSERT INTO ONEHOME_OHSDWSTAGE_ONECARE.DimCenter
        (CenterId, CenterNumber, CenterName, CenterFacilityName, CenterType, Address, 
        Address1, City, County, ZipCode, State, StateInitials, FullAddress, Fax, CreatedByUser, 
        CreatedByUserName, ModifiedByUser, ModifiedByUserName, CreatedDateTime, LastModifiedDateTime)
        WITH "OLE_SRC - Fetch OneCare Center - dbReader" as (
        SELECT 
            CenterId, CenterNumber, CenterName, CenterFacilityName, CenterType, Address, Address1, 
            City, County, Code, State, StateInitials, FullAddress, Fax, CreatedByUser, CreatedByUserName, 
            ModifiedByUser, ModifiedByUserName, CreatedDateTime, LastModifiedDateTime
        FROM Trans_DimCenter
        ), "OLE_SRC - Fetch OneCare Center" as (
        SELECT
            CenterId AS CenterId, CenterNumber AS CenterNumber, CenterName AS CenterName, 
            CenterFacilityName AS CenterFacilityName, CenterType AS CenterType, Address AS Address, 
            Address1 AS Address1, City AS City, County AS County, Code AS Code, State AS State, 
            StateInitials AS StateInitials, FullAddress AS FullAddress, Fax AS Fax, CreatedByUser AS CreatedByUser, 
            CreatedByUserName AS CreatedByUserName, ModifiedByUser AS ModifiedByUser, ModifiedByUserName AS ModifiedByUserName, 
            CreatedDateTime AS CreatedDateTime, LastModifiedDateTime AS LastModifiedDateTime
        FROM
            "OLE_SRC - Fetch OneCare Center - dbReader" AS dbReader
        )
        SELECT
        CenterId AS CenterId, CenterNumber AS CenterNumber, CenterName AS CenterName, 
        CenterFacilityName AS CenterFacilityName, CenterType AS CenterType, Address AS Address, 
        Address1 AS Address1, City AS City, County AS County, Code AS ZipCode, State AS State, 
        StateInitials AS StateInitials, FullAddress AS FullAddress, Fax AS Fax, CreatedByUser AS CreatedByUser, 
        CreatedByUserName AS CreatedByUserName, ModifiedByUser AS ModifiedByUser, ModifiedByUserName AS ModifiedByUserName, 
        CreatedDateTime AS CreatedDateTime, LastModifiedDateTime AS LastModifiedDateTime
        FROM
        "OLE_SRC - Fetch OneCare Center" AS OLE_DB_Source_Output""")

    sql3 = dedent("DROP TABLE IF EXISTS Trans_DimCenter")
    param = (LastModifiedDatetime, ETLCutoffDatetime,)
    execute_nonquery(snowflake_conn, sql1, param)
    print("Query 1 executed---------------------------------------------------------------------------------------------")
    execute_nonquery(snowflake_conn, sql2)
    print("Query 2 executed------------------------------------------------------------------------------------------------------")
    execute_nonquery(snowflake_conn, sql3)
    print("Query 3 executed")

    # connector.get_connection().commit()


@task
async def Package_SQL___Insert_Data_Quality_Rows(snowflake_conn, sql_insertDQRows: str):
    print("executing insert statement")
    execute_nonquery(snowflake_conn, sql_insertDQRows)
    pass


@task
async def Package_SQL___Populate_Center(snowflake_conn, sql_populateDWSproc: str):
    execute_nonquery(snowflake_conn, sql_populateDWSproc)


@flow
async def dimCenter(IsHistoricalFlag: str, ETLCutoffDatetime: str) -> None:
    snowflake_conn = get_snowflake_connection('onehome')
    destinationSchema = "ONEHOME_OHSDWSTAGE_ONECARE"
    destinationTable = "DIMCenter"
    destinationSchemaTable = destinationSchema + "." + destinationTable
    #LastModifiedDatetime = "1900-01-01 00:00:00.000"

    sql_FetchLastModifiedDatetime = "SELECT COALESCE(MAX(LastModifiedDatetime), '1900-01-01 00:00:00.000') AS LastModifiedDatetime FROM " + destinationSchemaTable
    sql_insertDQRows = "Call ONEHOME_OHSDW_DW_GOLD.spInsertDimensionDataQualityRows " + "('" + destinationTable + "','" + destinationSchema + "');"
    sql_populateDWSproc = "call ONEHOME_OHSDW_DW_GOLD.spPopulate" + destinationTable + "();"

    sql_truncateTable = "truncate table " + destinationSchema + "." + destinationTable

    await Package_SQL___Truncate_Staging_Table_Center(snowflake_conn)
    LastModifiedDatetime, = await Package_SQL___Get_Last_Loaded_DW_Modified_Date(snowflake_conn, sql_FetchLastModifiedDatetime)
    await Package_DFT___Stage_Center(snowflake_conn, LastModifiedDatetime, ETLCutoffDatetime)
    if IsHistoricalFlag == "1":
        await Package_SQL___Insert_Data_Quality_Rows(snowflake_conn, sql_insertDQRows)
    await Package_SQL___Populate_Center(snowflake_conn, sql_populateDWSproc)


if __name__ == "__main__":
    asyncio.run(dimCenter(IsHistoricalFlag="1", ETLCutoffDatetime="2021-12-31 00:00:00.0000"))
