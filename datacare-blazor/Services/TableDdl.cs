namespace DataCareLite.Services;

public static class TableDdl
{
    public static readonly Dictionary<string, string> All = new()
    {
        ["ExecutionLog"] = @"
            IF OBJECT_ID('dbo.ExecutionLog','U') IS NULL
            CREATE TABLE dbo.ExecutionLog (
                ExecutionId      UNIQUEIDENTIFIER,
                ExecutionDate    DATETIME2,
                ReportName       NVARCHAR(100),
                Status           NVARCHAR(50),
                RowsRetrieved    INT,
                RowsInserted     INT,
                DurationSeconds  INT,
                ErrorMessage     NVARCHAR(MAX),
                MachineName      NVARCHAR(255),
                AppVersion       NVARCHAR(50));",

        ["MicrosoftUsers"] = @"
            IF OBJECT_ID('dbo.MicrosoftUsers','U') IS NULL
            CREATE TABLE dbo.MicrosoftUsers (
                Id                NVARCHAR(255),
                DisplayName       NVARCHAR(255),
                UserPrincipalName NVARCHAR(255) NOT NULL,
                Mail              NVARCHAR(255),
                Department        NVARCHAR(255),
                JobTitle          NVARCHAR(255),
                AccountEnabled    NVARCHAR(50),
                CreatedDateTime   NVARCHAR(50),
                InsertedAt        DATETIME2,
                SourceReport      NVARCHAR(100),
                CountryOrRegion   NVARCHAR(50));",

        ["MicrosoftExchange"] = @"
            IF OBJECT_ID('dbo.MicrosoftExchange','U') IS NULL
            CREATE TABLE dbo.MicrosoftExchange (
                StorageUsedGB                          FLOAT,
                ___Report_Refresh_Date                 NVARCHAR(50),
                User_Principal_Name                    NVARCHAR(255) NOT NULL,
                Display_Name                           NVARCHAR(255),
                Is_Deleted                             NVARCHAR(50),
                Deleted_Date                           NVARCHAR(50),
                Created_Date                           NVARCHAR(50),
                Last_Activity_Date                     NVARCHAR(50),
                Item_Count                             INT,
                Storage_Used__Byte_                    BIGINT,
                Issue_Warning_Quota__Byte_             BIGINT,
                Prohibit_Send_Quota__Byte_             BIGINT,
                Prohibit_Send_Receive_Quota__Byte_     BIGINT,
                Deleted_Item_Count                     INT,
                Deleted_Item_Size__Byte_               BIGINT,
                Deleted_Item_Quota__Byte_              BIGINT,
                Has_Archive                            NVARCHAR(50),
                Report_Period                          NVARCHAR(50),
                ReportPeriod                           NVARCHAR(50),
                ReportDate                             DATETIME2,
                InsertedAt                             DATETIME2,
                SourceReport                           NVARCHAR(100),
                Department                             NVARCHAR(50),
                Primary_Item_Count                     INT,
                Primary_TotalItemSize                  NVARCHAR(50),
                Primary_Total_Size_Bytes               BIGINT,
                Primary_SystemMessage_Count            INT,
                Primary_SystemMessage_Size_Bytes       BIGINT,
                Primary_Recoverable_Count              INT,
                Primary_Recoverable_Size_Bytes         BIGINT,
                Primary_Recoverable_Mode               NVARCHAR(50),
                Archive_Item_Count                     INT,
                Archive_TotalItemSize                  NVARCHAR(50),
                Archive_Total_Size_Bytes               BIGINT,
                Archive_SystemMessage_Count            INT,
                Archive_SystemMessage_Size_Bytes       BIGINT,
                Archive_Recoverable_Count              INT,
                Archive_Recoverable_Size_Bytes         BIGINT,
                Archive_Recoverable_Mode               NVARCHAR(50));",

        ["MicrosoftOneDrive"] = @"
            IF OBJECT_ID('dbo.MicrosoftOneDrive','U') IS NULL
            CREATE TABLE dbo.MicrosoftOneDrive (
                StorageUsedGB            FLOAT,
                ___Report_Refresh_Date   NVARCHAR(50),
                Site_Id                  NVARCHAR(255),
                Site_URL                 NVARCHAR(500),
                Owner_Display_Name       NVARCHAR(255),
                Is_Deleted               NVARCHAR(50),
                Last_Activity_Date       NVARCHAR(50),
                File_Count               INT,
                Active_File_Count        INT,
                Storage_Used__Byte_      BIGINT,
                Storage_Allocated__Byte_ BIGINT,
                Owner_Principal_Name     NVARCHAR(255) NOT NULL,
                Department               NVARCHAR(50),
                Report_Period            NVARCHAR(50),
                ReportPeriod             NVARCHAR(50),
                ReportDate               DATETIME2,
                InsertedAt               DATETIME2,
                SourceReport             NVARCHAR(100));",

        ["MicrosoftSharePoint"] = @"
            IF OBJECT_ID('dbo.MicrosoftSharePoint','U') IS NULL
            CREATE TABLE dbo.MicrosoftSharePoint (
                StorageUsedGB            FLOAT,
                ___Report_Refresh_Date   NVARCHAR(50),
                Site_Id                  NVARCHAR(255),
                Site_URL                 NVARCHAR(500),
                Owner_Display_Name       NVARCHAR(255),
                Is_Deleted               NVARCHAR(50),
                Last_Activity_Date       NVARCHAR(50),
                File_Count               INT,
                Active_File_Count        INT,
                Page_View_Count          INT,
                Visited_Page_Count       INT,
                Storage_Used__Byte_      BIGINT,
                Storage_Allocated__Byte_ BIGINT,
                Root_Web_Template        NVARCHAR(100),
                Owner_Principal_Name     NVARCHAR(255),
                Report_Period            NVARCHAR(50),
                ReportPeriod             NVARCHAR(50),
                ReportDate               DATETIME2,
                InsertedAt               DATETIME2,
                SourceReport             NVARCHAR(100),
                Department               NVARCHAR(50));",

        ["PowerBIDataModelHistory"] = @"
            IF OBJECT_ID('dbo.PowerBIDataModelHistory','U') IS NULL
            CREATE TABLE dbo.PowerBIDataModelHistory (
                ExecutionId                                     UNIQUEIDENTIFIER,
                [Date]                                          DATETIME2,
                Department                                      NVARCHAR(255),
                Exchange_Total_Primary_Item_Count               BIGINT,
                Exchange_Total_Archive_Item_Count               BIGINT,
                Exchange_Total_Primary_Total_Size_GB            DECIMAL(18,2),
                Exchange_Total_Archive_Total_Size_GB            DECIMAL(18,2),
                Exchange_Total_Primary_Total_Size_Bytes         BIGINT,
                Exchange_Total_Primary_SystemMessage_Count      BIGINT,
                Exchange_Total_Primary_SystemMessage_Size_Bytes BIGINT,
                Exchange_Total_Primary_Recoverable_Count        BIGINT,
                Exchange_Total_Primary_Recoverable_Size_Bytes   BIGINT,
                Exchange_Total_Archive_Total_Size_Bytes         BIGINT,
                Exchange_Total_Archive_SystemMessage_Count      BIGINT,
                Exchange_Total_Archive_SystemMessage_Size_Bytes BIGINT,
                Exchange_Total_Archive_Recoverable_Count        BIGINT,
                Exchange_Total_Archive_Recoverable_Size_Bytes   BIGINT,
                OneDrive_Total_File_Count                       BIGINT,
                OneDrive_Total_StorageUsedGB                    DECIMAL(18,2),
                SharePoint_Total_File_Count                     BIGINT,
                SharePoint_Total_StorageUsedGB                  DECIMAL(18,2),
                Users_Total                                     INT);",

        ["PowerBICountryOrRegion"] = @"
            IF OBJECT_ID('dbo.PowerBICountryOrRegion','U') IS NULL
            CREATE TABLE dbo.PowerBICountryOrRegion (
                Department   NVARCHAR(255),
                CountryName  NVARCHAR(MAX),
                CountryCount INT);"
    };

    public const string PowerBIAggregateQuery = @"
        INSERT INTO dbo.PowerBIDataModelHistory
        SELECT
            @ExecutionId,
            GETDATE(),
            e.Department,
            e.Exchange_Total_Primary_Item_Count,
            e.Exchange_Total_Archive_Item_Count,
            e.Exchange_Total_Primary_Total_Size_GB,
            e.Exchange_Total_Archive_Total_Size_GB,
            e.Exchange_Total_Primary_Total_Size_Bytes,
            e.Exchange_Total_Primary_SystemMessage_Count,
            e.Exchange_Total_Primary_SystemMessage_Size_Bytes,
            e.Exchange_Total_Primary_Recoverable_Count,
            e.Exchange_Total_Primary_Recoverable_Size_Bytes,
            e.Exchange_Total_Archive_Total_Size_Bytes,
            e.Exchange_Total_Archive_SystemMessage_Count,
            e.Exchange_Total_Archive_SystemMessage_Size_Bytes,
            e.Exchange_Total_Archive_Recoverable_Count,
            e.Exchange_Total_Archive_Recoverable_Size_Bytes,
            o.OneDrive_Total_File_Count,
            o.OneDrive_Total_StorageUsedGB,
            s.SharePoint_Total_File_Count,
            s.SharePoint_Total_StorageUsedGB,
            u.Users_Total
        FROM (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                SUM(ISNULL([Primary_Item_Count],0)) AS Exchange_Total_Primary_Item_Count,
                SUM(ISNULL([Archive_Item_Count],0)) AS Exchange_Total_Archive_Item_Count,
                CAST(ROUND(SUM(ISNULL([Primary_Total_Size_Bytes],0))/1073741824.0,2) AS DECIMAL(18,2)) AS Exchange_Total_Primary_Total_Size_GB,
                CAST(ROUND(SUM(ISNULL([Archive_Total_Size_Bytes],0))/1073741824.0,2) AS DECIMAL(18,2)) AS Exchange_Total_Archive_Total_Size_GB,
                SUM(ISNULL([Primary_Total_Size_Bytes],0)) AS Exchange_Total_Primary_Total_Size_Bytes,
                SUM(ISNULL([Primary_SystemMessage_Count],0)) AS Exchange_Total_Primary_SystemMessage_Count,
                SUM(ISNULL([Primary_SystemMessage_Size_Bytes],0)) AS Exchange_Total_Primary_SystemMessage_Size_Bytes,
                SUM(ISNULL([Primary_Recoverable_Count],0)) AS Exchange_Total_Primary_Recoverable_Count,
                SUM(ISNULL([Primary_Recoverable_Size_Bytes],0)) AS Exchange_Total_Primary_Recoverable_Size_Bytes,
                SUM(ISNULL([Archive_Total_Size_Bytes],0)) AS Exchange_Total_Archive_Total_Size_Bytes,
                SUM(ISNULL([Archive_SystemMessage_Count],0)) AS Exchange_Total_Archive_SystemMessage_Count,
                SUM(ISNULL([Archive_SystemMessage_Size_Bytes],0)) AS Exchange_Total_Archive_SystemMessage_Size_Bytes,
                SUM(ISNULL([Archive_Recoverable_Count],0)) AS Exchange_Total_Archive_Recoverable_Count,
                SUM(ISNULL([Archive_Recoverable_Size_Bytes],0)) AS Exchange_Total_Archive_Recoverable_Size_Bytes
            FROM [dbo].[MicrosoftExchange]
            GROUP BY ISNULL(Department, 'Unknown')
        ) e
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                SUM(ISNULL([File_Count],0)) AS OneDrive_Total_File_Count,
                CAST(SUM(ISNULL([StorageUsedGB],0)) AS DECIMAL(18,2)) AS OneDrive_Total_StorageUsedGB
            FROM [dbo].[MicrosoftOneDrive]
            GROUP BY ISNULL(Department, 'Unknown')
        ) o ON e.Department = o.Department
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                SUM(ISNULL([File_Count],0)) AS SharePoint_Total_File_Count,
                CAST(SUM(ISNULL([StorageUsedGB],0)) AS DECIMAL(18,2)) AS SharePoint_Total_StorageUsedGB
            FROM [dbo].[MicrosoftSharePoint]
            GROUP BY ISNULL(Department, 'Unknown')
        ) s ON e.Department = s.Department
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                COUNT(DISTINCT [UserPrincipalName]) AS Users_Total
            FROM [dbo].[MicrosoftUsers]
            WHERE [UserPrincipalName] IS NOT NULL
            GROUP BY ISNULL(Department, 'Unknown')
        ) u ON e.Department = u.Department;";

    // Returns one row per calendar month (most recent execution per month),
    // summed across all departments (or filtered to a specific one).
    // Columns: [0] MonthLabel, [1] YearNum, [2] MonthNum, [3..19] metrics
    public static string DashboardQuery(string? department) => $@"
        WITH LastExecPerMonth AS (
            SELECT
                YEAR([Date])  AS YearNum,
                MONTH([Date]) AS MonthNum,
                MAX([Date])   AS LastDate,
                (
                    SELECT TOP 1 h2.ExecutionId
                    FROM dbo.PowerBIDataModelHistory h2
                    WHERE YEAR(h2.[Date]) = YEAR(h1.[Date])
                      AND MONTH(h2.[Date]) = MONTH(h1.[Date])
                    ORDER BY h2.[Date] DESC
                ) AS ExecutionId
            FROM dbo.PowerBIDataModelHistory h1
            GROUP BY YEAR([Date]), MONTH([Date])
        )
        SELECT
            FORMAT(lem.LastDate, 'MMMM', 'en-US')                          AS MonthLabel,
            lem.YearNum,
            lem.MonthNum,
            SUM(h.Exchange_Total_Primary_Item_Count)                        AS Exchange_Total_Primary_Item_Count,
            SUM(h.Exchange_Total_Archive_Item_Count)                        AS Exchange_Total_Archive_Item_Count,
            CAST(SUM(h.Exchange_Total_Primary_Total_Size_GB)  AS DECIMAL(18,2)) AS Exchange_Total_Primary_Total_Size_GB,
            CAST(SUM(h.Exchange_Total_Archive_Total_Size_GB)  AS DECIMAL(18,2)) AS Exchange_Total_Archive_Total_Size_GB,
            SUM(h.Exchange_Total_Primary_SystemMessage_Count)               AS Exchange_Total_Primary_SystemMessage_Count,
            SUM(h.Exchange_Total_Primary_SystemMessage_Size_Bytes)          AS Exchange_Total_Primary_SystemMessage_Size_Bytes,
            SUM(h.Exchange_Total_Primary_Recoverable_Count)                 AS Exchange_Total_Primary_Recoverable_Count,
            SUM(h.Exchange_Total_Primary_Recoverable_Size_Bytes)            AS Exchange_Total_Primary_Recoverable_Size_Bytes,
            SUM(h.Exchange_Total_Archive_SystemMessage_Count)               AS Exchange_Total_Archive_SystemMessage_Count,
            SUM(h.Exchange_Total_Archive_SystemMessage_Size_Bytes)          AS Exchange_Total_Archive_SystemMessage_Size_Bytes,
            SUM(h.Exchange_Total_Archive_Recoverable_Count)                 AS Exchange_Total_Archive_Recoverable_Count,
            SUM(h.Exchange_Total_Archive_Recoverable_Size_Bytes)            AS Exchange_Total_Archive_Recoverable_Size_Bytes,
            SUM(h.Exchange_Total_Archive_Total_Size_Bytes)                  AS Exchange_Total_Archive_Total_Size_Bytes,
            SUM(h.OneDrive_Total_File_Count)                                AS OneDrive_Total_File_Count,
            CAST(SUM(h.OneDrive_Total_StorageUsedGB)          AS DECIMAL(18,2)) AS OneDrive_Total_StorageUsedGB,
            SUM(h.SharePoint_Total_File_Count)                              AS SharePoint_Total_File_Count,
            CAST(SUM(h.SharePoint_Total_StorageUsedGB)        AS DECIMAL(18,2)) AS SharePoint_Total_StorageUsedGB,
            MAX(h.Users_Total)                                              AS Users_Total
        FROM LastExecPerMonth lem
        JOIN dbo.PowerBIDataModelHistory h ON h.ExecutionId = lem.ExecutionId
        WHERE 1=1
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND h.Department = '{department.Replace("'", "''")}'")}
        GROUP BY lem.YearNum, lem.MonthNum, lem.LastDate
        ORDER BY lem.YearNum, lem.MonthNum;";

    public static string CountryQuery(string? department) => $@"
        SELECT CountryName, CountryCount
        FROM dbo.PowerBICountryOrRegion
        WHERE CountryCount > 0
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND Department = '{department.Replace("'", "''")}'")};";

    // Departments are taken from the full history, not just the last run
    public static string DepartmentListQuery() => @"
        SELECT DISTINCT ISNULL(Department,'Unknown') AS Department
        FROM dbo.PowerBIDataModelHistory
        ORDER BY Department;";
}
