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

    public static string DashboardQuery(string? department) => $@"
        SELECT
            h.[Date],
            h.Department,
            h.Exchange_Total_Primary_Item_Count,
            h.Exchange_Total_Archive_Item_Count,
            h.Exchange_Total_Primary_Total_Size_GB,
            h.Exchange_Total_Archive_Total_Size_GB,
            h.Exchange_Total_Primary_SystemMessage_Count,
            h.Exchange_Total_Primary_SystemMessage_Size_Bytes,
            h.Exchange_Total_Primary_Recoverable_Count,
            h.Exchange_Total_Primary_Recoverable_Size_Bytes,
            h.Exchange_Total_Archive_SystemMessage_Count,
            h.Exchange_Total_Archive_SystemMessage_Size_Bytes,
            h.Exchange_Total_Archive_Recoverable_Count,
            h.Exchange_Total_Archive_Recoverable_Size_Bytes,
            h.OneDrive_Total_File_Count,
            h.OneDrive_Total_StorageUsedGB,
            h.SharePoint_Total_File_Count,
            h.SharePoint_Total_StorageUsedGB,
            h.Users_Total
        FROM dbo.PowerBIDataModelHistory h
        WHERE h.ExecutionId = (
            SELECT TOP 1 ExecutionId FROM dbo.PowerBIDataModelHistory ORDER BY [Date] DESC
        )
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND h.Department = '{department.Replace("'", "''")}'")};";

    public static string CountryQuery(string? department) => $@"
        SELECT CountryName, CountryCount
        FROM dbo.PowerBICountryOrRegion
        WHERE CountryCount > 0
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND Department = '{department.Replace("'", "''")}'")};";

    public static string DepartmentListQuery() => @"
        SELECT DISTINCT ISNULL(Department,'Unknown') AS Department
        FROM dbo.PowerBIDataModelHistory
        WHERE ExecutionId = (
            SELECT TOP 1 ExecutionId FROM dbo.PowerBIDataModelHistory ORDER BY [Date] DESC
        )
        ORDER BY Department;";
}
