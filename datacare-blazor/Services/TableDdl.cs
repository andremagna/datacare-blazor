namespace DataCare.Services;

public static class TableDdl
{
    public static readonly Dictionary<string, string> All = new()
    {
        ["ExecutionLog"] = @"
            IF OBJECT_ID('dbo.ExecutionLog','U') IS NULL
            CREATE TABLE dbo.ExecutionLog (
                ExecutionId        UNIQUEIDENTIFIER,
                ExecutionDate      DATETIME2,
                ReportName         NVARCHAR(100),
                Status             NVARCHAR(50),
                RowsRetrieved      INT,
                RowsInserted       INT,
                DurationTimeJob    NVARCHAR(255),
                ErrorMessage       NVARCHAR(MAX),
                MachineName        NVARCHAR(255),
                PowerShellVersion  NVARCHAR(50),
                TableSizeMB        FLOAT);",

        ["EnvironmentConfiguration"] = @"
            IF OBJECT_ID('dbo.EnvironmentConfiguration','U') IS NULL
            CREATE TABLE dbo.EnvironmentConfiguration (
                Id                   INT IDENTITY(1,1) PRIMARY KEY,
                EnvironmentKey       NVARCHAR(100) NOT NULL,
                TenantId             NVARCHAR(255) NOT NULL,
                ClientId             NVARCHAR(255) NOT NULL,
                CertificateThumbprint NVARCHAR(255) NOT NULL,
                CreatedAt            DATETIME2 DEFAULT SYSDATETIME(),
                UpdatedAt            DATETIME2 NULL,
                CONSTRAINT UQ_EnvironmentKey UNIQUE (EnvironmentKey));",

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
                StorageUsedGB                      FLOAT,
                ___Report_Refresh_Date             NVARCHAR(50),
                User_Principal_Name                NVARCHAR(255) NOT NULL,
                Display_Name                       NVARCHAR(255),
                Is_Deleted                         NVARCHAR(50),
                Deleted_Date                       NVARCHAR(50),
                Created_Date                       NVARCHAR(50),
                Last_Activity_Date                 NVARCHAR(50),
                Item_Count                         INT,
                Storage_Used__Byte_                BIGINT,
                Issue_Warning_Quota__Byte_         BIGINT,
                Prohibit_Send_Quota__Byte_         BIGINT,
                Prohibit_Send_Receive_Quota__Byte_ BIGINT,
                Deleted_Item_Count                 INT,
                Deleted_Item_Size__Byte_           BIGINT,
                DeletedItemSizeGB                  FLOAT,
                Deleted_Item_Quota__Byte_          BIGINT,
                Has_Archive                        NVARCHAR(50),
                Report_Period                      NVARCHAR(50),
                ReportPeriod                       NVARCHAR(50),
                ReportDate                         DATETIME2,
                InsertedAt                         DATETIME2,
                SourceReport                       NVARCHAR(100),
                Department                         NVARCHAR(50),
                CountryOrRegion                    NVARCHAR(50));",

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
                CountryOrRegion          NVARCHAR(50),
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
                Department               NVARCHAR(50),
                CountryOrRegion          NVARCHAR(50),
                Report_Period            NVARCHAR(50),
                ReportPeriod             NVARCHAR(50),
                ReportDate               DATETIME2,
                InsertedAt               DATETIME2,
                SourceReport             NVARCHAR(100));",

        // PowerBIDataModelHistory — allineata allo script PS CreatePowerBIDataModelHistory
        // Colonne: Exchange_StorageUsedGB, Item_Count, Deleted_Item_Count, DeletedItemSizeGB,
        //          OneDrive, SharePoint, Users — append-only, mai droppata.
        ["PowerBIDataModelHistory"] = @"
            IF OBJECT_ID('dbo.PowerBIDataModelHistory','U') IS NULL
            CREATE TABLE dbo.PowerBIDataModelHistory (
                ExecutionId                  UNIQUEIDENTIFIER,
                [Date]                       DATETIME,
                Department                   NVARCHAR(255),
                Exchange_StorageUsedGB       DECIMAL(18,2),
                Exchange_Item_Count          BIGINT,
                Exchange_Deleted_Item_Count  BIGINT,
                Exchange_DeletedItemSizeGB   DECIMAL(18,2),
                OneDrive_Total_File_Count    BIGINT,
                OneDrive_Total_StorageUsedGB DECIMAL(18,2),
                SharePoint_Total_File_Count  BIGINT,
                SharePoint_Total_StorageUsedGB DECIMAL(18,2),
                Users_Total                  INT);",

        ["PowerBICountryOrRegion"] = @"
            IF OBJECT_ID('dbo.PowerBICountryOrRegion','U') IS NULL
            CREATE TABLE dbo.PowerBICountryOrRegion (
                Department   NVARCHAR(255),
                CountryName  NVARCHAR(MAX),
                CountryCount INT);",

        ["DownloadFile"] = @"
            IF OBJECT_ID('dbo.DownloadFile','U') IS NULL
            CREATE TABLE dbo.DownloadFile (
                Id                INT IDENTITY(1,1) PRIMARY KEY,
                DateTimeDownload  DATETIME2 DEFAULT SYSDATETIME(),
                [File]            VARBINARY(MAX) NULL,
                FileType          NVARCHAR(100) NOT NULL,
                FileName          NVARCHAR(500) NOT NULL,
                FileStorage       NVARCHAR(500) NOT NULL);",

        ["TaskSchedulerJob"] = @"
            IF OBJECT_ID('dbo.TaskSchedulerJob','U') IS NULL
            CREATE TABLE dbo.TaskSchedulerJob (
                Id               INT IDENTITY(1,1) PRIMARY KEY,
                JobName          NVARCHAR(200) NOT NULL,
                Description      NVARCHAR(1000) NULL,
                DaysOfWeek       NVARCHAR(100) NOT NULL,
                StartTime        NVARCHAR(10)  NOT NULL,
                Status           NVARCHAR(50)  NOT NULL DEFAULT 'ENABLED',
                ProgramPath      NVARCHAR(1000) NOT NULL,
                Arguments        NVARCHAR(1000) NULL,
                TaskFolder       NVARCHAR(500)  NOT NULL DEFAULT '\DataCare\',
                RunAsUser        NVARCHAR(200)  NOT NULL DEFAULT 'SYSTEM',
                CreatedAt        DATETIME2 DEFAULT SYSDATETIME(),
                UpdatedAt        DATETIME2 NULL,
                LastRunAt        DATETIME2 NULL,
                LastRunStatus    NVARCHAR(50) NOT NULL DEFAULT 'NEVER',
                LastRunMessage   NVARCHAR(MAX) NULL,
                CONSTRAINT UQ_TaskSchedulerJobName UNIQUE (JobName));"
    };

    // ── DownloadFile queries ──────────────────────────────────────────────

    public const string DownloadFileInsert = @"
        INSERT INTO dbo.DownloadFile (DateTimeDownload, [File], FileType, FileName, FileStorage)
        VALUES (SYSDATETIME(), @File, @FileType, @FileName, @FileStorage);";

    public const string DownloadFileSelectAll = @"
        SELECT Id, DateTimeDownload, FileType, FileName, FileStorage,
               DATALENGTH([File]) AS FileSizeBytes
        FROM dbo.DownloadFile
        ORDER BY DateTimeDownload DESC;";

    // ── TaskSchedulerJob queries ──────────────────────────────────────────

    public const string TaskSchedulerJobSelectAll = @"
        SELECT Id, JobName, Description, DaysOfWeek, StartTime, Status,
               ProgramPath, Arguments, TaskFolder, RunAsUser,
               CreatedAt, UpdatedAt, LastRunAt, LastRunStatus, LastRunMessage
        FROM dbo.TaskSchedulerJob
        ORDER BY JobName;";

    public static string TaskSchedulerJobInsert(
        string jobName, string description, string daysOfWeek, string startTime,
        string status, string programPath, string? arguments,
        string taskFolder, string runAsUser)
    {
        string argsVal = arguments == null ? "NULL" : $"N'{Esc(arguments)}'";
        return $"INSERT INTO dbo.TaskSchedulerJob " +
               $"(JobName, Description, DaysOfWeek, StartTime, Status, ProgramPath, Arguments, TaskFolder, RunAsUser) " +
               $"VALUES (N'{Esc(jobName)}', N'{Esc(description)}', N'{Esc(daysOfWeek)}', N'{Esc(startTime)}', " +
               $"N'{Esc(status)}', N'{Esc(programPath)}', {argsVal}, N'{Esc(taskFolder)}', N'{Esc(runAsUser)}');";
    }

    public static string TaskSchedulerJobUpdate(
        int id, string jobName, string description, string daysOfWeek, string startTime,
        string status, string programPath, string? arguments,
        string taskFolder, string runAsUser)
    {
        string argsVal = arguments == null ? "NULL" : $"N'{Esc(arguments)}'";
        return $"UPDATE dbo.TaskSchedulerJob SET " +
               $"JobName=N'{Esc(jobName)}', Description=N'{Esc(description)}', " +
               $"DaysOfWeek=N'{Esc(daysOfWeek)}', StartTime=N'{Esc(startTime)}', " +
               $"Status=N'{Esc(status)}', ProgramPath=N'{Esc(programPath)}', " +
               $"Arguments={argsVal}, TaskFolder=N'{Esc(taskFolder)}', RunAsUser=N'{Esc(runAsUser)}', " +
               $"UpdatedAt=SYSDATETIME() WHERE Id={id};";
    }

    public static string TaskSchedulerJobDelete(int id) =>
        $"DELETE FROM dbo.TaskSchedulerJob WHERE Id={id};";

    // Mirrors the INSERT logic of CreatePowerBIDataModelHistory from the PS script.
    // Uses @ExecutionId declared by the caller.
    public const string PowerBIAggregateQuery = @"
        INSERT INTO dbo.PowerBIDataModelHistory
        SELECT
            @ExecutionId,
            GETDATE(),
            e.Department,
            e.Exchange_StorageUsedGB,
            e.Exchange_Item_Count,
            e.Exchange_Deleted_Item_Count,
            e.Exchange_DeletedItemSizeGB,
            o.OneDrive_Total_File_Count,
            o.OneDrive_Total_StorageUsedGB,
            s.SharePoint_Total_File_Count,
            s.SharePoint_Total_StorageUsedGB,
            u.Users_Total
        FROM (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                CAST(SUM(ISNULL([StorageUsedGB], 0))      AS DECIMAL(18,2)) AS Exchange_StorageUsedGB,
                SUM(ISNULL([Item_Count], 0))               AS Exchange_Item_Count,
                SUM(ISNULL([Deleted_Item_Count], 0))       AS Exchange_Deleted_Item_Count,
                CAST(SUM(ISNULL([DeletedItemSizeGB], 0))  AS DECIMAL(18,2)) AS Exchange_DeletedItemSizeGB
            FROM dbo.MicrosoftExchange
            WHERE CountryOrRegion != 'Russia'
            GROUP BY ISNULL(Department, 'Unknown')
        ) e
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                SUM(ISNULL([File_Count], 0))              AS OneDrive_Total_File_Count,
                CAST(SUM(ISNULL([StorageUsedGB], 0)) AS DECIMAL(18,2)) AS OneDrive_Total_StorageUsedGB
            FROM dbo.MicrosoftOneDrive
            WHERE CountryOrRegion != 'Russia'
            GROUP BY ISNULL(Department, 'Unknown')
        ) o ON e.Department = o.Department
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                SUM(ISNULL([File_Count], 0))              AS SharePoint_Total_File_Count,
                CAST(SUM(ISNULL([StorageUsedGB], 0)) AS DECIMAL(18,2)) AS SharePoint_Total_StorageUsedGB
            FROM dbo.MicrosoftSharePoint
            WHERE CountryOrRegion != 'Russia'
            GROUP BY ISNULL(Department, 'Unknown')
        ) s ON e.Department = s.Department
        LEFT JOIN (
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                COUNT(DISTINCT [UserPrincipalName]) AS Users_Total
            FROM dbo.MicrosoftUsers
            WHERE AccountEnabled = 'True'
              AND CountryOrRegion != 'Russia'
            GROUP BY ISNULL(Department, 'Unknown')
        ) u ON e.Department = u.Department;";

    // Dashboard query — returns one aggregated row per month using the LAST SUCCESSFUL execution.
    // Joins PowerBIDataModelHistory with ExecutionLog WHERE Status='SUCCESS' AND ReportName='TOTAL'
    // to ensure only fully successful ETL runs are represented in the dashboard.
    public static string DashboardQuery(string? department, int? yearMonth = null) => $@"
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
            FORMAT(lem.LastDate, 'MMMM', 'en-US')                              AS MonthLabel,
            lem.YearNum,
            lem.MonthNum,
            SUM(h.Exchange_Item_Count)                                          AS Exchange_Item_Count,
            SUM(h.Exchange_Deleted_Item_Count)                                  AS Exchange_Deleted_Item_Count,
            CAST(SUM(h.Exchange_StorageUsedGB)     AS DECIMAL(18,2))            AS Exchange_StorageUsedGB,
            CAST(SUM(h.Exchange_DeletedItemSizeGB) AS DECIMAL(18,2))            AS Exchange_DeletedItemSizeGB,
            SUM(h.OneDrive_Total_File_Count)                                    AS OneDrive_Total_File_Count,
            CAST(SUM(h.OneDrive_Total_StorageUsedGB) AS DECIMAL(18,2))          AS OneDrive_Total_StorageUsedGB,
            SUM(h.SharePoint_Total_File_Count)                                  AS SharePoint_Total_File_Count,
            CAST(SUM(h.SharePoint_Total_StorageUsedGB) AS DECIMAL(18,2))        AS SharePoint_Total_StorageUsedGB,
            SUM(CAST(h.Users_Total AS BIGINT))                                  AS Users_Total
        FROM LastExecPerMonth lem
        JOIN dbo.PowerBIDataModelHistory h ON h.ExecutionId = lem.ExecutionId
        WHERE 1=1
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND h.Department = '{department.Replace("'", "''")}'")}
        {(yearMonth.HasValue ? $"AND (lem.YearNum * 100 + lem.MonthNum) = {yearMonth.Value}" : "")}
        GROUP BY lem.YearNum, lem.MonthNum, lem.LastDate
        ORDER BY lem.YearNum, lem.MonthNum;";

    public static string CountryQuery(string? department) => $@"
        SELECT CountryName, CountryCount
        FROM dbo.PowerBICountryOrRegion
        WHERE CountryCount > 0
        {(string.IsNullOrWhiteSpace(department) ? "" : $"AND Department = '{department.Replace("'", "''")}'")}; ";

    public static string DepartmentListQuery() => @"
        SELECT DISTINCT ISNULL(Department,'Unknown') AS Department
        FROM dbo.PowerBIDataModelHistory
        ORDER BY Department;";

    // Distinct months available in PowerBIDataModelHistory, newest first
    public static string MonthListQuery() => @"
        SELECT DISTINCT
            YEAR([Date]) * 100 + MONTH([Date])        AS YearMonth,
            FORMAT(MAX([Date]), 'MMMM yyyy', 'en-US') AS MonthLabel
        FROM dbo.PowerBIDataModelHistory
        GROUP BY YEAR([Date]), MONTH([Date])
        ORDER BY 1 DESC;";

    // ── EnvironmentConfiguration queries ─────────────────────────────────

    public const string EnvConfigDdl = @"
        IF OBJECT_ID('dbo.EnvironmentConfiguration','U') IS NULL
        CREATE TABLE dbo.EnvironmentConfiguration (
            Id                    INT IDENTITY(1,1) PRIMARY KEY,
            EnvironmentKey        NVARCHAR(100) NOT NULL,
            TenantId              NVARCHAR(255) NOT NULL,
            ClientId              NVARCHAR(255) NOT NULL,
            CertificateThumbprint NVARCHAR(255) NOT NULL,
            CreatedAt             DATETIME2 DEFAULT SYSDATETIME(),
            UpdatedAt             DATETIME2 NULL,
            CONSTRAINT UQ_EnvironmentKey UNIQUE (EnvironmentKey));";

    public const string EnvConfigSelectAll = @"
        SELECT Id, EnvironmentKey, TenantId, ClientId, CertificateThumbprint, CreatedAt, UpdatedAt
        FROM dbo.EnvironmentConfiguration
        ORDER BY EnvironmentKey;";

    public static string EnvConfigInsert(string key, string tenantId, string clientId, string thumb) =>
        $"INSERT INTO dbo.EnvironmentConfiguration (EnvironmentKey, TenantId, ClientId, CertificateThumbprint) " +
        $"VALUES (N'{Esc(key)}', N'{Esc(tenantId)}', N'{Esc(clientId)}', N'{Esc(thumb)}');";

    public static string EnvConfigUpdate(int id, string key, string tenantId, string clientId, string thumb) =>
        $"UPDATE dbo.EnvironmentConfiguration SET " +
        $"EnvironmentKey=N'{Esc(key)}', TenantId=N'{Esc(tenantId)}', ClientId=N'{Esc(clientId)}', " +
        $"CertificateThumbprint=N'{Esc(thumb)}', UpdatedAt=SYSDATETIME() " +
        $"WHERE Id={id};";

    public static string EnvConfigKeyExists(string key, int? excludeId = null) =>
        excludeId.HasValue
            ? $"SELECT COUNT(*) FROM dbo.EnvironmentConfiguration WHERE EnvironmentKey=N'{Esc(key)}' AND Id<>{excludeId.Value};"
            : $"SELECT COUNT(*) FROM dbo.EnvironmentConfiguration WHERE EnvironmentKey=N'{Esc(key)}';";

    private static string Esc(string s) => s.Replace("'", "''");
}
