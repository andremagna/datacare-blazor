using System.Net.Http.Headers;
using System.Text.Json;
using DataCareLite.Models;
using Microsoft.Data.SqlClient;

namespace DataCareLite.Services;

public class EtlService
{
    private readonly EtlStateService _state;
    private readonly HttpClient _http = new();
    private string _token = string.Empty;

    public EtlService(EtlStateService state) => _state = state;

    public async Task RunAsync(RunConfig cfg, CancellationToken ct)
    {
        var executionId = Guid.NewGuid();
        var started = DateTime.UtcNow;

        try
        {
            Log("Verifying SQL Server connection...");
            await using (var c = new SqlConnection(cfg.TargetConnectionString))
                await c.OpenAsync(ct);
            Log("SQL Server connection successful", TerminalLevel.Success);
            _state.SetProgress(4, "SQL connected");

            await InitDatabaseAsync(cfg, ct);
            _state.SetProgress(10, "Database ready");

            Log("Acquiring Microsoft Graph token...");
            await AcquireTokenAsync(cfg, ct);
            Log("Graph token acquired", TerminalLevel.Success);
            _state.SetProgress(16, "Token acquired");

            foreach (var t in new[] { "Exchange", "OneDrive", "SharePoint",
                                       "Users", "PowerBIDataModel", "CountryOrRegion" })
                await DropRecreateIfNeededAsync(cfg, t, ct);
            _state.SetProgress(22, "Tables ready");

            // STEP 1a — Exchange
            Log("STEP 1 — Exchange mailbox usage...", TerminalLevel.Step);
            var exRows = await FetchExchangeAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "Exchange", exRows, ct);
            Log($"Exchange: {exRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(40, "Exchange done");
            await WriteExecLogAsync(cfg, executionId, "Exchange", "SUCCESS",
                exRows.Count, exRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 1b — OneDrive
            Log("STEP 1 — OneDrive usage...", TerminalLevel.Step);
            var odRows = await FetchOneDriveAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "OneDrive", odRows, ct);
            Log($"OneDrive: {odRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(54, "OneDrive done");
            await WriteExecLogAsync(cfg, executionId, "OneDrive", "SUCCESS",
                odRows.Count, odRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 1c — SharePoint
            Log("STEP 1 — SharePoint site usage...", TerminalLevel.Step);
            var spRows = await FetchSharePointAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "SharePoint", spRows, ct);
            Log($"SharePoint: {spRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(68, "SharePoint done");
            await WriteExecLogAsync(cfg, executionId, "SharePoint", "SUCCESS",
                spRows.Count, spRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 2 — Users
            Log("STEP 2 — Azure AD users...", TerminalLevel.Step);
            var usRows = await FetchUsersAsync(ct);
            await BulkInsertMappedAsync(cfg, "Users", usRows, ct);
            Log($"Users: {usRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(80, "Users done");
            await WriteExecLogAsync(cfg, executionId, "Users", "SUCCESS",
                usRows.Count, usRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 3a — PowerBI
            Log("STEP 3 — Building PowerBI data model...", TerminalLevel.Step);
            await BuildPowerBIModelAsync(cfg, ct);
            Log("PowerBI data model populated", TerminalLevel.Success);
            _state.SetProgress(91, "PowerBI model done");

            // STEP 3b — CountryOrRegion
            Log("STEP 3 — Aggregating CountryOrRegion...", TerminalLevel.Step);
            await BuildCountryOrRegionAsync(cfg, ct);
            Log("dbo.CountryOrRegion populated", TerminalLevel.Success);
            _state.SetProgress(97, "CountryOrRegion done");

            int total = exRows.Count + odRows.Count + spRows.Count + usRows.Count;
            int duration = Elapsed(started);
            await WriteExecLogAsync(cfg, executionId, "TOTAL", "SUCCESS",
                total, total, duration, null, ct);
            Log($"=== ETL COMPLETED — {duration}s · {total:N0} rows ===", TerminalLevel.Success);

            var history = await LoadHistoryAsync(cfg, ct);
            _state.CompleteRun(true, history);
        }
        catch (OperationCanceledException)
        {
            Log("ETL cancelled by user", TerminalLevel.Warning);
            await WriteExecLogAsync(cfg, executionId, "TOTAL", "CANCELLED",
                0, 0, Elapsed(started), "Cancelled by user", CancellationToken.None);
            var history = await LoadHistoryAsync(cfg, CancellationToken.None);
            _state.HistoryRows.Clear();
            _state.HistoryRows.AddRange(history);
            _state.SetCancelled();
        }
        catch (Exception ex)
        {
            Log($"ERROR: {ex.Message}", TerminalLevel.Error);
            await WriteExecLogAsync(cfg, executionId, "TOTAL", "FAILED",
                0, 0, Elapsed(started), ex.Message, CancellationToken.None);
            var history = await LoadHistoryAsync(cfg, CancellationToken.None);
            _state.CompleteRun(false, history);
        }
    }

    // ── STEP 1a: Exchange ─────────────────────────────────────────────────────
    // Uses exact Graph CSV column names → ExchangeRow typed mapping
    // Department = real value from Graph (null if not set)
    // Deep stats via EXO PowerShell cmdlets (if available) or Graph report data
    private async Task<List<ExchangeRow>> FetchExchangeAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getMailboxUsageDetail(period='{cfg.Period}')", ct);

        var result = new List<ExchangeRow>();

        foreach (var row in csvRows)
        {
            ct.ThrowIfCancellationRequested();

            var upn = (row.GetValueOrDefault("User Principal Name") ?? "")
                      .Trim().TrimStart('\uFEFF');
            if (string.IsNullOrWhiteSpace(upn)) continue;

            // Read department directly from Graph — real value or null
            string? dept = await GetUserRealDepartmentAsync(upn, ct);

            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));
            long deletedCount = ParseLong(row.GetValueOrDefault("Deleted Item Count"));
            long deletedSize = ParseLong(row.GetValueOrDefault("Deleted Item Size (Byte)"));
            long deletedQuota = ParseLong(row.GetValueOrDefault("Deleted Item Quota (Byte)"));

            var er = new ExchangeRow
            {
                User_Principal_Name = upn,
                Display_Name = row.GetValueOrDefault("Display Name"),
                Department = dept,
                Report_Refresh_Date = GetRefreshDate(row),
                Is_Deleted = row.GetValueOrDefault("Is Deleted"),
                Deleted_Date = row.GetValueOrDefault("Deleted Date"),
                Created_Date = row.GetValueOrDefault("Created Date"),
                Last_Activity_Date = row.GetValueOrDefault("Last Activity Date"),
                Item_Count = ParseLong(row.GetValueOrDefault("Item Count")),
                Storage_Used_Byte = storageBytes,
                StorageUsedGB = storageBytes > 0 ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 2) : 0,
                Issue_Warning_Quota_Byte = ParseLong(row.GetValueOrDefault("Issue Warning Quota (Byte)")),
                Prohibit_Send_Quota_Byte = ParseLong(row.GetValueOrDefault("Prohibit Send Quota (Byte)")),
                Prohibit_Send_Receive_Quota_Byte = ParseLong(row.GetValueOrDefault("Prohibit Send/Receive Quota (Byte)")),
                Deleted_Item_Count = deletedCount,
                Deleted_Item_Size_Byte = deletedSize,
                Deleted_Item_Quota_Byte = deletedQuota,
                Has_Archive = row.GetValueOrDefault("Has Archive"),
                Report_Period = row.GetValueOrDefault("Report Period"),
            };

            // Deep stats via EXO cmdlets (only if PowerShell + EXO module available)
            if (string.Equals(dept, cfg.Department, StringComparison.OrdinalIgnoreCase))
            {
                try { await EnrichWithEXOStatsAsync(er, ct); }
                catch (Exception ex)
                { Log($"  EXO deep stats skipped for {upn}: {ex.Message}", TerminalLevel.Warning); }
            }

            Log($"  Exchange → {upn} [dept={dept ?? "null"}]");
            result.Add(er);
        }

        return result;
    }

    // ── EXO deep stats via PowerShell cmdlets ─────────────────────────────────
    // Mirrors Get-ExchangeMailboxDeepStats from the PS script.
    // Requires ExchangeOnlineManagement module installed and Connect-ExchangeOnline
    // called before. If the module is not available the method throws and the
    // caller catches it gracefully (stats remain at default 0 values).
    private async Task EnrichWithEXOStatsAsync(ExchangeRow er, CancellationToken ct)
    {
        await Task.Run(() =>
        {
            using var ps = System.Management.Automation.PowerShell.Create();

            // Primary mailbox statistics
            ps.AddCommand("Get-EXOMailboxStatistics")
              .AddParameter("Identity", er.User_Principal_Name)
              .AddParameter("Properties", new[] { "ItemCount", "TotalItemSize",
                                                   "SystemMessageCount", "SystemMessageSize" });

            var primaryResult = ps.Invoke();
            ps.Commands.Clear();

            if (primaryResult.Count > 0)
            {
                var obj = primaryResult[0];
                er.Primary_Item_Count = GetPSInt(obj, "ItemCount");
                er.Primary_TotalItemSize = GetPSStr(obj, "TotalItemSize");
                er.Primary_Total_Size_Bytes = ConvertToBytes(er.Primary_TotalItemSize);
                er.Primary_SystemMessage_Count = GetPSInt(obj, "SystemMessageCount");
                er.Primary_SystemMessage_Size_Bytes = ConvertToBytes(GetPSStr(obj, "SystemMessageSize"));
            }

            // Primary recoverable items
            ps.AddCommand("Get-MailboxFolderStatistics")
              .AddParameter("Identity", er.User_Principal_Name)
              .AddParameter("FolderScope", "RecoverableItems");

            var primaryFolders = ps.Invoke();
            ps.Commands.Clear();

            foreach (var folder in primaryFolders)
            {
                if (GetPSStr(folder, "Name") == "Recoverable Items")
                {
                    er.Primary_Recoverable_Count = GetPSInt(folder, "ItemsInFolderAndSubfolders");
                    er.Primary_Recoverable_Size_Bytes = ConvertToBytes(GetPSStr(folder, "FolderAndSubfolderSize"));
                    er.Primary_Recoverable_Mode = "Aggregated";
                    break;
                }
            }

            // Archive mailbox statistics
            ps.AddCommand("Get-EXOMailboxStatistics")
              .AddParameter("Identity", er.User_Principal_Name)
              .AddParameter("Archive", true)
              .AddParameter("Properties", new[] { "ItemCount", "TotalItemSize",
                                                   "SystemMessageCount", "SystemMessageSize" });

            var archiveResult = ps.Invoke();
            ps.Commands.Clear();

            if (archiveResult.Count > 0)
            {
                var obj = archiveResult[0];
                er.Archive_Item_Count = GetPSInt(obj, "ItemCount");
                er.Archive_TotalItemSize = GetPSStr(obj, "TotalItemSize");
                er.Archive_Total_Size_Bytes = ConvertToBytes(er.Archive_TotalItemSize);
                er.Archive_SystemMessage_Count = GetPSInt(obj, "SystemMessageCount");
                er.Archive_SystemMessage_Size_Bytes = ConvertToBytes(GetPSStr(obj, "SystemMessageSize"));

                // Archive recoverable items
                ps.AddCommand("Get-MailboxFolderStatistics")
                  .AddParameter("Identity", er.User_Principal_Name)
                  .AddParameter("Archive", true)
                  .AddParameter("FolderScope", "RecoverableItems");

                var archiveFolders = ps.Invoke();
                ps.Commands.Clear();

                foreach (var folder in archiveFolders)
                {
                    if (GetPSStr(folder, "Name") == "Recoverable Items")
                    {
                        er.Archive_Recoverable_Count = GetPSInt(folder, "ItemsInFolderAndSubfolders");
                        er.Archive_Recoverable_Size_Bytes = ConvertToBytes(GetPSStr(folder, "FolderAndSubfolderSize"));
                        er.Archive_Recoverable_Mode = "Aggregated";
                        break;
                    }
                }
            }
        }, ct);
    }

    // Mirrors Convert-ToBytes from PS script
    private static long ConvertToBytes(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return 0;
        var m = System.Text.RegularExpressions.Regex.Match(s, @"\((\d[\d,]*) bytes\)");
        if (m.Success) return long.Parse(m.Groups[1].Value.Replace(",", ""));
        m = System.Text.RegularExpressions.Regex.Match(s, @"([\d,.]+)\s*GB");
        if (m.Success) return (long)(double.Parse(m.Groups[1].Value,
            System.Globalization.CultureInfo.InvariantCulture) * 1_073_741_824);
        m = System.Text.RegularExpressions.Regex.Match(s, @"([\d,.]+)\s*MB");
        if (m.Success) return (long)(double.Parse(m.Groups[1].Value,
            System.Globalization.CultureInfo.InvariantCulture) * 1_048_576);
        m = System.Text.RegularExpressions.Regex.Match(s, @"([\d,.]+)\s*KB");
        if (m.Success) return (long)(double.Parse(m.Groups[1].Value,
            System.Globalization.CultureInfo.InvariantCulture) * 1_024);
        return 0;
    }

    private static int GetPSInt(System.Management.Automation.PSObject o, string p) =>
        o.Properties[p]?.Value is int i ? i : 0;
    private static string GetPSStr(System.Management.Automation.PSObject o, string p) =>
        o.Properties[p]?.Value?.ToString() ?? "";

    // ── STEP 1b: OneDrive — real department or null, correct field names ───────
    private async Task<List<OneDriveRow>> FetchOneDriveAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='{cfg.Period}')", ct);

        var result = new List<OneDriveRow>();

        foreach (var row in csvRows)
        {
            ct.ThrowIfCancellationRequested();

            var upn = (row.GetValueOrDefault("Owner Principal Name") ?? "")
                      .Trim().TrimStart('\uFEFF');
            if (string.IsNullOrWhiteSpace(upn)) continue;

            string? dept = await GetUserRealDepartmentAsync(upn, ct);
            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));
            double storageGB = storageBytes > 0
                ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 2) : 0;

            result.Add(new OneDriveRow
            {
                Report_Refresh_Date = GetRefreshDate(row),
                Site_Id = row.GetValueOrDefault("Site Id"),
                Site_URL = row.GetValueOrDefault("Site URL"),
                Owner_Display_Name = row.GetValueOrDefault("Owner Display Name"),
                Is_Deleted = row.GetValueOrDefault("Is Deleted"),
                Last_Activity_Date = row.GetValueOrDefault("Last Activity Date"),
                // Exact CSV column name: "File Count" → int
                File_Count = (int)ParseLong(row.GetValueOrDefault("File Count")),
                // Exact CSV column name: "Active File Count"
                Active_File_Count = (int)ParseLong(row.GetValueOrDefault("Active File Count")),
                Storage_Used_Byte = storageBytes,
                StorageUsedGB = storageGB,
                Storage_Allocated_Byte = ParseLong(row.GetValueOrDefault("Storage Allocated (Byte)")),
                Owner_Principal_Name = upn,
                Department = dept,
                Report_Period = row.GetValueOrDefault("Report Period"),
            });

            Log($"  OneDrive → {upn} [dept={dept ?? "null"}] {storageGB:F2} GB");
        }

        return result;
    }

    // ── STEP 1c: SharePoint ───────────────────────────────────────────────────
    private async Task<List<SharePointRow>> FetchSharePointAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getSharePointSiteUsageDetail(period='{cfg.Period}')", ct);

        var result = new List<SharePointRow>();

        foreach (var row in csvRows)
        {
            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));
            double storageGB = storageBytes > 0
                ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 2) : 0;

            var ownerUpn = (row.GetValueOrDefault("Owner Principal Name") ?? "").Trim();
            if (string.IsNullOrWhiteSpace(ownerUpn)) ownerUpn = "N/A";

            result.Add(new SharePointRow
            {
                Report_Refresh_Date = GetRefreshDate(row),
                Site_Id = row.GetValueOrDefault("Site Id"),
                Site_URL = row.GetValueOrDefault("Site URL"),
                Owner_Display_Name = row.GetValueOrDefault("Owner Display Name"),
                Is_Deleted = row.GetValueOrDefault("Is Deleted"),
                Last_Activity_Date = row.GetValueOrDefault("Last Activity Date"),
                File_Count = (int)ParseLong(row.GetValueOrDefault("File Count")),
                Active_File_Count = (int)ParseLong(row.GetValueOrDefault("Active File Count")),
                Page_View_Count = (int)ParseLong(row.GetValueOrDefault("Page View Count")),
                Visited_Page_Count = (int)ParseLong(row.GetValueOrDefault("Visited Page Count")),
                Storage_Used_Byte = storageBytes,
                StorageUsedGB = storageGB,
                Storage_Allocated_Byte = ParseLong(row.GetValueOrDefault("Storage Allocated (Byte)")),
                Root_Web_Template = row.GetValueOrDefault("Root Web Template"),
                Owner_Principal_Name = ownerUpn,
                Report_Period = row.GetValueOrDefault("Report Period"),
            });
        }

        Log($"  SharePoint: {result.Count} rows");
        return result;
    }

    // ── STEP 2: Users ─────────────────────────────────────────────────────────
    private async Task<List<UserRow>> FetchUsersAsync(CancellationToken ct)
    {
        var all = new List<UserRow>();
        string? next = "https://graph.microsoft.com/v1.0/users" +
                       "?$select=id,displayName,userPrincipalName,mail," +
                       "department,jobTitle,accountEnabled,createdDateTime,country";

        while (next != null)
        {
            var req = new HttpRequestMessage(HttpMethod.Get, next);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            req.Headers.Add("ConsistencyLevel", "eventual");
            var resp = await _http.SendAsync(req, ct);
            resp.EnsureSuccessStatusCode();

            var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
            if (doc.RootElement.TryGetProperty("value", out var arr))
                foreach (var u in arr.EnumerateArray())
                    all.Add(new UserRow
                    {
                        Id = Str(u, "id"),
                        DisplayName = Str(u, "displayName"),
                        UserPrincipalName = Str(u, "userPrincipalName"),
                        Mail = Str(u, "mail"),
                        Department = NullIfEmpty(Str(u, "department")),
                        JobTitle = NullIfEmpty(Str(u, "jobTitle")),
                        AccountEnabled = u.TryGetProperty("accountEnabled", out var ae)
                                           ? ae.GetBoolean().ToString() : null,
                        CreatedDateTime = NullIfEmpty(Str(u, "createdDateTime")),
                        CountryOrRegion = NullIfEmpty(Str(u, "country")),
                    });

            next = doc.RootElement.TryGetProperty("@odata.nextLink", out var nl)
                   ? nl.GetString() : null;
        }

        Log($"  Users: {all.Count} records");
        return all;
    }

    // ── Department lookup — returns real value or null ────────────────────────
    private async Task<string?> GetUserRealDepartmentAsync(string upn, CancellationToken ct)
    {
        try
        {
            var req = new HttpRequestMessage(HttpMethod.Get,
                $"https://graph.microsoft.com/v1.0/users/{Uri.EscapeDataString(upn)}?$select=department");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);

            var resp = await _http.SendAsync(req, ct);
            if (!resp.IsSuccessStatusCode) return null;

            var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
            var dept = doc.RootElement.TryGetProperty("department", out var d) ? d.GetString() : null;
            return NullIfEmpty(dept);
        }
        catch { return null; }
    }

    // ── Bulk insert — typed rows → SQL via DataTable ──────────────────────────
    private async Task BulkInsertMappedAsync<T>(RunConfig cfg, string table,
        List<T> rows, CancellationToken ct) where T : IRowMappable
    {
        if (rows.Count == 0) return;

        var cols = await GetColumnsAsync(cfg, table, ct);
        var dt = new System.Data.DataTable();
        foreach (var c in cols) dt.Columns.Add(c);

        foreach (var row in rows)
        {
            var dr = dt.NewRow();
            var dict = row.ToColumnDictionary();

            foreach (var col in cols)
            {
                var nl = Normalize(col);
                if (nl == "insertedat") { dr[col] = DateTime.Now; continue; }
                if (nl == "sourcereport") { dr[col] = table; continue; }
                if (nl == "reportdate") { dr[col] = DateTime.Now; continue; }

                if (dict.TryGetValue(col, out var val))
                    dr[col] = val ?? (object)System.DBNull.Value;
                else
                    dr[col] = System.DBNull.Value;
            }
            dt.Rows.Add(dr);
        }

        await using var conn = new SqlConnection(cfg.TargetConnectionString);
        await conn.OpenAsync(ct);
        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = $"dbo.{table}",
            BatchSize = 5000,
            BulkCopyTimeout = 0
        };
        foreach (var c in cols) bulk.ColumnMappings.Add(c, c);
        await bulk.WriteToServerAsync(dt, ct);
    }

    // ── PowerBI model ─────────────────────────────────────────────────────────
    private async Task BuildPowerBIModelAsync(RunConfig cfg, CancellationToken ct)
    {
        string date = DateTime.Now.ToString("yyyy_MM_dd");
        string backup = $"dbo.PowerBIDataModelBackup_{date}";

        string backupDdl = $@"
            IF OBJECT_ID('{backup}','U') IS NULL
            CREATE TABLE {backup} (
                Exchange_Total_Primary_Item_Count               INT,
                Exchange_Total_Archive_Item_Count               INT,
                Exchange_Total_Primary_Total_Size_GB            DECIMAL(18,2),
                Exchange_Total_Archive_Total_Size_GB            DECIMAL(18,2),
                Exchange_Total_Primary_Total_Size_Bytes         BIGINT,
                Exchange_Total_Primary_SystemMessage_Count      INT,
                Exchange_Total_Primary_SystemMessage_Size_Bytes BIGINT,
                Exchange_Total_Primary_Recoverable_Count        INT,
                Exchange_Total_Primary_Recoverable_Size_Bytes   BIGINT,
                Exchange_Total_Archive_Total_Size_Bytes         BIGINT,
                Exchange_Total_Archive_SystemMessage_Count      INT,
                Exchange_Total_Archive_SystemMessage_Size_Bytes BIGINT,
                Exchange_Total_Archive_Recoverable_Count        INT,
                Exchange_Total_Archive_Recoverable_Size_Bytes   BIGINT,
                OneDrive_Total_File_Count                       INT,
                OneDrive_Total_StorageUsedGB                    FLOAT,
                SharePoint_Total_File_Count                     INT,
                SharePoint_Total_StorageUsedGB                  FLOAT,
                Users_Total                                     INT);";

        await ExecAsync(cfg.TargetConnectionString, backupDdl, ct);
        await ExecAsync(cfg.TargetConnectionString,
            $"INSERT INTO {backup} {TableDdl.PowerBIAggregateQuery};", ct);
        await ExecAsync(cfg.TargetConnectionString,
            $"INSERT INTO dbo.PowerBIDataModel {TableDdl.PowerBIAggregateQuery};", ct);
        Log($"  Backup {backup} populated", TerminalLevel.Success);
    }

    private async Task BuildCountryOrRegionAsync(RunConfig cfg, CancellationToken ct) =>
        await ExecAsync(cfg.TargetConnectionString, @"
            INSERT INTO dbo.CountryOrRegion (CountryName, CountryCount)
            SELECT CountryOrRegion, COUNT(*)
            FROM dbo.Users
            WHERE CountryOrRegion IS NOT NULL AND CountryOrRegion <> ''
            GROUP BY CountryOrRegion
            ORDER BY COUNT(*) DESC;", ct);

    // ── SQL init ──────────────────────────────────────────────────────────────
    private async Task InitDatabaseAsync(RunConfig cfg, CancellationToken ct)
    {
        Log($"Initializing database [{cfg.SqlDatabase}]...");
        await ExecAsync(cfg.MasterConnectionString,
            $"IF DB_ID(N'{cfg.SqlDatabase}') IS NULL CREATE DATABASE [{cfg.SqlDatabase}];", ct);
        foreach (var (name, ddl) in TableDdl.All)
        {
            await ExecAsync(cfg.TargetConnectionString, ddl, ct);
            Log($"  Table '{name}' verified", TerminalLevel.Success);
        }
        Log("Database initialization completed", TerminalLevel.Success);
    }

    private async Task DropRecreateIfNeededAsync(RunConfig cfg, string table, CancellationToken ct)
    {
        int count = 0;
        try
        {
            count = await ScalarAsync<int>(cfg.TargetConnectionString,
                  $"SELECT COUNT(*) FROM dbo.{table}", ct);
        }
        catch { return; }
        if (count == 0) return;

        Log($"Dropping dbo.{table} ({count} rows)...", TerminalLevel.Warning);
        await ExecAsync(cfg.TargetConnectionString,
            $"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DROP TABLE dbo.{table};", ct);
        if (TableDdl.All.TryGetValue(table, out var ddl))
            await ExecAsync(cfg.TargetConnectionString, ddl, ct);
        Log($"  dbo.{table} recreated", TerminalLevel.Success);
    }

    private async Task WriteExecLogAsync(RunConfig cfg, Guid execId,
        string report, string status, int retrieved, int inserted,
        int duration, string? error, CancellationToken ct)
    {
        string safeErr = error != null ? $"'{error.Replace("'", "''")}'" : "NULL";
        await ExecAsync(cfg.TargetConnectionString, $@"
            INSERT INTO dbo.ExecutionLog
            (ExecutionId,ExecutionDate,ReportName,Status,
             RowsRetrieved,RowsInserted,DurationSeconds,
             ErrorMessage,MachineName,AppVersion)
            VALUES('{execId}',SYSDATETIME(),'{report}','{status}',
                   {retrieved},{inserted},{duration},
                   {safeErr},'{Environment.MachineName}','1.0.0')", ct);
    }

    private async Task<List<ExecutionLogEntry>> LoadHistoryAsync(RunConfig cfg, CancellationToken ct)
    {
        var list = new List<ExecutionLogEntry>();
        try
        {
            await using var conn = new SqlConnection(cfg.TargetConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(@"
                SELECT TOP 50 ExecutionDate,ReportName,Status,
                    RowsRetrieved,RowsInserted,DurationSeconds,ErrorMessage
                FROM dbo.ExecutionLog ORDER BY ExecutionDate DESC", conn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
                list.Add(new ExecutionLogEntry
                {
                    ExecutionDate = rdr.GetDateTime(0),
                    ReportName = rdr.GetString(1),
                    Status = rdr.GetString(2),
                    RowsRetrieved = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                    RowsInserted = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4),
                    DurationSeconds = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5),
                    ErrorMessage = rdr.IsDBNull(6) ? null : rdr.GetString(6),
                });
        }
        catch { }
        return list;
    }

    // ── Graph auth ────────────────────────────────────────────────────────────
    private async Task AcquireTokenAsync(RunConfig cfg, CancellationToken ct)
    {
        var body = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string,string>("client_id",     cfg.ClientId),
            new KeyValuePair<string,string>("client_secret", cfg.ClientSecret),
            new KeyValuePair<string,string>("scope",         "https://graph.microsoft.com/.default"),
            new KeyValuePair<string,string>("grant_type",    "client_credentials"),
        });
        var resp = await _http.PostAsync(
            $"https://login.microsoftonline.com/{cfg.TenantId}/oauth2/v2.0/token", body, ct);
        resp.EnsureSuccessStatusCode();
        var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        _token = doc.RootElement.GetProperty("access_token").GetString()!;
    }

    // ── CSV fetch ─────────────────────────────────────────────────────────────
    private async Task<List<Dictionary<string, string?>>> FetchCsvAsync(string url, CancellationToken ct)
    {
        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
        req.Headers.Accept.ParseAdd("text/csv");
        var resp = await _http.SendAsync(req, ct);
        resp.EnsureSuccessStatusCode();
        return ParseCsv(await resp.Content.ReadAsStringAsync(ct));
    }

    private static List<Dictionary<string, string?>> ParseCsv(string csv)
    {
        var result = new List<Dictionary<string, string?>>();
        var lines = csv.TrimStart('\uFEFF').Split('\n', StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length < 2) return result;
        var headers = SplitLine(lines[0]);
        for (int i = 1; i < lines.Length; i++)
        {
            var vals = SplitLine(lines[i]);
            var dict = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            for (int j = 0; j < headers.Count && j < vals.Count; j++)
                dict[headers[j]] = string.IsNullOrEmpty(vals[j]) ? null : vals[j];
            result.Add(dict);
        }
        return result;
    }

    private static List<string> SplitLine(string line)
    {
        var parts = new List<string>();
        bool inQ = false;
        var cur = new System.Text.StringBuilder();
        foreach (char c in line)
        {
            if (c == '"') inQ = !inQ;
            else if (c == ',' && !inQ) { parts.Add(cur.ToString().Trim()); cur.Clear(); }
            else cur.Append(c);
        }
        parts.Add(cur.ToString().Trim());
        return parts;
    }

    // ── SQL helpers ───────────────────────────────────────────────────────────
    private async Task<List<string>> GetColumnsAsync(RunConfig cfg, string table, CancellationToken ct)
    {
        var cols = new List<string>();
        await using var conn = new SqlConnection(cfg.TargetConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(
            $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}'", conn);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct)) cols.Add(rdr.GetString(0));
        return cols;
    }

    private async Task ExecAsync(string connStr, string sql, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 180 };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<T> ScalarAsync<T>(string connStr, string sql, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn);
        var res = await cmd.ExecuteScalarAsync(ct);
        return (T)Convert.ChangeType(res ?? 0, typeof(T));
    }

    // ── Utilities ─────────────────────────────────────────────────────────────
    private static string GetRefreshDate(Dictionary<string, string?> row) =>
        row.FirstOrDefault(k => k.Key.Contains("Report Refresh Date",
            StringComparison.OrdinalIgnoreCase)).Value ?? "";

    private static long ParseLong(string? s) =>
        long.TryParse(s, out var v) ? v : 0;

    private static string Normalize(string s) =>
        new string(s.ToLower().Where(char.IsLetterOrDigit).ToArray());

    private static string Str(JsonElement el, string key) =>
        el.TryGetProperty(key, out var p) ? p.GetString() ?? "" : "";

    private static string? NullIfEmpty(string? s) =>
        string.IsNullOrWhiteSpace(s) ? null : s;

    private static int Elapsed(DateTime started) =>
        (int)(DateTime.UtcNow - started).TotalSeconds;

    private void Log(string msg, TerminalLevel lvl = TerminalLevel.Info) =>
        _state.AppendLine(msg, lvl);
}

// ── Typed row models + interface ─────────────────────────────────────────────
public interface IRowMappable
{
    // Returns exact SQL column name → value (null = DBNull)
    Dictionary<string, object?> ToColumnDictionary();
}

public class ExchangeRow : IRowMappable
{
    public string? User_Principal_Name { get; set; }
    public string? Display_Name { get; set; }
    public string? Department { get; set; }
    public string? Report_Refresh_Date { get; set; }
    public string? Is_Deleted { get; set; }
    public string? Deleted_Date { get; set; }
    public string? Created_Date { get; set; }
    public string? Last_Activity_Date { get; set; }
    public long Item_Count { get; set; }
    public long Storage_Used_Byte { get; set; }
    public double StorageUsedGB { get; set; }
    public long Issue_Warning_Quota_Byte { get; set; }
    public long Prohibit_Send_Quota_Byte { get; set; }
    public long Prohibit_Send_Receive_Quota_Byte { get; set; }
    public long Deleted_Item_Count { get; set; }
    public long Deleted_Item_Size_Byte { get; set; }
    public long Deleted_Item_Quota_Byte { get; set; }
    public string? Has_Archive { get; set; }
    public string? Report_Period { get; set; }
    public int Primary_Item_Count { get; set; }
    public string? Primary_TotalItemSize { get; set; }
    public long Primary_Total_Size_Bytes { get; set; }
    public int Primary_SystemMessage_Count { get; set; }
    public long Primary_SystemMessage_Size_Bytes { get; set; }
    public int Primary_Recoverable_Count { get; set; }
    public long Primary_Recoverable_Size_Bytes { get; set; }
    public string Primary_Recoverable_Mode { get; set; } = "NotPresent";
    public int Archive_Item_Count { get; set; }
    public string? Archive_TotalItemSize { get; set; }
    public long Archive_Total_Size_Bytes { get; set; }
    public int Archive_SystemMessage_Count { get; set; }
    public long Archive_SystemMessage_Size_Bytes { get; set; }
    public int Archive_Recoverable_Count { get; set; }
    public long Archive_Recoverable_Size_Bytes { get; set; }
    public string Archive_Recoverable_Mode { get; set; } = "NotPresent";

    public Dictionary<string, object?> ToColumnDictionary() => new()
    {
        ["User_Principal_Name"] = User_Principal_Name,
        ["Display_Name"] = Display_Name,
        ["Department"] = Department,
        ["___Report_Refresh_Date"] = Report_Refresh_Date,
        ["Is_Deleted"] = Is_Deleted,
        ["Deleted_Date"] = Deleted_Date,
        ["Created_Date"] = Created_Date,
        ["Last_Activity_Date"] = Last_Activity_Date,
        ["Item_Count"] = Item_Count,
        ["Storage_Used__Byte_"] = Storage_Used_Byte,
        ["StorageUsedGB"] = StorageUsedGB,
        ["Issue_Warning_Quota__Byte_"] = Issue_Warning_Quota_Byte,
        ["Prohibit_Send_Quota__Byte_"] = Prohibit_Send_Quota_Byte,
        ["Prohibit_Send_Receive_Quota__Byte_"] = Prohibit_Send_Receive_Quota_Byte,
        ["Deleted_Item_Count"] = Deleted_Item_Count,
        ["Deleted_Item_Size__Byte_"] = Deleted_Item_Size_Byte,
        ["Deleted_Item_Quota__Byte_"] = Deleted_Item_Quota_Byte,
        ["Has_Archive"] = Has_Archive,
        ["Report_Period"] = Report_Period,
        ["Primary_Item_Count"] = Primary_Item_Count,
        ["Primary_TotalItemSize"] = Primary_TotalItemSize,
        ["Primary_Total_Size_Bytes"] = Primary_Total_Size_Bytes,
        ["Primary_SystemMessage_Count"] = Primary_SystemMessage_Count,
        ["Primary_SystemMessage_Size_Bytes"] = Primary_SystemMessage_Size_Bytes,
        ["Primary_Recoverable_Count"] = Primary_Recoverable_Count,
        ["Primary_Recoverable_Size_Bytes"] = Primary_Recoverable_Size_Bytes,
        ["Primary_Recoverable_Mode"] = Primary_Recoverable_Mode,
        ["Archive_Item_Count"] = Archive_Item_Count,
        ["Archive_TotalItemSize"] = Archive_TotalItemSize,
        ["Archive_Total_Size_Bytes"] = Archive_Total_Size_Bytes,
        ["Archive_SystemMessage_Count"] = Archive_SystemMessage_Count,
        ["Archive_SystemMessage_Size_Bytes"] = Archive_SystemMessage_Size_Bytes,
        ["Archive_Recoverable_Count"] = Archive_Recoverable_Count,
        ["Archive_Recoverable_Size_Bytes"] = Archive_Recoverable_Size_Bytes,
        ["Archive_Recoverable_Mode"] = Archive_Recoverable_Mode,
    };
}

public class OneDriveRow : IRowMappable
{
    public string? Report_Refresh_Date { get; set; }
    public string? Site_Id { get; set; }
    public string? Site_URL { get; set; }
    public string? Owner_Display_Name { get; set; }
    public string? Is_Deleted { get; set; }
    public string? Last_Activity_Date { get; set; }
    public int File_Count { get; set; }
    public int Active_File_Count { get; set; }
    public long Storage_Used_Byte { get; set; }
    public double StorageUsedGB { get; set; }
    public long Storage_Allocated_Byte { get; set; }
    public string Owner_Principal_Name { get; set; } = "";
    public string? Department { get; set; }
    public string? Report_Period { get; set; }

    public Dictionary<string, object?> ToColumnDictionary() => new()
    {
        ["___Report_Refresh_Date"] = Report_Refresh_Date,
        ["Site_Id"] = Site_Id,
        ["Site_URL"] = Site_URL,
        ["Owner_Display_Name"] = Owner_Display_Name,
        ["Is_Deleted"] = Is_Deleted,
        ["Last_Activity_Date"] = Last_Activity_Date,
        ["File_Count"] = File_Count,
        ["Active_File_Count"] = Active_File_Count,
        ["Storage_Used__Byte_"] = Storage_Used_Byte,
        ["StorageUsedGB"] = StorageUsedGB,
        ["Storage_Allocated__Byte_"] = Storage_Allocated_Byte,
        ["Owner_Principal_Name"] = Owner_Principal_Name,
        ["Department"] = Department,
        ["Report_Period"] = Report_Period,
    };
}

public class SharePointRow : IRowMappable
{
    public string? Report_Refresh_Date { get; set; }
    public string? Site_Id { get; set; }
    public string? Site_URL { get; set; }
    public string? Owner_Display_Name { get; set; }
    public string? Is_Deleted { get; set; }
    public string? Last_Activity_Date { get; set; }
    public int File_Count { get; set; }
    public int Active_File_Count { get; set; }
    public int Page_View_Count { get; set; }
    public int Visited_Page_Count { get; set; }
    public long Storage_Used_Byte { get; set; }
    public double StorageUsedGB { get; set; }
    public long Storage_Allocated_Byte { get; set; }
    public string? Root_Web_Template { get; set; }
    public string Owner_Principal_Name { get; set; } = "N/A";
    public string? Report_Period { get; set; }

    public Dictionary<string, object?> ToColumnDictionary() => new()
    {
        ["___Report_Refresh_Date"] = Report_Refresh_Date,
        ["Site_Id"] = Site_Id,
        ["Site_URL"] = Site_URL,
        ["Owner_Display_Name"] = Owner_Display_Name,
        ["Is_Deleted"] = Is_Deleted,
        ["Last_Activity_Date"] = Last_Activity_Date,
        ["File_Count"] = File_Count,
        ["Active_File_Count"] = Active_File_Count,
        ["Page_View_Count"] = Page_View_Count,
        ["Visited_Page_Count"] = Visited_Page_Count,
        ["Storage_Used__Byte_"] = Storage_Used_Byte,
        ["StorageUsedGB"] = StorageUsedGB,
        ["Storage_Allocated__Byte_"] = Storage_Allocated_Byte,
        ["Root_Web_Template"] = Root_Web_Template,
        ["Owner_Principal_Name"] = Owner_Principal_Name,
        ["Report_Period"] = Report_Period,
    };
}

public class UserRow : IRowMappable
{
    public string? Id { get; set; }
    public string? DisplayName { get; set; }
    public string? UserPrincipalName { get; set; }
    public string? Mail { get; set; }
    public string? Department { get; set; }
    public string? JobTitle { get; set; }
    public string? AccountEnabled { get; set; }
    public string? CreatedDateTime { get; set; }
    public string? CountryOrRegion { get; set; }

    public Dictionary<string, object?> ToColumnDictionary() => new()
    {
        ["Id"] = Id,
        ["DisplayName"] = DisplayName,
        ["UserPrincipalName"] = UserPrincipalName,
        ["Mail"] = Mail,
        ["Department"] = Department,
        ["JobTitle"] = JobTitle,
        ["AccountEnabled"] = AccountEnabled,
        ["CreatedDateTime"] = CreatedDateTime,
        ["CountryOrRegion"] = CountryOrRegion,
    };
}
