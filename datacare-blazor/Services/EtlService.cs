using System.Net.Http.Headers;
using System.Text.Json;
using DataCare.Models;
using Microsoft.Data.SqlClient;

namespace DataCare.Services;

public class EtlService
{
    private readonly EtlStateService _state;
    private readonly HttpClient _http = new();
    private string _token = string.Empty;

    private const int BatchSize = 200;

    public EtlService(EtlStateService state) => _state = state;

    public async Task RunAsync(RunConfig cfg, CancellationToken ct)
    {
        var executionId = Guid.NewGuid();
        var started = DateTime.UtcNow;

        try
        {
            Log("Verifying SQL Server connection...");
            // Connect to master — target DB may not exist yet
            await using (var c = new SqlConnection(cfg.MasterConnectionString))
                await c.OpenAsync(ct);
            Log("SQL Server connection successful", TerminalLevel.Success);
            _state.SetProgress(4, "SQL connected");

            await InitDatabaseAsync(cfg, ct);
            _state.SetProgress(10, "Database ready");

            Log("Acquiring Microsoft Graph token...");
            await AcquireTokenAsync(cfg, ct);
            Log("Graph token acquired", TerminalLevel.Success);
            _state.SetProgress(16, "Token acquired");

            // PowerBIDataModelHistory is append-only (full history) — never drop it.
            // PowerBICountryOrRegion is rebuilt fresh each run via INSERT — drop only the transient tables.
            foreach (var t in new[] { "MicrosoftExchange", "MicrosoftOneDrive", "MicrosoftSharePoint",
                                       "MicrosoftUsers", "PowerBICountryOrRegion" })
                await DropRecreateIfNeededAsync(cfg, t, ct);
            _state.SetProgress(22, "Tables ready");

            // STEP 1a — Exchange
            Log("STEP 1 — Exchange mailbox usage...", TerminalLevel.Step);
            var exRows = await FetchExchangeAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "MicrosoftExchange", exRows, ct);
            Log($"Exchange: {exRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(40, "Exchange done");
            await WriteExecLogAsync(cfg, executionId, "MicrosoftExchange", "SUCCESS",
                exRows.Count, exRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 1b — OneDrive
            Log("STEP 1 — OneDrive usage...", TerminalLevel.Step);
            var odRows = await FetchOneDriveAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "MicrosoftOneDrive", odRows, ct);
            Log($"OneDrive: {odRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(54, "OneDrive done");
            await WriteExecLogAsync(cfg, executionId, "MicrosoftOneDrive", "SUCCESS",
                odRows.Count, odRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 1c — SharePoint
            Log("STEP 1 — SharePoint site usage...", TerminalLevel.Step);
            var spRows = await FetchSharePointAsync(cfg, ct);
            await BulkInsertMappedAsync(cfg, "MicrosoftSharePoint", spRows, ct);
            Log($"SharePoint: {spRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(68, "SharePoint done");
            await WriteExecLogAsync(cfg, executionId, "MicrosoftSharePoint", "SUCCESS",
                spRows.Count, spRows.Count, Elapsed(started), null, ct);
            ct.ThrowIfCancellationRequested();

            // STEP 2 — Users
            Log("STEP 2 — Azure AD users...", TerminalLevel.Step);
            var usRows = await FetchUsersAsync(ct);
            await BulkInsertMappedAsync(cfg, "MicrosoftUsers", usRows, ct);
            Log($"Users: {usRows.Count} rows inserted", TerminalLevel.Success);
            _state.SetProgress(80, "Users done");
            await WriteExecLogAsync(cfg, executionId, "MicrosoftUsers", "SUCCESS",
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
    // Fetches Graph CSV report, resolves department + country per-user via Graph,
    // logs progress every BatchSize records.
    private async Task<List<ExchangeRow>> FetchExchangeAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getMailboxUsageDetail(period='{cfg.Period}')", ct);

        var result = new List<ExchangeRow>();
        int processed = 0;

        foreach (var row in csvRows)
        {
            ct.ThrowIfCancellationRequested();

            var upn = (row.GetValueOrDefault("User Principal Name") ?? "")
                      .Trim().TrimStart('\uFEFF');
            if (string.IsNullOrWhiteSpace(upn)) continue;

            // Resolve department AND country in a single Graph call — mirrors PS: ?$select=department,country
            var (dept, country) = await GetUserDepartmentAndCountryAsync(upn, ct);

            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));
            long deletedSize = ParseLong(row.GetValueOrDefault("Deleted Item Size (Byte)"));

            result.Add(new ExchangeRow
            {
                User_Principal_Name = upn,
                Display_Name = row.GetValueOrDefault("Display Name"),
                Department = dept,
                CountryOrRegion = country,
                Report_Refresh_Date = GetRefreshDate(row),
                Is_Deleted = row.GetValueOrDefault("Is Deleted"),
                Deleted_Date = row.GetValueOrDefault("Deleted Date"),
                Created_Date = row.GetValueOrDefault("Created Date"),
                Last_Activity_Date = row.GetValueOrDefault("Last Activity Date"),
                Item_Count = ParseLong(row.GetValueOrDefault("Item Count")),
                Storage_Used_Byte = storageBytes,
                StorageUsedGB = storageBytes > 0 ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 5) : 0,
                Issue_Warning_Quota_Byte = ParseLong(row.GetValueOrDefault("Issue Warning Quota (Byte)")),
                Prohibit_Send_Quota_Byte = ParseLong(row.GetValueOrDefault("Prohibit Send Quota (Byte)")),
                Prohibit_Send_Receive_Quota_Byte = ParseLong(row.GetValueOrDefault("Prohibit Send/Receive Quota (Byte)")),
                Deleted_Item_Count = ParseLong(row.GetValueOrDefault("Deleted Item Count")),
                Deleted_Item_Size_Byte = deletedSize,
                DeletedItemSizeGB = deletedSize > 0 ? Math.Round(deletedSize / (double)(1024L * 1024 * 1024), 5) : 0,
                Deleted_Item_Quota_Byte = ParseLong(row.GetValueOrDefault("Deleted Item Quota (Byte)")),
                Has_Archive = row.GetValueOrDefault("Has Archive"),
                Report_Period = row.GetValueOrDefault("Report Period"),
            });

            processed++;
            if (processed % BatchSize == 0)
                Log($"  Exchange → batch {processed / BatchSize} ({processed} records processed)...", TerminalLevel.Info);
        }

        Log($"  Exchange: {result.Count} rows fetched", TerminalLevel.Info);
        return result;
    }

    // ── STEP 1b: OneDrive ─────────────────────────────────────────────────────
    private async Task<List<OneDriveRow>> FetchOneDriveAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='{cfg.Period}')", ct);

        var result = new List<OneDriveRow>();
        int processed = 0;

        foreach (var row in csvRows)
        {
            ct.ThrowIfCancellationRequested();

            var upn = (row.GetValueOrDefault("Owner Principal Name") ?? "")
                      .Trim().TrimStart('\uFEFF');
            if (string.IsNullOrWhiteSpace(upn)) continue;

            var (dept, country) = await GetUserDepartmentAndCountryAsync(upn, ct);

            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));

            result.Add(new OneDriveRow
            {
                Report_Refresh_Date = GetRefreshDate(row),
                Site_Id = row.GetValueOrDefault("Site Id"),
                Site_URL = row.GetValueOrDefault("Site URL"),
                Owner_Display_Name = row.GetValueOrDefault("Owner Display Name"),
                Is_Deleted = row.GetValueOrDefault("Is Deleted"),
                Last_Activity_Date = row.GetValueOrDefault("Last Activity Date"),
                File_Count = (int)ParseLong(row.GetValueOrDefault("File Count")),
                Active_File_Count = (int)ParseLong(row.GetValueOrDefault("Active File Count")),
                Storage_Used_Byte = storageBytes,
                StorageUsedGB = storageBytes > 0 ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 5) : 0,
                Storage_Allocated_Byte = ParseLong(row.GetValueOrDefault("Storage Allocated (Byte)")),
                Owner_Principal_Name = upn,
                Department = dept,
                CountryOrRegion = country,
                Report_Period = row.GetValueOrDefault("Report Period"),
            });

            processed++;
            if (processed % BatchSize == 0)
                Log($"  OneDrive → batch {processed / BatchSize} ({processed} records processed)...", TerminalLevel.Info);
        }

        Log($"  OneDrive: {result.Count} rows fetched", TerminalLevel.Info);
        return result;
    }

    // ── STEP 1c: SharePoint ───────────────────────────────────────────────────
    private async Task<List<SharePointRow>> FetchSharePointAsync(RunConfig cfg, CancellationToken ct)
    {
        var csvRows = await FetchCsvAsync(
            $"https://graph.microsoft.com/v1.0/reports/getSharePointSiteUsageDetail(period='{cfg.Period}')", ct);

        var result = new List<SharePointRow>();
        int processed = 0;

        foreach (var row in csvRows)
        {
            ct.ThrowIfCancellationRequested();

            long storageBytes = ParseLong(row.GetValueOrDefault("Storage Used (Byte)"));

            var ownerUpn = (row.GetValueOrDefault("Owner Principal Name") ?? "").Trim();
            if (string.IsNullOrWhiteSpace(ownerUpn)) ownerUpn = "N/A";

            string? dept = null;
            string? country = null;
            if (ownerUpn != "N/A")
                (dept, country) = await GetUserDepartmentAndCountryAsync(ownerUpn, ct);

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
                StorageUsedGB = storageBytes > 0 ? Math.Round(storageBytes / (double)(1024L * 1024 * 1024), 5) : 0,
                Storage_Allocated_Byte = ParseLong(row.GetValueOrDefault("Storage Allocated (Byte)")),
                Root_Web_Template = row.GetValueOrDefault("Root Web Template"),
                Owner_Principal_Name = ownerUpn,
                Department = dept,
                CountryOrRegion = country,
                Report_Period = row.GetValueOrDefault("Report Period"),
            });

            processed++;
            if (processed % BatchSize == 0)
                Log($"  SharePoint → batch {processed / BatchSize} ({processed} records processed)...", TerminalLevel.Info);
        }

        Log($"  SharePoint: {result.Count} rows fetched", TerminalLevel.Info);
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
                {
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

                    if (all.Count % BatchSize == 0)
                        Log($"  Users → batch {all.Count / BatchSize} ({all.Count} records processed)...", TerminalLevel.Info);
                }

            next = doc.RootElement.TryGetProperty("@odata.nextLink", out var nl)
                   ? nl.GetString() : null;
        }

        Log($"  Users: {all.Count} records fetched", TerminalLevel.Info);
        return all;
    }

    // ── Department + Country lookup — single Graph call per user ─────────────
    // Mirrors PS: ?$select=department,country per ogni UPN
    private async Task<(string? dept, string? country)> GetUserDepartmentAndCountryAsync(
        string upn, CancellationToken ct)
    {
        try
        {
            var req = new HttpRequestMessage(HttpMethod.Get,
                $"https://graph.microsoft.com/v1.0/users/{Uri.EscapeDataString(upn)}?$select=department,country");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            req.Headers.Add("ConsistencyLevel", "eventual");

            var resp = await _http.SendAsync(req, ct);
            if (!resp.IsSuccessStatusCode) return (null, null);

            var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
            var dept = doc.RootElement.TryGetProperty("department", out var d) ? d.GetString() : null;
            var country = doc.RootElement.TryGetProperty("country", out var c) ? c.GetString() : null;
            return (NullIfEmpty(dept), NullIfEmpty(country));
        }
        catch { return (null, null); }
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
    // Mirrors CreatePowerBIDataModelHistory from the PS script (append-only history).
    private async Task BuildPowerBIModelAsync(RunConfig cfg, CancellationToken ct)
    {
        var execId = Guid.NewGuid();
        string sql = $"DECLARE @ExecutionId UNIQUEIDENTIFIER = '{execId}';\n" +
                     TableDdl.PowerBIAggregateQuery;
        await ExecAsync(cfg.TargetConnectionString, sql, ct);
        Log($"  dbo.PowerBIDataModelHistory populated (ExecutionId={execId})", TerminalLevel.Success);
    }

    // Mirrors CreatePowerBIDataModelCountryOrRegion from the PS script.
    private async Task BuildCountryOrRegionAsync(RunConfig cfg, CancellationToken ct) =>
        await ExecAsync(cfg.TargetConnectionString, @"
            INSERT INTO dbo.PowerBICountryOrRegion (Department, CountryName, CountryCount)
            SELECT
                ISNULL(Department, 'Unknown') AS Department,
                CountryOrRegion               AS CountryName,
                COUNT(*)                      AS CountryCount
            FROM dbo.MicrosoftUsers
            WHERE CountryOrRegion IS NOT NULL AND CountryOrRegion <> ''
            GROUP BY ISNULL(Department, 'Unknown'), CountryOrRegion
            ORDER BY ISNULL(Department, 'Unknown'), COUNT(*) DESC;", ct);

    // ── SQL init ──────────────────────────────────────────────────────────────
    private async Task InitDatabaseAsync(RunConfig cfg, CancellationToken ct)
    {
        Log($"Initializing database [{cfg.SqlDatabase}]...");
        // Use IF NOT EXISTS — avoids the 'file already exists' error when the DB
        // was previously detached/dropped but the .mdf file is still on disk.
        await ExecAsync(cfg.MasterConnectionString,
            $"IF DB_ID(N'{cfg.SqlDatabase}') IS NULL CREATE DATABASE [{cfg.SqlDatabase}];", ct);
        // If the DB already exists but was just created above, connect to it now.
        // Either way the target connection string is now valid.
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

        // Formatta durata come hh:mm:ss
        var ts = TimeSpan.FromSeconds(duration);
        string durStr = $"{(int)ts.TotalHours:D2}:{ts.Minutes:D2}:{ts.Seconds:D2}";

        // Calcola TableSizeMB per la singola tabella (NULL per righe aggregate)
        bool isAggregate = report is "TOTAL" || report.StartsWith("CANCEL") || report.StartsWith("FAIL");
        string sizeSql = isAggregate
            ? "NULL"
            : $@"(SELECT CAST(SUM(a.data_pages) * 8.0 / 1024 AS FLOAT)
                  FROM sys.tables t
                  JOIN sys.indexes i ON t.object_id = i.object_id
                  JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                  JOIN sys.allocation_units a ON p.partition_id = a.container_id
                  WHERE t.name = '{report}' AND a.type = 1)";

        await ExecAsync(cfg.TargetConnectionString, $@"
            INSERT INTO dbo.ExecutionLog
            (ExecutionId,ExecutionDate,ReportName,Status,
             RowsRetrieved,RowsInserted,DurationTimeJob,
             ErrorMessage,MachineName,PowerShellVersion,TableSizeMB)
            VALUES('{execId}',SYSDATETIME(),'{report}','{status}',
                   {retrieved},{inserted},'{durStr}',
                   {safeErr},'{Environment.MachineName}','C#-1.0',{sizeSql})", ct);
    }

    private async Task<List<ExecutionLogEntry>> LoadHistoryAsync(RunConfig cfg, CancellationToken ct)
    {
        var list = new List<ExecutionLogEntry>();
        try
        {
            await using var conn = new SqlConnection(cfg.TargetConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(@"
                SELECT ExecutionDate, ReportName, Status,
                       RowsRetrieved, RowsInserted, DurationTimeJob, ErrorMessage
                FROM dbo.ExecutionLog
                WHERE ExecutionId = (
                    SELECT TOP 1 ExecutionId
                    FROM dbo.ExecutionLog
                    ORDER BY ExecutionDate DESC
                )
                ORDER BY ExecutionDate DESC", conn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
                list.Add(new ExecutionLogEntry
                {
                    ExecutionDate = rdr.GetDateTime(0),
                    ReportName = rdr.GetString(1),
                    Status = rdr.GetString(2),
                    RowsRetrieved = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                    RowsInserted = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4),
                    DurationTimeJob = rdr.GetString(5),
                    ErrorMessage = rdr.IsDBNull(6) ? null : rdr.GetString(6),
                });
        }
        catch { }
        return list;
    }

    // ── Graph auth — certificate-based client assertion (RS256 JWT) ──────────
    // Mirrors Get-GraphAccessToken from the PowerShell script exactly.
    private async Task AcquireTokenAsync(RunConfig cfg, CancellationToken ct)
    {
        var thumbprint = cfg.CertificateThumbprint.Trim();
        var store = new System.Security.Cryptography.X509Certificates.X509Store(
            System.Security.Cryptography.X509Certificates.StoreName.My,
            System.Security.Cryptography.X509Certificates.StoreLocation.CurrentUser);
        store.Open(System.Security.Cryptography.X509Certificates.OpenFlags.ReadOnly);

        var certs = store.Certificates.Find(
            System.Security.Cryptography.X509Certificates.X509FindType.FindByThumbprint,
            thumbprint, validOnly: false);
        store.Close();

        if (certs.Count == 0)
            throw new InvalidOperationException(
                $"Certificate not found in CurrentUser\\My with thumbprint: {thumbprint}");

        var cert = certs[0];
        if (!cert.HasPrivateKey)
            throw new InvalidOperationException("Certificate does not have a private key.");

        string x5t = Base64UrlEncode(cert.GetCertHash());

        var headerObj = new { alg = "RS256", typ = "JWT", x5t };
        string headerB64 = Base64UrlEncode(
            System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(headerObj)));

        var now = DateTimeOffset.UtcNow;
        var payloadObj = new
        {
            aud = $"https://login.microsoftonline.com/{cfg.TenantId}/oauth2/v2.0/token",
            iss = cfg.ClientId,
            sub = cfg.ClientId,
            jti = Guid.NewGuid().ToString(),
            nbf = now.ToUnixTimeSeconds(),
            exp = now.AddMinutes(10).ToUnixTimeSeconds(),
        };
        string payloadB64 = Base64UrlEncode(
            System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payloadObj)));

        string unsignedToken = $"{headerB64}.{payloadB64}";
        byte[] bytesToSign = System.Text.Encoding.UTF8.GetBytes(unsignedToken);
        byte[] signatureBytes;

        var rsa = System.Security.Cryptography.X509Certificates
                        .RSACertificateExtensions.GetRSAPrivateKey(cert);
        if (rsa != null)
        {
            signatureBytes = rsa.SignData(
                bytesToSign,
                System.Security.Cryptography.HashAlgorithmName.SHA256,
                System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        }
        else
        {
            var csp = cert.PrivateKey as System.Security.Cryptography.RSACryptoServiceProvider
                      ?? throw new InvalidOperationException(
                             "No usable RSA private key found on the certificate.");
            signatureBytes = csp.SignData(bytesToSign,
                new System.Security.Cryptography.SHA256Managed());
        }

        string clientAssertion = $"{unsignedToken}.{Base64UrlEncode(signatureBytes)}";

        var body = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string,string>("client_id",             cfg.ClientId),
            new KeyValuePair<string,string>("scope",                 "https://graph.microsoft.com/.default"),
            new KeyValuePair<string,string>("grant_type",            "client_credentials"),
            new KeyValuePair<string,string>("client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"),
            new KeyValuePair<string,string>("client_assertion",      clientAssertion),
        });

        var resp = await _http.PostAsync(
            $"https://login.microsoftonline.com/{cfg.TenantId}/oauth2/v2.0/token", body, ct);
        resp.EnsureSuccessStatusCode();

        var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        _token = doc.RootElement.GetProperty("access_token").GetString()!;
    }

    private static string Base64UrlEncode(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

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
    Dictionary<string, object?> ToColumnDictionary();
}

public class ExchangeRow : IRowMappable
{
    public string? User_Principal_Name { get; set; }
    public string? Display_Name { get; set; }
    public string? Department { get; set; }
    public string? CountryOrRegion { get; set; }
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
    public double DeletedItemSizeGB { get; set; }
    public long Deleted_Item_Quota_Byte { get; set; }
    public string? Has_Archive { get; set; }
    public string? Report_Period { get; set; }

    public Dictionary<string, object?> ToColumnDictionary() => new()
    {
        ["User_Principal_Name"] = User_Principal_Name,
        ["Display_Name"] = Display_Name,
        ["Department"] = Department,
        ["CountryOrRegion"] = CountryOrRegion,
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
        ["DeletedItemSizeGB"] = DeletedItemSizeGB,
        ["Deleted_Item_Quota__Byte_"] = Deleted_Item_Quota_Byte,
        ["Has_Archive"] = Has_Archive,
        ["Report_Period"] = Report_Period,
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
    public string? CountryOrRegion { get; set; }
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
        ["CountryOrRegion"] = CountryOrRegion,
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
    public string? Department { get; set; }
    public string? CountryOrRegion { get; set; }
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
        ["Department"] = Department,
        ["CountryOrRegion"] = CountryOrRegion,
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
