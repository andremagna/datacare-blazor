namespace DataCare.Models;

public class ExecutionLogEntry
{
    public Guid? ExecutionId { get; set; }
    public DateTime ExecutionDate { get; set; }
    public string ReportName { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int RowsRetrieved { get; set; }
    public int RowsInserted { get; set; }
    /// <summary>Duration as hh:mm:ss string, matching the PS script's DurationTimeJob column.</summary>
    public string DurationTimeJob { get; set; } = string.Empty;
    public float? TableSizeMB { get; set; }
    public string? ErrorMessage { get; set; }
    public string? MachineName { get; set; }
    public string? PowerShellVersion { get; set; }
    public bool IsSuccess => Status == "SUCCESS";
}

public enum TerminalLevel { Info, Step, Success, Warning, Error }

public record TerminalLine(string Message, TerminalLevel Level, DateTime Timestamp)
{
    public TerminalLine(string message, TerminalLevel level)
        : this(message, level, DateTime.Now) { }

    public string CssClass => Level switch
    {
        TerminalLevel.Success => "t-g",
        TerminalLevel.Step => "t-y",
        TerminalLevel.Error => "t-r",
        TerminalLevel.Warning => "t-w",
        _ => "t-d"
    };
}

/// <summary>A file download event saved in dbo.DownloadFile.</summary>
public class DownloadFileEntry
{
    public int Id { get; set; }
    public DateTime DateTimeDownload { get; set; }
    /// <summary>Raw file content (bytes) stored as VARBINARY in DB.</summary>
    public byte[]? File { get; set; }
    /// <summary>MIME type: application/pdf | text/csv | application/json</summary>
    public string FileType { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    /// <summary>Storage path or identifier (e.g. "dbo.DownloadFile / BinaryColumn").</summary>
    public string FileStorage { get; set; } = string.Empty;
    public long? FileSizeBytes { get; set; }
}

/// <summary>A Windows Task Scheduler job saved in dbo.TaskSchedulerJob.</summary>
public class TaskSchedulerJob
{
    public int Id { get; set; }
    public string JobName { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    /// <summary>Comma-separated days of week: Mon,Tue,Wed,Thu,Fri,Sat,Sun</summary>
    public string DaysOfWeek { get; set; } = string.Empty;
    /// <summary>Time of day HH:mm (24h)</summary>
    public string StartTime { get; set; } = string.Empty;
    /// <summary>ENABLED | DISABLED</summary>
    public string Status { get; set; } = "ENABLED";
    /// <summary>Full path to the executable or script</summary>
    public string ProgramPath { get; set; } = string.Empty;
    public string? Arguments { get; set; }
    /// <summary>Windows Task Scheduler folder path, e.g. \DataCare\</summary>
    public string TaskFolder { get; set; } = @"\DataCare\";
    /// <summary>Run-as user (e.g. SYSTEM or a service account)</summary>
    public string RunAsUser { get; set; } = "SYSTEM";
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
    /// <summary>Last time the job was actually triggered by Task Scheduler</summary>
    public DateTime? LastRunAt { get; set; }
    /// <summary>SUCCESS | FAILED | NEVER</summary>
    public string LastRunStatus { get; set; } = "NEVER";
    public string? LastRunMessage { get; set; }
}

/// <summary>An environment configuration saved in dbo.EnvironmentConfiguration.</summary>
public class EnvironmentConfig
{
    public int Id { get; set; }
    public string EnvironmentKey { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string CertificateThumbprint { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
}
