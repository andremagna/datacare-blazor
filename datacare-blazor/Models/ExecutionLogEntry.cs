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
