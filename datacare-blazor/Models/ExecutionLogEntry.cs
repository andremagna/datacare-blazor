namespace DataCareLite.Models;

public class ExecutionLogEntry
{
    public DateTime  ExecutionDate   { get; set; }
    public string    ReportName      { get; set; } = string.Empty;
    public string    Status          { get; set; } = string.Empty;
    public int       RowsRetrieved   { get; set; }
    public int       RowsInserted    { get; set; }
    public int       DurationSeconds { get; set; }
    public string?   ErrorMessage    { get; set; }
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
        TerminalLevel.Step    => "t-y",
        TerminalLevel.Error   => "t-r",
        TerminalLevel.Warning => "t-w",
        _                     => "t-d"
    };
}
