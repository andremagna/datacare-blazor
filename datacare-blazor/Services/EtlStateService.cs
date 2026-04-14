using DataCareLite.Models;

namespace DataCareLite.Services;

public class EtlStateService
{
    public bool IsRunning { get; private set; }
    public int Progress { get; private set; }
    public string ProgressLabel { get; private set; } = "Idle";
    public string StatusText { get; private set; } = string.Empty;
    public string StatusCss { get; private set; } = string.Empty;

    // Dashboard state
    public bool HasRun { get; private set; } = false;
    public bool LastRunSuccess { get; private set; } = false;
    public string? LastConnectionString { get; private set; }

    public List<TerminalLine> TerminalLines { get; } = new();
    public List<ExecutionLogEntry> HistoryRows { get; } = new();

    public event Action? OnChanged;

    public void StartRun(string connectionString)
    {
        IsRunning = true;
        Progress = 0;
        ProgressLabel = "Starting...";
        StatusText = "Running...";
        StatusCss = "st run";
        LastConnectionString = connectionString;
        TerminalLines.Clear();
        Notify();
    }

    public void AppendLine(string message, TerminalLevel level = TerminalLevel.Info)
    {
        TerminalLines.Add(new TerminalLine(message, level));
        Notify();
    }

    public void SetProgress(int pct, string label)
    {
        Progress = pct;
        ProgressLabel = label;
        Notify();
    }

    public void CompleteRun(bool success, IEnumerable<ExecutionLogEntry> history)
    {
        IsRunning = false;
        HasRun = true;
        LastRunSuccess = success;
        Progress = success ? 100 : Progress;
        ProgressLabel = success ? "Done" : "Failed";
        StatusText = success ? "Completed successfully" : "Failed — see log";
        StatusCss = success ? "st ok" : "st err";
        HistoryRows.Clear();
        HistoryRows.AddRange(history);
        Notify();
    }

    public void SetCancelled()
    {
        IsRunning = false;
        HasRun = true;
        LastRunSuccess = false;
        StatusText = "Cancelled";
        StatusCss = "st err";
        ProgressLabel = "Cancelled";
        Notify();
    }

    public void SetFailed(string error)
    {
        IsRunning = false;
        HasRun = true;
        LastRunSuccess = false;
        StatusText = "Failed — see log";
        StatusCss = "st err";
        ProgressLabel = "Failed";
        Notify();
    }

    private void Notify() => OnChanged?.Invoke();
}
