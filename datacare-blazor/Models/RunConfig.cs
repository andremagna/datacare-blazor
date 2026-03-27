namespace DataCareLite.Models;

/// <summary>
/// The 7 user-configurable parameters bound directly from the UI inputs.
/// Passed by the page into EtlService.RunAsync so every call uses
/// exactly what the user typed — nothing is read from appsettings.
/// </summary>
public class RunConfig
{
    public string TenantId     { get; set; } = string.Empty;
    public string ClientId     { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string SqlServer    { get; set; } = @"localhost\SQLEXPRESS";
    public string SqlDatabase  { get; set; } = "DataCareTest";
    public string Period       { get; set; } = "D180";
    public string Department   { get; set; } = "Information Technology";

    public string TargetConnectionString =>
        $"Server={SqlServer};Database={SqlDatabase};" +
        "Trusted_Connection=True;TrustServerCertificate=True;";
    public string MasterConnectionString =>
        $"Server={SqlServer};Database=master;" +
        "Trusted_Connection=True;TrustServerCertificate=True;";
}
