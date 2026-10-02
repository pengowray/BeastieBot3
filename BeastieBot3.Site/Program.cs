var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.MapGet("/", () => "BeastieBot3.Site skeleton");
app.Run();

public partial class Program;
