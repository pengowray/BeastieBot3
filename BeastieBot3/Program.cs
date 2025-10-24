namespace BeastieBot3 {
    internal class Program {
        static void Main(string[] args) {
            // Demo: read all path values from default paths.ini
            var reader = new IniPathReader();
            var all = reader.GetAll();

            Console.WriteLine($"Reading paths from: {reader.SourceFilePath}");
            foreach (var kv in all) {
                Console.WriteLine($"{kv.Key} = {kv.Value}");
            }
        }
    }
}
