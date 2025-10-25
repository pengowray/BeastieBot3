namespace BeastieBot3 {
    internal class Program {
        static void Main(string[] args) {
            // Detect container path for COL dataset
            var colPath = "/app/datasets/Catalogue_of_Life_2025-10-10_XR";
            if (Directory.Exists(colPath)) {
                Console.WriteLine($"COL dataset mounted at: {colPath}");
            } else {
                Console.WriteLine("COL dataset not mounted. Configure docker-compose volume mapping.");
            }

            // Still read all path values from default paths.ini
            var reader = new IniPathReader();
            var all = reader.GetAll();
            Console.WriteLine($"Reading paths from: {reader.SourceFilePath}");
            foreach (var kv in all) {
                Console.WriteLine($"{kv.Key} = {kv.Value}");
            }
        }
    }
}
