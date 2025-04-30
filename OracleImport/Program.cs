using System.Text;
using OracleImport.Utils;

namespace OracleImport
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            LogService.Init();

            var folder = GetFolder();

            var batchSize = ConfigUtils.GetInt("Import:BatchSize") ?? 10000;

            var csvFiles = Directory.GetFiles(folder, "*.csv");

            foreach (var csvFile in csvFiles.OrderBy(t => new FileInfo(t).Length))
            {
                var fileName = Path.GetFileNameWithoutExtension(csvFile);

                if (fileName.EndsWith("_DATA_TABLE"))
                {
                    fileName = fileName.Replace("_DATA_TABLE", string.Empty);
                }

                Console.WriteLine($"Importing {fileName}...");

                try
                {
                    await DapperImport.Import(csvFile, fileName, batchSize);
                }
                catch (Exception ex)
                {
                    LogService.Error(ex);
                    Console.WriteLine($"Error importing {fileName}: {ex.Message}");
                    continue;
                }

                Console.WriteLine($"Import {fileName} completed.");
            }

            Console.ReadLine();
        }

        static string GetFolder()
        {
            var folder = ConfigUtils.GetSectionValue("Import:Folder");
            if (!string.IsNullOrEmpty(folder) && Directory.Exists(folder)) return folder;

            while (true)
            {
                Console.WriteLine("Please input folder:");
                folder = Console.ReadLine();

                if (!Directory.Exists(folder))
                {
                    Console.WriteLine("Folder not exists, please input again.");
                    continue;
                }

                return folder;
            }
        }

        static int GetIntFromConsole(string msg, int def = 0)
        {
            Console.WriteLine(msg);
            var input = Console.ReadLine();
            if (string.IsNullOrEmpty(input)) return def;
            if (int.TryParse(input, out var result)) return result;
            return def;
        }
    }
}
