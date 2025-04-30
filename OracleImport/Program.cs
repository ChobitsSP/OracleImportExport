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

            while (true)
            {
                Console.WriteLine("Please input folder:");
                var folder = Console.ReadLine();

                if (!Directory.Exists(folder))
                {
                    Console.WriteLine("Folder not exists, please input again.");
                    continue;
                }

                var batchSize = GetIntFromConsole("Please input batch size (default 1000):", 1000);

                var csvFiles = Directory.GetFiles(folder, "*.csv");

                foreach (var csvFile in csvFiles)
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
