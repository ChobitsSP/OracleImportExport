using OracleBackup.Utils;
using System.Text;

namespace OracleBackup
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            LogService.Init();

            while (true)
            {
                Console.WriteLine("Please input table:");
                var tableName = Console.ReadLine();

                if (string.IsNullOrEmpty(tableName)) continue;

                try
                {
                    await DapperExport.TableToCsv(tableName);
                    Console.WriteLine($"Export {tableName} completed.");
                }
                catch (Exception ex)
                {
                    LogService.Error(ex);
                    Console.WriteLine($"Error exporting {tableName}: {ex.Message}");
                }
            }
        }
    }
}
