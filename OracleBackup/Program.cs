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
                await DapperExport.TableToCsv(tableName);
            }
        }
    }
}
