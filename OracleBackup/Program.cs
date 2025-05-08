using OracleBackup.Utils;

namespace OracleBackup
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("Please input table:");
                var tableName = Console.ReadLine();
                await DapperExport.TableToCsv(tableName);
            }
        }
    }
}
