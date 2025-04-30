using System.Data;
using Oracle.ManagedDataAccess.Client;
using CsvHelper;
using System.Globalization;
using System.Text;

namespace OracleImport
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            while (true)
            {
                Console.WriteLine("Please input folder:");
                var folder = Console.ReadLine();

                if (!Directory.Exists(folder))
                {
                    Console.WriteLine("Folder not exists, please input again.");
                    continue;
                }

                var batchSize = ("Please input batch size (default 1000):", 1000);

                var csvFiles = Directory.GetFiles(folder, "*.csv");

                foreach (var csvFile in csvFiles)
                {
                    var fileName = Path.GetFileName(csvFile);

                    if (fileName.EndsWith("_DATA_TABLE"))
                    {
                        fileName = fileName.Replace("_DATA_TABLE", string.Empty);
                    }

                    Console.WriteLine($"Importing {fileName}...");

                    try
                    {
                        ImportData(csvFile, fileName, batchSize);
                    }
                    catch (Exception ex)
                    {
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

        public static void ImportData(string csvFilePath, string tableName, int batchSize = 1000)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);

            conn.Open();

            using var bulkCopy = new OracleBulkCopy(conn)
            {
                DestinationTableName = tableName,
                BatchSize = batchSize,
            };

            var table = ReadCsv(csvFilePath);
            bulkCopy.WriteToServer(table);

            conn.Close();
        }

        public static DataTable ReadCsv(string filePath)
        {
            var dt = new DataTable();

            using (var reader = new StreamReader(filePath, encoding: Encoding.GetEncoding("GBK")))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                // Do any configuration to `CsvReader` before creating CsvDataReader.
                using (var dr = new CsvDataReader(csv))
                {
                    dt.Load(dr);
                }
            }

            return dt;
        }
    }
}
