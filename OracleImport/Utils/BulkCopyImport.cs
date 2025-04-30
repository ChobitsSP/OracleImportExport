using CsvHelper;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Globalization;
using System.Text;

namespace OracleImport.Utils
{
    public static class BulkCopyImport
    {
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
            bulkCopy.WriteToServer(table, DataRowState.Added);

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
