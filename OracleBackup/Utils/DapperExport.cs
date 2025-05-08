using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace OracleBackup.Utils
{
    public static class DapperExport
    {
        public static Task TableToCsv(string tableName)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);
            return TableToCsv(conn, tableName, string.Empty);
        }

        public static async Task TableToCsv(IDbConnection conn, string tableName, string dir)
        {
            var sql = $"SELECT * FROM {tableName}";
            using var reader = await conn.ExecuteReaderAsync(sql);
            var filePath = Path.Combine(dir, $"{tableName}.csv");
            await WriteToCsv(reader, filePath);
        }

        static async Task WriteToCsv(IDataReader reader, string filePath)
        {
            using var writer = new StreamWriter(filePath);

            var csv = new CsvWriter(writer, new CsvConfiguration(new System.Globalization.CultureInfo("zh-CN"))
            {
                Encoding = Encoding.GetEncoding("GBK"),
            });

            await csv.WriteRecordsAsync(Enumerable.Range(0, reader.FieldCount).Select(reader.GetName));

            while (reader.Read())
            {
                var cells = Enumerable.Range(0, reader.FieldCount)
                    .Select(reader.GetValue)
                    .Select(t =>
                    {
                        if (t == null) return null;
                        return t.ToString();
                    });

                await csv.WriteRecordsAsync(cells);
            }

            writer.Close();
        }
    }
}
