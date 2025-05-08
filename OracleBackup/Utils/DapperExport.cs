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
using System.Formats.Asn1;

namespace OracleBackup.Utils
{
    public static class DapperExport
    {
        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Backup:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static Task TableToCsv(string tableName)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);
            var folder = ConfigUtils.GetSectionValue("Backup:Folder");
            return TableToCsv(conn, tableName, folder);
        }

        public static async Task TableToCsv(IDbConnection conn, string tableName, string dir)
        {
            var sql = $"SELECT * FROM {tableName}";
            using var reader = await conn.ExecuteReaderAsync(sql);
            var filePath = Path.Combine(dir, $"{tableName}.csv");
            await WriteToCsv(reader, filePath);
        }

        static IEnumerable<string> GetColumns(IDataReader reader)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                yield return reader.GetName(i);
            }
        }

        static IEnumerable<string> GetCells(IDataReader reader)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.GetValue(i);

                if (value == null)
                {
                    yield return null;
                }
                else if (value.GetType() == typeof(DateTime))
                {
                    yield return ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss");
                }
                else
                {
                    yield return value.ToString();
                }
            }
        }

        static async Task WriteToCsv(IDataReader reader, string filePath)
        {
            using var writer = new StreamWriter(filePath);

            var csv = new CsvWriter(writer, new CsvConfiguration(new System.Globalization.CultureInfo("zh-CN"))
            {
                Encoding = GetEncoding(),
            });

            foreach (var columnName in GetColumns(reader))
            {
                csv.WriteField(columnName);
            }

            await csv.NextRecordAsync();

            while (reader.Read())
            {
                foreach (var v in GetCells(reader))
                {
                    csv.WriteField(v);
                }
                await csv.NextRecordAsync();
            }

            writer.Close();
        }
    }
}
