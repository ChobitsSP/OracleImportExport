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
        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Backup:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static async Task BackupToCsv()
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);
            var folder = ConfigUtils.GetSectionValue("Backup:Folder");

            var dir = Path.Combine(folder, DateTime.Now.ToString("yyMMddHHmmss"));
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            var tableNames = await GetTableNames(conn);

            foreach (var tableName in tableNames)
            {
                LogService.Info($"Exporting {tableName} Start");
                await TableToCsv(conn, tableName, dir);
                LogService.Info($"Exporting {tableName} End");
            }

            //::todo 压缩文件

            // Directory.Delete(dir, true);
        }

        public static async Task<IEnumerable<string>> GetTableNames(IDbConnection conn)
        {
            const string sql = "SELECT table_name FROM user_tables order by table_name";
            var list = await conn.QueryAsync<string>(sql);
            return list;
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
