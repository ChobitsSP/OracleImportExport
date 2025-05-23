﻿using CsvHelper;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Globalization;
using System.Text;

namespace OracleImport.Utils
{
    public static class DapperImport
    {
        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Import:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static string GetInsertSql(List<TableColumn> columns, string table_name)
        {
            // var columnNames = string.Join(",", columns.Select(t => $@"""{t.name}"""));
            var columnNames = string.Join(",", columns.Select(t => t.name));
            var parameterNames = string.Join(",", columns.Select(t => ":" + t.name));
            return $"INSERT INTO {table_name} ({columnNames}) VALUES ({parameterNames})";
        }

        public class TableColumn
        {
            public int id { get; set; }
            public string name { get; set; }
            public string type { get; set; }
            public bool null_able { get; set; }
            public long? data_length { get; set; }
            public int? numeric_precision { get; set; }
            public int? numeric_scale { get; set; }
        }

        public class TableColumnsItem
        {
            public int COLUMN_ID { get; set; }
            public string COLUMN_NAME { get; set; }
            public string DATA_TYPE { get; set; }
            public string NULLABLE { get; set; }
            public long? DATA_LENGTH { get; set; }
            public int? DATA_PRECISION { get; set; }
            public int? DATA_SCALE { get; set; }
        }

        static async Task<List<TableColumn>> GetColumns(OracleConnection conn, string table_name)
        {
            var sql = $@"
select
  t2.COLUMN_ID,
  t1.COLUMN_NAME,
  t2.DATA_TYPE,
  t2.DATA_LENGTH,
  t2.DATA_PRECISION,
  t2.DATA_SCALE,
  t2.NULLABLE
from
  user_col_comments t1
  inner join user_tab_columns t2 on t1.COLUMN_NAME = t2.COLUMN_NAME
  and t1.TABLE_NAME = t2.TABLE_NAME
where 1=1
   and t1.table_name = :table_name
order by t1.TABLE_NAME, column_id
";

            var list = await conn.QueryAsync<TableColumnsItem>(sql, new { table_name });

            var result = list.Select(t => new TableColumn()
            {
                id = t.COLUMN_ID,
                name = t.COLUMN_NAME,
                null_able = t.NULLABLE == "Y",
                type = t.DATA_TYPE,
                data_length = t.DATA_LENGTH,
                numeric_precision = t.DATA_PRECISION,
                numeric_scale = t.DATA_SCALE,
            }).ToList();

            return result;
        }

        public static IEnumerable<List<T>> GroupList<T>(IEnumerable<T> items, int count)
        {
            List<T> currentGroup = new List<T>();
            foreach (var item in items)
            {
                currentGroup.Add(item);
                if (currentGroup.Count == count)
                {
                    yield return currentGroup;
                    currentGroup = new List<T>();
                }
            }

            // 返回最后一组（可能包含少于 count 个元素）
            if (currentGroup.Count > 0)
            {
                yield return currentGroup;
            }
        }

        public static IEnumerable<Dictionary<string, string>> ReadCsv(string filePath)
        {
            using var reader = new StreamReader(filePath, encoding: GetEncoding());
            using var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

            csvReader.Read();
            csvReader.ReadHeader();
            var headers = csvReader.HeaderRecord;

            while (csvReader.Read())
            {
                var record = new Dictionary<string, string>();
                foreach (var header in headers)
                {
                    var vala = csvReader[header];
                    record[header] = csvReader[header];
                }
                yield return record;
            }
        }

        static object GetInsertObj(Dictionary<string, string> row, List<TableColumn> columns)
        {
            var item = new Dictionary<string, object>();

            foreach (var key in row.Keys)
            {
                var column = columns.FirstOrDefault(t => string.Equals(t.name, key, StringComparison.OrdinalIgnoreCase));
                if (column == null) throw new Exception("column not found: " + key);
                var value = GetValueObj(row[key], column);
                item[key] = value;
            }

            return item;
        }

        static object GetValueObj(string value, TableColumn column)
        {
            //string[] strlist = [
            //    "VARCHAR2",
            //    "NVARCHAR2",
            //    "CLOB",
            //    "NCHAR",
            //    "XMLTYPE",
            //];

            if (column.type == "FLOAT")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (decimal?)null;
                }
                return decimal.Parse(value);
            }

            if (column.type == "DATE")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (DateTime?)null;
                }
                return DateTime.Parse(value);
            }

            if (column.type == "NUMBER")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (int?)null;
                }
                if ((column.numeric_scale ?? 0) == 0)
                {
                    return long.Parse(value);
                }
                return decimal.Parse(value);
            }

            // 字符串类型
            if (string.IsNullOrEmpty(value) && column.null_able)
            {
                return (string)null;
            }
            return value;
        }

        public static async Task Import(string filePath, string tableName, int batchSize)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);

            if (conn.State != System.Data.ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            try
            {
                var columns = await GetColumns(conn, tableName);

                var sql = GetInsertSql(columns, tableName);

                var allList = ReadCsv(filePath);
                var glist = GroupList(allList, batchSize);

                int rowCount = 0;

                await conn.ExecuteAsync("TRUNCATE TABLE " + tableName);

                foreach (var group in glist)
                {
                    using var trans = await conn.BeginTransactionAsync();

                    try
                    {
                        var addList = group.Select(t => GetInsertObj(t, columns)).ToArray();
                        await conn.ExecuteAsync(sql, addList, trans);
                        await trans.CommitAsync();
                        rowCount += addList.Length;
                    }
                    catch (Exception ex)
                    {
                        LogService.Warn($"Table {tableName} Import Error: {ex.Message}");
                        LogService.Error(ex);
                        await trans.RollbackAsync();
                    }

                    LogService.Info($"Start Import {tableName} {rowCount}");
                }

                LogService.Info("End Import " + tableName);
            }
            catch (Exception ex)
            {
                LogService.Error(ex);
                LogService.Warn($"Table {tableName} Import Failed: " + ex.Message);
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        public static async Task SeqFix(IEnumerable<string> tableNames)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);

            if (conn.State != System.Data.ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            var sb = new StringBuilder();

            foreach (var name in tableNames)
            {
                try
                {
                    await SeqFix(conn, name, sb);
                }
                catch (Exception ex)
                {
                    LogService.Warn($"修复{name}报错: {ex.Message}");
                    LogService.Error(ex);
                }
            }

            await conn.CloseAsync();

            File.WriteAllText("fix_seq.sql", sb.ToString(), Encoding.UTF8);
        }

        public static async Task SeqFix(OracleConnection conn, string tableName, StringBuilder sb)
        {
            var columns = await GetColumns(conn, tableName);

            var keyColumn = columns.First();
            if (!keyColumn.name.StartsWith("SEQ_")) return;

            var maxId = (await conn.QueryFirstOrDefaultAsync<long?>($"select max({keyColumn.name}) from {tableName}")) ?? 0;

            var seq_name = "SEQ_" + tableName;
            var currentId = await conn.QueryFirstOrDefaultAsync<long?>($"SELECT LAST_NUMBER FROM user_sequences WHERE sequence_name = '{seq_name}'");

            if (currentId == null)
            {
                LogService.Warn($"序列不存在: {seq_name}");
                return;
            }

            if (currentId >= maxId) return;

            var add_value = maxId - currentId + 1;

            //using var trans = await conn.BeginTransactionAsync();

            // http://www.gokhanatil.com/2011/01/how-to-set-current-value-of-a-sequence-without-droppingrecreating.html

            try
            {
                //await conn.ExecuteAsync($"ALTER SEQUENCE {seq_name} INCREMENT BY {add_value}", transaction: trans);
                //await conn.ExecuteAsync($"SELECT {seq_name}.NEXTVAL FROM dual");
                //await conn.ExecuteAsync($"ALTER SEQUENCE {seq_name} INCREMENT BY 1");
                //await trans.CommitAsync();
                LogService.Info($"已修复序列: {tableName}, 修复前: {currentId}, 修复后: {maxId}");

                //http://www.gokhanatil.com/2011/01/how-to-set-current-value-of-a-sequence-without-droppingrecreating.html
                sb.AppendFormat("ALTER SEQUENCE {0} INCREMENT BY {1};", seq_name, add_value);
                sb.AppendLine();
                sb.AppendFormat("SELECT {0}.NEXTVAL FROM dual;", seq_name);
                sb.AppendLine();
                sb.AppendFormat("ALTER SEQUENCE {0} INCREMENT BY 1;", seq_name);
                sb.AppendLine();
            }
            catch (Exception ex)
            {
                LogService.Error(ex);
                //await trans.RollbackAsync();
            }
        }
    }
}
