using CsvHelper;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using System.Globalization;
using System.Text;

namespace OracleImport.Utils
{
    public static class DapperImport
    {
        public static async Task<string> GetInsertSql(OracleConnection conn, string table_name)
        {
            var columns = await GetColumns(conn, table_name);
            var columnNames = string.Join(",", columns.Select(t => $@"""{t.name}"""));
            var parameterNames = string.Join(",", columns.Select(t => ":" + t.name));
            return $"INSERT INTO {table_name} ({columnNames}) VALUES ({parameterNames})";
        }

        public class TableColumn
        {
            public int id { get; set; }
            public string name { get; set; }
            public string type { get; set; }
            public bool null_able { get; set; }
        }

        public class TableColumnsItem
        {
            public int COLUMN_ID { get; set; }
            public string COLUMN_NAME { get; set; }
            public string DATA_TYPE { get; set; }
            public string NULLABLE { get; set; }
        }

        static async Task<List<TableColumn>> GetColumns(OracleConnection conn, string table_name)
        {
            var sql = $@"
select
  t2.COLUMN_ID,
  t1.COLUMN_NAME,
  t2.DATA_TYPE,
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

        public static IEnumerable<Dictionary<string, object>> ReadCsv(string filePath)
        {
            using var reader = new StreamReader(filePath, encoding: Encoding.GetEncoding("GBK"));
            using var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

            csvReader.Read();
            csvReader.ReadHeader();
            var headers = csvReader.HeaderRecord;

            while (csvReader.Read())
            {
                var record = new Dictionary<string, object>();
                foreach (var header in headers)
                {
                    record[header] = csvReader[header];
                }
                yield return record;
            }
        }

        public static async Task Import(string filePath, string tableName, int batchSize)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new OracleConnection(constr);

            var sql = await GetInsertSql(conn, tableName);

            var allList = ReadCsv(filePath);
            var glist = GroupList(allList, batchSize);

            foreach (var group in glist)
            {
                using var trans = await conn.BeginTransactionAsync();

                try
                {
                    await conn.ExecuteAsync(sql, group);
                    await trans.CommitAsync();
                }
                catch (Exception ex)
                {
                    LogService.Warn($"Table {tableName} Import Error: {ex.Message}");
                    LogService.Error(ex);
                }
            }
        }
    }
}
