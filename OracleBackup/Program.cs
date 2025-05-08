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

            try
            {
                await DapperExport.BackupToCsv();
            }
            catch (Exception ex)
            {
                LogService.Error(ex);
            }
        }
    }
}
