using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.Runtime.Serialization;
using System.Text;
using System.Transactions;

// Define your class that matches the CSV structure
public class CsvObject
{
    public required string EXERCICIO { get; set; }
    public required string EMPENHO { get; set; }
    public required string LIQUIDACAO { get; set; }
    public required string OPERACAO { get; set; }
    public required string VALOR { get; set; }
    public required string DOCUMENTO { get; set; }
    public required string NUMERO { get; set; }
    public required string DATAOPERACAO { get; set; }
    public required string FAVORECIDO { get; set; }
    public required string VALORDOCUMENTO { get; set; }
}

public class Program
{
    public static void Main()
    {
        var connection = new SqlConnection("Data Source=.;Initial Catalog=banco_lanchonete;Persist Security Info=False;User ID=comcorp;Password=comcorp;Connection Timeout=300;");
        connection.Open();
        try
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ";" };
            var sqlString = new StringBuilder();

            Console.WriteLine("Insira o caminho do arquivo");
            var csvFilePath = Console.ReadLine().Replace("\'", "\\");

            using (var reader = new StreamReader(csvFilePath))
            using (var csv = new CsvReader(reader, config))
            {
                var records = new List<CsvObject>();
                csv.Read();
                csv.ReadHeader();
                while (csv.Read())
                {

                    var headers = csv.HeaderRecord;

                    var obj = new object();

                    var csvRow = new CsvObject
                    {
                        EXERCICIO = string.IsNullOrEmpty(csv.GetField(0)) ? "NULL" : csv.GetField(0),
                        EMPENHO = string.IsNullOrEmpty(csv.GetField(1)) ? "NULL" : csv.GetField(1),
                        LIQUIDACAO = string.IsNullOrEmpty(csv.GetField(2)) ? "NULL" : csv.GetField(2),
                        OPERACAO = string.IsNullOrEmpty(csv.GetField(3)) ? "NULL" : csv.GetField(3),
                        VALOR = string.IsNullOrEmpty(csv.GetField(4)) ? "NULL" : csv.GetField(4).Replace(',', '.'),
                        DOCUMENTO = string.IsNullOrEmpty(csv.GetField(5)) ? "NULL" : csv.GetField(5),
                        NUMERO = string.IsNullOrEmpty(csv.GetField(6)) ? "NULL" : csv.GetField(6),
                        DATAOPERACAO = string.IsNullOrEmpty(csv.GetField(7)) ? "NULL" : csv.GetField(7),
                        FAVORECIDO = string.IsNullOrEmpty(csv.GetField(8)) ? "NULL" : csv.GetField(8),
                        VALORDOCUMENTO = string.IsNullOrEmpty(csv.GetField(9)) ? "NULL" : csv.GetField(9).Replace(',', '.')
                    };

                    records.Add(csvRow);
                }

                sqlString.AppendLine("BEGIN TRY ");
                sqlString.AppendLine("  BEGIN TRAN;");

                foreach (var record in records)
                {
                        sqlString.AppendLine($@" INSERT INTO despesas                                ");
                        sqlString.AppendLine($@"         (EXERCICIO,                         ");
                        sqlString.AppendLine($@"          EMPENHO,                                       ");
                        sqlString.AppendLine($@"          LIQUIDACAO,                                  ");
                        sqlString.AppendLine($@"          OPERACAO,                               ");
                        sqlString.AppendLine($@"          VALOR,                                 ");
                        sqlString.AppendLine($@"          DOCUMENTO,                                   ");
                        sqlString.AppendLine($@"          NUMERO,                                   ");
                        sqlString.AppendLine($@"          DATAOPERACAO,                                   ");
                        sqlString.AppendLine($@"          FAVORECIDO,                                   ");
                        sqlString.AppendLine($@"          VALORDOCUMENTO)                               ");
                        sqlString.AppendLine($@"          VALUES (                                   ");
                        sqlString.AppendLine($@"          {record.EXERCICIO},                ");
                        sqlString.AppendLine($@"          {record.EMPENHO},                              ");
                        sqlString.AppendLine($@"          {record.LIQUIDACAO},                         ");
                        sqlString.AppendLine($@"          '{record.OPERACAO}',                    ");
                        sqlString.AppendLine($@"          {record.VALOR},                      ");
                        sqlString.AppendLine($@"          '{record.DOCUMENTO}',                          ");
                        sqlString.AppendLine($@"          '{record.NUMERO}',                          ");
                        sqlString.AppendLine($@"          '{record.DATAOPERACAO}',                          ");
                        sqlString.AppendLine($@"          '{record.FAVORECIDO}',                          ");
                        sqlString.AppendLine($@"          {record.VALORDOCUMENTO})                      ");
                        sqlString.AppendLine($@"                                                     ");
                }
            }

            sqlString.AppendLine("  COMMIT TRAN;");
            sqlString.AppendLine("END TRY");
            sqlString.AppendLine("BEGIN CATCH");
            sqlString.AppendLine("  IF (@@TRANCOUNT > 0)");
            sqlString.AppendLine("  BEGIN");
            sqlString.AppendLine("          ROLLBACK TRAN;");
            sqlString.AppendLine("          THROW");
            sqlString.AppendLine("  END");
            sqlString.AppendLine("END CATCH");

            connection.Execute(sqlString.ToString());

            connection.Close();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Error: " + ex.ToString());
            connection.Close();
        }
    }
}
