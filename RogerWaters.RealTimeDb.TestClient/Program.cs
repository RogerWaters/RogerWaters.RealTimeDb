using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace RogerWaters.RealTimeDb.TestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = "LOCALHOST",
                IntegratedSecurity = true,
                InitialCatalog = "SomeDB"
            };
            var connectionString = builder.ToString();

            var config = new DatabaseConfig(connectionString);
            using (var database = config.SetupDatabase())
            {
                database.GetOrAddTable("MyTable");
                database.GetOrAddView("asd", "MyTable_id");

                foreach (var table in database.Tables)
                {
                    table.OnTableDataChanged += Table_OnTableDataChanged;
                }
                foreach (var view in database.Views)
                {
                    view.OnTableDataChanged += Table_OnTableDataChanged;
                }

                using (var query = database.CustomQuery(new CountAsd()))
                {
                    Console.WriteLine("Enter to close query");
                    Console.ReadLine();
                }
                

                Console.WriteLine("Enter to escape");
                Console.ReadLine();
                database.CleanupSchemaChanges();
            }
        }

        private static void Table_OnTableDataChanged(object sender, EventArgs.TableDataChangedEventArgs e)
        {
            if (sender is Table table)
            {
                Console.WriteLine(table.TableName);
                Console.WriteLine("Got {0} for {1} rows",e.ChangeType,e.Rows.Count);
            }
            if (sender is View view)
            {
                Console.WriteLine(view.ViewName);
                Console.WriteLine("Got {0} for {1} rows",e.ChangeType,e.Rows.Count);
            }
        }
    }

    class CountAsd : UserQuery
    {
        public int Count { get; private set; }
        public override string CommandText => "SELECT ISNULL(COUNT(*),0) AS NumberOfRows FROM asd";
        public override string PrimaryKeyColumn => "NumberOfRows";
        public override string[] AdditionalPrimaryKeyColumns { get; }

        public override void Initialize(SqlDataReader obj)
        {
            Console.WriteLine("Initialized");
            if (obj.Read())
            {
                Count = (int) obj[0];
            }
        }

        public override void RowsInserted(IReadOnlyList<Row> rows)
        {
            Console.WriteLine("Rows Inserted");
            Count = (int) rows[0][0];
        }

        public override void RowsUpdated(IReadOnlyList<Row> rows)
        {
            Console.WriteLine("Rows updated");
            Count = (int)rows[0][0];
        }

        public override void RowsDeleted(IReadOnlyList<Row> rows)
        {
            Console.WriteLine("Rows Deleted");
        }
    }
}
