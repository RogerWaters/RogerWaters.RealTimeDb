using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.Linq2Sql;
using RogerWaters.RealTimeDb.SqlObjects.Caching;

namespace RogerWaters.RealTimeDb.TestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            DoWithDb(CachingType.InMemory);
            DoWithDb(CachingType.SqlTable);
            DoWithDb(CachingType.SqlInMemoryTable);
            //DoWithDbStressTest();
            //DefaultContex();
            Console.WriteLine("Enter to close");
            Console.ReadLine();
        }

        public static void DoWithDb(CachingType cachingType)
        {
            Console.WriteLine(cachingType);
            var stopwatch = Stopwatch.StartNew();
            using (
            var context = new RealtimeDbDataContextBuilder<SomeDbDataContext>(() => new SomeDbDataContext()){CachingType = cachingType}.Build()
            )
            {
                Console.WriteLine("Context Created {0}ms",stopwatch.Elapsed.TotalMilliseconds);

                context.DatabaseConfig.DatabaseConnectionString.WithConnection(con => con.ExecuteNonQuery(@"
UPDATE MyTable
SET [some] = NULL
WHERE [some] = 'Merge'"));

                context.DatabaseConfig.DatabaseConnectionString.WithConnection(con => con.ExecuteNonQuery(@"
UPDATE MyTable2
SET [some] = NULL
WHERE [some] = 'Merge'"));

                context.DatabaseConfig.DatabaseConnectionString.WithConnection(con => con.ExecuteNonQuery(@"
UPDATE TOP (1) MyTable2
SET [some] = 'Merge'
WHERE [some] IS NULL"));

                context.DatabaseConfig.DatabaseConnectionString.WithConnection(con => con.ExecuteNonQuery(@"
UPDATE TOP (2000) MyTable
SET [some] = 'Merge'
WHERE [some] IS NULL"));

                Thread.Sleep(100);

                stopwatch.Restart();

                var queries =
                    Enumerable.Range(0, 1000).AsParallel().Select(i => context.Query
                    (
                        c =>
                            from m in c.MyTable
                            join m2 in c.MyTable2 on m.some equals m2.some
                            where m.some != null && m2.some != "asd"
                            where m.some == "Merge"
                            select new
                            {
                                m.MyTable_id,
                                m.some,
                                m2.MyTable2_id,
                                Some2 = m2.some
                            },
                        r => new
                        {
                            r.MyTable_id,
                            r.MyTable2_id
                        }
                    )).ToArray();
                Console.WriteLine("Queries created {0}ms",stopwatch.Elapsed.TotalMilliseconds);
                stopwatch.Restart();
                Task.WaitAll(queries);

                var results = queries.Select(q => q.Result).ToList();
                Console.WriteLine("Queries loaded  {0}ms",stopwatch.Elapsed.TotalMilliseconds);
                stopwatch.Restart();
                var reference = results.First();
                var count = reference.Count();
                if (results.All(q => q.Count() == count) == false)
                {
                    Console.WriteLine("Queries have different count");
                }

                context.DatabaseConfig.DatabaseConnectionString.WithConnection(con => con.ExecuteNonQuery(@"
UPDATE TOP (1000) MyTable
SET [some] = 'Merge'
WHERE [some] IS NULL"));
                count+=1000;

                while (true)
                {
                    Thread.Sleep(100);
                    if (results.All(q => q.Count() == count))
                    {
                        break;
                    }
                }
                Console.WriteLine("Wait for Sync after change {0}ms",stopwatch.Elapsed.TotalMilliseconds);
                stopwatch.Restart();

                queries.AsParallel().ForAll(q => q.Dispose());
                Console.WriteLine("Dispose Queries {0}ms",stopwatch.Elapsed.TotalMilliseconds);
                stopwatch.Restart();
            }

            Console.WriteLine("Dispose {0}ms",stopwatch.Elapsed.TotalMilliseconds);
        }

        public static void DoWithDbStressTest()
        {
            using (var context = new RealtimeDbDataContextBuilder<TPCCDataContext>(() => new TPCCDataContext()).Build())
            {
                Console.ReadLine();
                context.DisposeBehavior = DisposeBehavior.CleanupSchema;
            }
        }
    }
}
