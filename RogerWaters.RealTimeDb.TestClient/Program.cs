using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.Linq2Sql;

namespace RogerWaters.RealTimeDb.TestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            DoWithDb();
            //DoWithDbStressTest();
            //DefaultContex();
            Console.WriteLine("Enter to close");
            Console.ReadLine();
        }

        public static void DoWithDb()
        {
            using (var context = new RealtimeDbDataContext<SomeDbDataContext>(() => new SomeDbDataContext()))
            {
                var queries =
                    Enumerable.Range(0, 100).AsParallel().Select(i => context.Query
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

                Console.ReadLine();
                foreach (var query in queries)
                {
                    query.Result.Dispose();
                }
            }
        }

        public static void DoWithDbStressTest()
        {
            using (var context = new RealtimeDbDataContext<TPCCDataContext>(() => new TPCCDataContext()))
            {
                Console.ReadLine();
                context.DisposeBehavior = DisposeBehavior.CleanupSchema;
            }
        }
    }
}
