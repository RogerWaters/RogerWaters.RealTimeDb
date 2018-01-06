using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb.Scripts.Queries
{
    internal sealed class MergeSelectViewChanges
    {
        private static readonly string _script = SqlHelper.LoadScript(nameof(MergeSelectViewChanges));

        private readonly string _replacedScript;

        public MergeSelectViewChanges(SqlObjectName cacheName, SqlObjectName viewName, string[] allTargetCollumns, string[] keyColumns, string[] notNullColumns)
        {
            string sc = _script;
            foreach (var replacement in BuildReplacements(cacheName,viewName,allTargetCollumns,keyColumns,notNullColumns))
            {
                sc = sc.Replace($"{{{replacement.Item1}}}", replacement.Item2);
            }

            _replacedScript = sc;
        }

        private IEnumerable<(string, string)> BuildReplacements(SqlObjectName cacheName, SqlObjectName viewName, string[] allTargetCollumns, string[] keyColumns, string[] notNullColumns)
        {
            yield return ("cacheName", cacheName);
            yield return ("viewName", viewName);
            yield return 
            (
                "allTargetCollumns", 
                String.Join
                (
                    ", ",
                    allTargetCollumns.Select(c => $"[{c}]")
                )
            );
            yield return ("sourceAlias", "s");
            yield return ("targetAlias", "t");
            yield return ("keyColumnsMatchCondition", BuildKeyColumnsMatchCondition(keyColumns));
            yield return ("firstNotNullPrimaryKeyColumn", $"[{keyColumns.Intersect(notNullColumns).First()}]");
            yield return ("valuesUpdate", BuildValuesUpdate(allTargetCollumns,keyColumns));
            yield return ("notMatchedValuesWithNullCheck", NotMatchedValuesWithNullCheck(allTargetCollumns,keyColumns,notNullColumns));
        }

        private string BuildKeyColumnsMatchCondition(string[] keyColumns)
        {
            return string.Join(" AND ", keyColumns.Select(key => $"[s].[{key}] = [t].[{key}]"));
        }

        private string BuildValuesUpdate(string[] allTargetCollumns, string[] keyColumns)
        {
            return string.Join(", ", allTargetCollumns.Except(keyColumns).Select(c => $"[{c}] = [s].[{c}]"));
        }
        private string NotMatchedValuesWithNullCheck(string[] allTargetCollumns, string[] keyColumns, string[] notNullColumns)
        {
            return string.Join
            (
                " AND ", 
                allTargetCollumns
                    .Except(keyColumns)
                    .Select
                    (
                        c => 
                            notNullColumns.Contains(c) ? 
                                $"[t].[{c}] <> [s].[{c}]" : 
                                $"([t].[{c}] <> [s].[{c}] OR ([t].[{c}] IS NULL AND [s].[{c}] IS NOT NULL) OR ([t].[{c}] IS NOT NULL AND [s].[{c}] IS NULL))"
                    )
            );
        }

        public void Execute(string sqlConnection,Action<SqlDataReader> insert, Action<SqlDataReader> update, Action<SqlDataReader> delete)
        {
            sqlConnection.WithReader(_replacedScript, reader =>
            {
                insert(reader);
                reader.NextResult();
                update(reader);
                reader.NextResult();
                delete(reader);
            });
        }
    }
}
