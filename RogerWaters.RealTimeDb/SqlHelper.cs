using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb
{
    public static class SqlHelper
    {
        public static void WithReader(this string sqlConnection, string commandText, Action<SqlDataReader> readAction)
        {
            sqlConnection.WithConnection(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = commandText;
                    using (var reader = command.ExecuteReader())
                    {
                        readAction(reader);
                    }
                }
            });
        }

        public static void WithConnection(this string sqlConnection, Action<SqlConnection> connectionAction)
        {
            using (var connection = new SqlConnection(sqlConnection))
            {
                connection.Open();
                connectionAction(connection);
            }
        }

        public static int ExecuteNonQuery(this SqlTransaction tran, string query)
        {
            using (var command = tran.Connection.CreateCommand())
            {
                command.CommandText = query;
                command.Transaction = tran;
                return command.ExecuteNonQuery();
            }
        }

        public static void EnableBroker(this string connection)
        {
            var builder = new SqlConnectionStringBuilder(connection);
            var db = builder.InitialCatalog;
            builder.InitialCatalog = "master";
            WithConnection(builder.ToString(), con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = $"ALTER DATABASE [{db}] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE";
                }
            });
        }

        public static IEnumerable<string> ReceiveMessages(this SqlTransaction tran, string queueName)
        {
            return ReceiveMessages(tran, queueName, TimeSpan.Zero);
        }

        public static IEnumerable<string> ReceiveMessages(this string connectionString, string queueName, TimeSpan timeoutMs)
        {
            IEnumerable<string> result = Enumerable.Empty<string>();
            connectionString.WithConnection(con =>
            {
                var transaction = con.BeginTransaction();
                try
                {
                    result = ReceiveMessages(transaction, queueName, timeoutMs).ToArray();
                    transaction.Commit();
                }
                finally
                {
                    if (transaction.Connection == null || transaction.Connection.State != ConnectionState.Closed)
                    {
                        transaction.Dispose();
                    }
                }
            });
            return result;
        }

        public static IEnumerable<string> ReceiveMessages(this SqlTransaction tran, string queueName, TimeSpan timeoutMs)
        {
            var script = LoadScript("ReceiveMessages");
            script = script.Replace("{queueName}", queueName);
            script = script.Replace("{timeoutMs}", timeoutMs.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
            using (var command = tran.Connection.CreateCommand())
            {
                command.CommandText = script;
                command.Transaction = tran;
                command.CommandTimeout = (int) timeoutMs.Add(TimeSpan.FromSeconds(2)).TotalSeconds;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return reader.GetString(0);
                    }
                }
            }
        }

        public static async Task<IEnumerable<string>> ReceiveMessagesAsync(this string connectionString, string queueName, TimeSpan timeoutMs)
        {
            List<string> messages = new List<string>();
            var script = LoadScript("ReceiveMessages");
            script = script.Replace("{queueName}", queueName);
            script = script.Replace("{timeoutMs}", timeoutMs.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
            using (var con = new SqlConnection(connectionString))
            {
                await con.OpenAsync();
                using (var tran = con.BeginTransaction())
                {
                    using (var command = tran.Connection.CreateCommand())
                    {
                        command.CommandText = script;
                        command.Transaction = tran;
                        command.CommandTimeout = (int)timeoutMs.Add(TimeSpan.FromSeconds(2)).TotalSeconds;
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (reader.Read())
                            {
                                messages.Add(reader.GetString(0));
                            }
                        }
                    }
                    tran.Commit();
                }
            }
            return messages;
        }

        public static void CreateContract(this SqlTransaction tran, string contractName, string messageTypeName)
        {
            var script = LoadScript("CreateContract");
            script = script.Replace("{contractName}", contractName);
            script = script.Replace("{messageTypeName}", messageTypeName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateQueue(this SqlTransaction tran, string queueName)
        {
            var script = LoadScript("CreateQueue");
            script = script.Replace("{queueName}", queueName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateViewCache(this SqlTransaction tran, string viewName, string cacheName)
        {
            var script = LoadScript("CreateViewCache");
            script = script.Replace("{viewName}", viewName);
            script = script.Replace("{cacheName}", cacheName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateViewCachePrimaryIndex(this SqlTransaction tran, string cacheName, params string[] primaryColumns)
        {
            var script = LoadScript("CreateViewCachePrimaryIndex");
            script = script.Replace("{cacheName}", cacheName);
            script = script.Replace("{primaryColumnList}", String.Join(", ",primaryColumns.Select(c => $"[{c}]")));
            tran.ExecuteNonQuery(script);
        }

        public static void MergeViewChanges(this string sqlConnection, string cacheName, string viewName, string[] keyColumns, string[] valueColumns)
        {
            var script = LoadScript("MergeViewChanges");
            script = script.Replace("{cacheName}", cacheName);
            script = script.Replace("{viewName}", viewName);
            script = script.Replace("{targetAlias}", "t");
            script = script.Replace("{sourceAlias}", "s");
            script = script.Replace
            (
                "{keyColumnsMatchCondition}", 
                String.Join(" AND ", keyColumns.Select(c => $"s.[{c}] = t.[{c}]"))
            );
            if (valueColumns.Length > 0)
            {
                script = script.Replace("{whenMatchedClause}", @"    WHEN MATCHED AND ({valueColumnsChangedCondition})
        THEN 
			UPDATE SET {valueColumnUpdates}");
                script = script.Replace
                (
                    "{valueColumnsChangedCondition}",
                    String.Join(" OR ", valueColumns.Select(c => $"s.[{c}] <> t.[{c}]"))
                );
                script = script.Replace
                (
                    "{valueColumnUpdates}",
                    String.Join(", ", valueColumns.Select(c => $"t.[{c}] = s.[{c}]"))
                );
            }
            else
            {
                script = script.Replace("{whenMatchedClause}", String.Empty);
            }
            script = script.Replace
            (
                "{allTargetCollumns}", 
                String.Join(", ", keyColumns.Union(valueColumns).Select(c => $"[{c}]"))
            );
            script = script.Replace
            (
                "{allSourceColumns}", 
                String.Join(", ", keyColumns.Union(valueColumns).Select(c => $"s.[{c}]"))
            );
            sqlConnection.WithConnection(con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = script;
                    command.ExecuteNonQuery();
                }
            });
        }

        public static void CreateService(this SqlTransaction tran, string serviceName, string queueName, string contractName)
        {
            var script = LoadScript("CreateService");
            script = script.Replace("{serviceName}", serviceName);
            script = script.Replace("{queueName}", queueName);
            script = script.Replace("{contractName}", contractName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateTriggerDelete(this SqlTransaction tran, string triggerName, string tableName, string dialogHandle, string messageType)
        {
            tran.CreateTrigger("CreateDeleteTrigger", triggerName, tableName,dialogHandle, messageType);
        }
        public static void CreateTriggerInsert(this SqlTransaction tran, string triggerName, string tableName, string dialogHandle, string messageType)
        {
            tran.CreateTrigger("CreateInsertTrigger", triggerName, tableName,dialogHandle, messageType);
        }

        public static void CreateTriggerUpdate(this SqlTransaction tran, string triggerName, string tableName, string dialogHandle, string messageType)
        {
            tran.CreateTrigger("CreateUpdateTrigger",triggerName,tableName,dialogHandle, messageType);
        }

        public static void DropTrigger(this SqlTransaction tran, string triggerName)
        {
            tran.ExecuteNonQuery($"DROP TRIGGER [{triggerName}]");
        }

        private static void CreateTrigger(this SqlTransaction tran, string triggerResource, string triggerName, string tableName, string dialogHandle, string messageType)
        {
            var script = LoadScript(triggerResource);
            script = script.Replace("{triggerName}", triggerName);
            script = script.Replace("{tableName}", tableName);
            script = script.Replace("{dialogHandle}", dialogHandle);
            script = script.Replace("{messageType}", messageType);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateMessageType(this SqlTransaction tran, string messageTypeName)
        {
            var script = LoadScript("CreateMessageType");
            script = script.Replace("{messageTypeName}", messageTypeName);
            tran.ExecuteNonQuery(script);
        }

        public static void DropMessageType(this SqlTransaction tran, string messageTypeName)
        {
            tran.ExecuteNonQuery($"DROP MESSAGE TYPE [{messageTypeName}]");
        }

        public static void DropContract(this SqlTransaction tran, string contract)
        {
            tran.ExecuteNonQuery($"DROP CONTRACT [{contract}]");
        }
        
        public static void DropService(this SqlTransaction tran, string serviceName)
        {
            tran.ExecuteNonQuery($"DROP SERVICE [{serviceName}]");
        }

        public static void DropQueue(this SqlTransaction tran, string queueName)
        {
            tran.ExecuteNonQuery($"DROP QUEUE [{queueName}]");
        }

        public static void EndConversation(this SqlTransaction tran, Guid conversation)
        {
            tran.ExecuteNonQuery($"DECLARE @dialog_handle UNIQUEIDENTIFIER = '{conversation}';{Environment.NewLine}END CONVERSATION @dialog_handle WITH CLEANUP;");
        }

        public static Guid GetConversation(this SqlTransaction tran, string senderServiceName, string recieverServiceName, string contractName)
        {
            var script = LoadScript("GetConversation");
            script = script.Replace("{recieverServiceName}", recieverServiceName);
            script = script.Replace("{senderServiceName}", senderServiceName);
            script = script.Replace("{contractName}", contractName);
            using (var command = tran.Connection.CreateCommand())
            {
                command.CommandText = script;
                command.Transaction = tran;
                return (Guid) command.ExecuteScalar();
            }
        }

        private static string LoadScript(string name)
        {
            var assembly = typeof(SqlHelper).Assembly;
            var ressourceName = $@"{assembly.GetName().Name}.Scripts.{name}.Sql";
            using (var stream = assembly.GetManifestResourceStream(ressourceName) ?? throw new ArgumentNullException(nameof(name),"Ressource not found"))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
