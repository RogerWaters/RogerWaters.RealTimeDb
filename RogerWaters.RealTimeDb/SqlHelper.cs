﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.SqlObjects;

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
                command.CommandTimeout = 0;
                return command.ExecuteNonQuery();
            }
        }

        public static int ExecuteNonQuery(this SqlConnection con, string query)
        {
            using (var command = con.CreateCommand())
            {
                command.CommandText = query;
                command.CommandTimeout = 0;
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

        public static IEnumerable<string> ReceiveMessages(this SqlTransaction tran, SqlObjectName queueName)
        {
            return ReceiveMessages(tran, queueName, TimeSpan.Zero);
        }

        public static IEnumerable<string> ReceiveMessages(this string connectionString, SqlObjectName queueName, TimeSpan timeoutMs)
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

        public static IEnumerable<string> ReceiveMessages(this SqlTransaction tran, SqlObjectName queueName, TimeSpan timeoutMs)
        {
            var script = LoadScript("ReceiveMessages");
            script = script.Replace("{queueName}", queueName);
            script = script.Replace("{timeoutMs}", timeoutMs.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
            using (var command = tran.Connection.CreateCommand())
            {
                command.CommandText = script;
                command.Transaction = tran;
                command.CommandTimeout = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return reader.GetString(0);
                    }
                }
            }
        }

        public static async Task<IEnumerable<string>> ReceiveMessagesAsync(this string connectionString, SqlObjectName queueName, TimeSpan timeoutMs)
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

        public static void CreateContract(this SqlTransaction tran, SqlSchemalessObjectName contractName, SqlSchemalessObjectName messageTypeName)
        {
            var script = LoadScript("CreateContract");
            script = script.Replace("{contractName.Name}", contractName.Name);
            script = script.Replace("{contractName}", contractName);
            script = script.Replace("{messageTypeName}", messageTypeName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateQueue(this SqlTransaction tran, SqlObjectName queueName)
        {
            var script = LoadScript("CreateQueue");
            script = script.Replace("{queueName}", queueName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateViewCache(this string connectionString, SqlObjectName viewName, SqlObjectName cacheName, params string[] primaryColumns)
        {
            var script = LoadScript("CreateViewCache");
            script = script.Replace("{viewName}", viewName);
            script = script.Replace("{cacheName}", cacheName);
            script = script.Replace("{cacheName.Schema}", cacheName.Schema);
            script = script.Replace("{cacheName.Name}", cacheName.Name);
            script = script.Replace("{primaryColumnList}", String.Join(", ",primaryColumns.Select(c => $"[{c}]")));
            connectionString.WithConnection(con => { con.ExecuteNonQuery(script); });
        }

        public static void CreateMemoryViewCache(this string connectionString, SqlObjectName viewName, SqlObjectName cacheName, params string[] primaryColumns)
        {
            var script = LoadScript("CreateMemoryViewCache");
            script = script.Replace("{viewName}", viewName);
            script = script.Replace("{cacheName}", cacheName);
            script = script.Replace("{cacheName.Schema}", cacheName.Schema);
            script = script.Replace("{cacheName.Name}", cacheName.Name);
            script = script.Replace("{primaryColumnList}", String.Join(", ",primaryColumns.Select(c => $"[{c}]")));
            connectionString.WithConnection(con => { con.ExecuteNonQuery(script); });
        }
        

        public static IReadOnlyCollection<string> GetPrimaryKeyColumns(this string sqlConnectionString, SqlObjectName tableName)
        {
            var script = LoadScript("GetPrimaryIndexes");
            script = script.Replace("{tableName}", tableName);
            List<Tuple<string,string,string>> indexedPrimaryColumns = new List<Tuple<string, string, string>>();
            sqlConnectionString.WithReader(script, reader =>
            {
                while (reader.Read())
                {
                    indexedPrimaryColumns.Add
                    (
                        new Tuple<string, string, string>
                        (
                            reader.GetString(0),
                            reader.GetString(1),
                            reader.GetString(2)
                        )
                    );
                }
            });
            var columns = indexedPrimaryColumns.Where(c => c.Item2 == "PK").Select(c => c.Item3).ToList();
            if (columns.Count > 0)
            {
                return columns;
            }
            var someKeys = indexedPrimaryColumns.Where(c => c.Item2 == "UX").GroupBy(c => c.Item1,c => c.Item3).FirstOrDefault();
            if (someKeys != null)
            {
                return someKeys.ToList();
            }
            return new List<string>();
        }

        public static void MergeViewChanges(this string sqlConnection, SqlObjectName cacheName, SqlObjectName viewName, string[] keyColumns, string[] valueColumns)
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
                    command.CommandTimeout = 0;
                    command.CommandText = script;
                    command.ExecuteNonQuery();
                }
            });
        }

        public static void MergeViewChanges
        (
            this string sqlConnection, 
            SqlObjectName cacheName, 
            SqlObjectName viewName, 
            string[] keyColumns, 
            string[] valueColumns,
            string[] notNullColumns
        )
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
                    command.CommandTimeout = 0;
                    command.CommandText = script;
                    command.ExecuteNonQuery();
                }
            });
        }

        public static void CreateService(this SqlTransaction tran, SqlSchemalessObjectName serviceName, SqlObjectName queueName, SqlSchemalessObjectName contractName)
        {
            var script = LoadScript("CreateService");
            script = script.Replace("{serviceName.Name}", serviceName.Name);
            script = script.Replace("{serviceName}", serviceName);
            script = script.Replace("{queueName}", queueName);
            script = script.Replace("{contractName}", contractName);
            tran.ExecuteNonQuery(script);
        }

        public static void CreateTriggerDelete(this SqlTransaction tran, SqlObjectName triggerName, SqlObjectName tableName, string dialogHandle, SqlSchemalessObjectName messageType, bool withNative)
        {
            tran.CreateTrigger("CreateDeleteTrigger", triggerName, tableName,dialogHandle, messageType,withNative);
        }
        public static void CreateTriggerInsert(this SqlTransaction tran, SqlObjectName triggerName, SqlObjectName tableName, string dialogHandle, SqlSchemalessObjectName messageType, bool withNative)
        {
            tran.CreateTrigger("CreateInsertTrigger", triggerName, tableName,dialogHandle, messageType,withNative);
        }

        public static void CreateTriggerUpdate(this SqlTransaction tran, SqlObjectName triggerName, SqlObjectName tableName, string dialogHandle, SqlSchemalessObjectName messageType, bool withNative)
        {
            tran.CreateTrigger("CreateUpdateTrigger",triggerName,tableName,dialogHandle, messageType,withNative);
        }

        public static void DropTrigger(this SqlTransaction tran, SqlObjectName triggerName)
        {
            tran.ExecuteNonQuery($"DROP TRIGGER {triggerName}");
        }

        private static void CreateTrigger(this SqlTransaction tran, string triggerResource, SqlObjectName triggerName, SqlObjectName tableName, string dialogHandle, SqlSchemalessObjectName messageType, bool withNative)
        {
            var script = LoadScript(triggerResource);
            script = script.Replace("{triggerName}", triggerName);
            script = script.Replace("{tableName.Schema}", tableName.Schema);
            script = script.Replace("{tableName.Name}", tableName.Name);
            script = script.Replace("{tableName}", tableName);
            script = script.Replace("{dialogHandle}", dialogHandle);
            script = script.Replace("{messageType}", messageType);
            if (withNative)
            {
                script = script.Replace("{with}", "WITH NATIVE_COMPILATION, SCHEMABINDING");
                script = script.Replace("{as}", "BEGIN ATOMIC WITH ( TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english' )");
            }
            else
            {
                script = script.Replace("{with}", String.Empty);
                script = script.Replace("{as}", String.Empty);
            }
            tran.ExecuteNonQuery(script);
        }

        public static void CreateMessageType(this SqlTransaction tran, SqlSchemalessObjectName messageTypeName)
        {
            var script = LoadScript("CreateMessageType");
            script = script.Replace("{messageTypeName.Name}", messageTypeName.Name);
            script = script.Replace("{messageTypeName}", messageTypeName);
            tran.ExecuteNonQuery(script);
        }

        public static void DropMessageType(this SqlTransaction tran, SqlSchemalessObjectName messageTypeName)
        {
            tran.ExecuteNonQuery($"DROP MESSAGE TYPE {messageTypeName}");
        }

        public static void DropContract(this SqlTransaction tran, SqlSchemalessObjectName contract)
        {
            tran.ExecuteNonQuery($"DROP CONTRACT {contract}");
        }
        
        public static void DropService(this SqlTransaction tran, SqlSchemalessObjectName serviceName)
        {
            tran.ExecuteNonQuery($"DROP SERVICE {serviceName}");
        }

        public static void DropQueue(this SqlTransaction tran, SqlObjectName queueName)
        {
            tran.ExecuteNonQuery($"DROP QUEUE {queueName}");
        }

        public static void EndConversation(this SqlTransaction tran, Guid conversation)
        {
            tran.ExecuteNonQuery($"DECLARE @dialog_handle UNIQUEIDENTIFIER = '{conversation}';{Environment.NewLine}END CONVERSATION @dialog_handle WITH CLEANUP;");
        }

        public static Guid GetConversation(this SqlTransaction tran, SqlSchemalessObjectName senderServiceName, SqlSchemalessObjectName recieverServiceName, SqlSchemalessObjectName contractName)
        {
            var script = LoadScript("GetConversation");
            script = script.Replace("{recieverServiceName.Name}", recieverServiceName.Name);
            script = script.Replace("{recieverServiceName}", recieverServiceName);
            script = script.Replace("{senderServiceName.Name}", senderServiceName.Name);
            script = script.Replace("{senderServiceName}", senderServiceName);
            script = script.Replace("{contractName}", contractName);
            using (var command = tran.Connection.CreateCommand())
            {
                command.CommandText = script;
                command.Transaction = tran;
                return (Guid) command.ExecuteScalar();
            }
        }

        internal static string LoadScript(string name)
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
