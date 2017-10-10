﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Transactions;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Oracle.JobQueue;
using Hangfire.Oracle.Monitoring;
using Hangfire.Server;
using Hangfire.Storage;
using IsolationLevel = System.Transactions.IsolationLevel;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle
{
    public class OracleStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof (OracleStorage));

        private readonly string _connectionString;
        private readonly OracleConnection _existingConnection;
        private readonly OracleStorageOptions _options;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public OracleStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new OracleStorageOptions())
        {
        }

        public OracleStorage(string nameOrConnectionString, OracleStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException("nameOrConnectionString");
            if (options == null) throw new ArgumentNullException("options");

            if (IsConnectionString(nameOrConnectionString))
            {
                _connectionString = ApplyAllowUserVariablesProperty(nameOrConnectionString);
            }
            else if (IsConnectionStringInConfiguration(nameOrConnectionString))
            {
                _connectionString = 
                    ApplyAllowUserVariablesProperty(
                        ConfigurationManager.ConnectionStrings[nameOrConnectionString].ConnectionString);
            }
            else
            {
                throw new ArgumentException(
                    string.Format(
                        "Could not find connection string with name '{0}' in application config file",
                        nameOrConnectionString));
            }
            _options = options;

            if (options.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    OracleObjectsInstaller.Install(connection);
                }
            }

            InitializeQueueProviders();
        }

        private string ApplyAllowUserVariablesProperty(string connectionString)
        {
            if (connectionString.ToLower().Contains("allow user variables"))
            {
                return connectionString;
            }

            return connectionString + ";Allow User Variables=True;";
        }

        internal OracleStorage(OracleConnection existingConnection)
        {
            if (existingConnection == null) throw new ArgumentNullException("existingConnection");

            _existingConnection = existingConnection;
            _options = new OracleStorageOptions();

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            QueueProviders = 
                new PersistentJobQueueProviderCollection(
                    new OracleJobQueueProvider(this, _options));
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var parts = _connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x[1].Trim() })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0) builder.Append("@");

                foreach (var alias in new[] { "Database", "Initial Catalog" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                return builder.Length != 0
                    ? String.Format("Server: {0}", builder)
                    : canNotParseMessage;
            }
            catch (Exception ex)
            {
                Logger.ErrorException(ex.Message, ex);
                return canNotParseMessage;
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new OracleStorageConnection(this);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        private bool IsConnectionStringInConfiguration(string connectionStringName)
        {
            var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringName];

            return connectionStringSetting != null;
        }

        internal void UseTransaction([InstantHandle] Action<OracleConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
            [InstantHandle] Func<OracleConnection, T> func, IsolationLevel? isolationLevel)
        {
            using (var transaction = CreateTransaction(isolationLevel ?? _options.TransactionIsolationLevel))
            {
                var result = UseConnection(func);
                transaction.Complete();

                return result;
            }
        }
        private TransactionScope CreateTransaction(IsolationLevel? isolationLevel)
        {
            var trnScope = isolationLevel != null
                ? new TransactionScope(TransactionScopeOption.Required,
                    new TransactionOptions { IsolationLevel = isolationLevel.Value, Timeout = _options.TransactionTimeout })
                : new TransactionScope();
            return trnScope;
        }

        internal void UseConnection([InstantHandle] Action<OracleConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<OracleConnection, T> func)
        {
            OracleConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal OracleConnection CreateAndOpenConnection()
        {
            if (_existingConnection != null)
            {
                return _existingConnection;
            }

            var connection = new OracleConnection(_connectionString);
            connection.Open();
            
            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !ReferenceEquals(connection, _existingConnection))
            {
                connection.Dispose();
            }
        }
        public void Dispose()
        {
        }
    }
}
