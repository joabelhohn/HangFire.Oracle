using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly string[] ProcessedTables =
        {
            "HANGFIRE_AGGREGATEDCOUNTER",
            "HANGFIRE_JOB",
            "HANGFIRE_LIST",
            "HANGFIRE_SET",
            "HANGFIRE_HASH",
        };

        private readonly OracleStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(OracleStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(OracleStorage storage, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            Logger.DebugFormat("delete from {0} where ExpireAt < :now and rownum <= :count;", table);

                            using (
                                new OracleDistributedLock(
                                    connection, 
                                    DistributedLockKey, 
                                    DefaultLockTimeout,
                                    cancellationToken).Acquire())
                            {

                                var param = new DynamicParameters();
                                param.Add("now", DateTime.UtcNow);//, System.Data.DbType.DateTime, System.Data.ParameterDirection.Input);
                                param.Add("countValue", NumberOfRecordsInSinglePass);//, System.Data.DbType.DateTime, System.Data.ParameterDirection.Input);
                                var sql = String.Format(@"DELETE FROM {0} WHERE EXPIREAT < :now and rownum <= :countValue", table);
                                removedCount = connection.Execute(sql, param);
                            }

                            Logger.DebugFormat("removed records count={0}",removedCount);
                        }
                        catch (OracleException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            table));

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
