﻿using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Oracle
{
    internal class CountersAggregator : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly OracleStorage _storage;
        private readonly TimeSpan _interval;

        public CountersAggregator(OracleStorage storage, TimeSpan interval)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _interval = interval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");

            int removedCount = 0;

            do
            {
                _storage.UseConnection(connection =>
                {
                    removedCount = connection.Execute(
                        GetAggregationQuery(),
                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private static string GetAggregationQuery()
        {
            //TODO
            return @"
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

INSERT INTO HANGFIRE_AGGREGATEDCOUNTER (KEY, VALUE, ExpireAt)
    SELECT Key, SUM(Value) as Value, MAX(ExpireAt) AS ExpireAt 
    FROM (
            SELECT KEY, VALUE, ExpireAt
            FROM HANGFIRE_COUNTER
            LIMIT @count) tmp
	GROUP BY Key
        ON DUPLICATE KEY UPDATE 
            Value = Value + VALUES(Value),
            ExpireAt = GREATEST(ExpireAt,VALUES(ExpireAt));

DELETE FROM HANGFIRE_COUNTER
LIMIT @count;

COMMIT;";
        }
    }
}
