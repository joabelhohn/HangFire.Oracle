using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;

namespace Hangfire.Oracle.JobQueue
{
    internal class OracleJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();
        private List<string> _queuesCache = new List<string>();
        private DateTime _cacheUpdated;

        private readonly OracleStorage _storage;
        public OracleJobQueueMonitoringApi(OracleStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    var result = _storage.UseConnection(connection =>
                    {
                        return connection.Query("select distinct(Queue) \"Queue\" from HANGFIRE_JOBQUEUE").Select(x => (string)x.Queue).ToList();
                    });

                    _queuesCache = result;
                    _cacheUpdated = DateTime.UtcNow;
                }

                return _queuesCache.ToList();
            }
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            string sqlQuery = @"
Select x.JobId from (select jq.JobId, rownum R
from HANGFIRE_JOBQUEUE jq
where jq.Queue = :queue
order by jq.Id) x
Where x.R Between :startValue and :endValue
";

            return _storage.UseConnection(connection =>
                connection.Query<int>(
                    sqlQuery,
                    new {queue = queue, startValue = @from + 1, endValue = @from + perPage}));
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return Enumerable.Empty<int>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<int>(
                        "select count(Id) from HANGFIRE_JOBQUEUE where Queue = :queue", new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result,
                };
            });
        }
    }
}