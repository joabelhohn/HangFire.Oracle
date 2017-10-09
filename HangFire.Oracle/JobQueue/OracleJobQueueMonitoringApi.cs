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
                        return connection.Query("select distinct(Queue) from HANGFIRE_JOBQUEUE").Select(x => (string)x.Queue).ToList();
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
select jq.JobId
from HANGFIRE_JOBQUEUE jq
where jq.Queue = :queue
and rownum between :start and :end
order by jq.Id
";

            return _storage.UseConnection(connection =>
                connection.Query<int>(
                    sqlQuery,
                    new {queue = queue, start = @from + 1, end = @from + perPage}));
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