using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.JobQueue
{
    internal class OracleJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleJobQueue));

        private readonly OracleStorage _storage;
        private readonly OracleStorageOptions _options;
        public OracleJobQueue(OracleStorage storage, OracleStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _storage = storage;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            FetchedJob fetchedJob = null;
            OracleConnection connection = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection = _storage.CreateAndOpenConnection();
                
                try
                {
                    using (new OracleDistributedLock(_storage, "JobQueue", TimeSpan.FromSeconds(30)))
                    {
                        string token = Guid.NewGuid().ToString();

                        var timeout = _options.InvisibilityTimeout.Negate().TotalSeconds;
                        int nUpdated = connection.Execute(
                            $@" update HANGFIRE_JOBQUEUE set FetchedAt = sysdate, FetchToken = :fetchToken
where(FetchedAt is null or FetchedAt < (sysdate + INTERVAL '{timeout}' SECOND))
and Queue in :queues
and rownum = 1",
                            new
                            {
                                queues = queues,
                                fetchToken = token
                            });

                        if(nUpdated != 0)
                        {
                            fetchedJob =
                                connection
                                    .Query<FetchedJob>(
                                        "select Id, JobId, Queue from HANGFIRE_JOBQUEUE where FetchToken = :fetchToken",
                                        new
                                        {
                                            fetchToken = token
                                        })
                                    .SingleOrDefault();
                        }
                    }
                }
                catch (OracleException ex)
                {
                    Logger.ErrorException(ex.Message, ex);
                    _storage.ReleaseConnection(connection);
                    throw;
                }

                if (fetchedJob == null)
                {
                    _storage.ReleaseConnection(connection);

                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new OracleFetchedJob(_storage, connection, fetchedJob);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            connection.Execute("insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (:jobId, :queue)", new {jobId, queue});
        }
    }
}