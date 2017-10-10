using System;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.Oracle.JobQueue;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests.JobQueue
{
    public class OracleJobQueueMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly OracleJobQueueMonitoringApi _sut;
        private readonly OracleStorage _storage;
        private readonly OracleConnection _connection;
        private readonly string _queue = "default";

        public OracleJobQueueMonitoringApiTests()
        {
            _connection = new OracleConnection(ConnectionUtils.GetConnectionString());
            _connection.Open();

            _storage = new OracleStorage(_connection);

            _sut = new OracleJobQueueMonitoringApi(_storage);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedAndFetchedCount_ReturnsEqueuedCount_WhenExists()
        {
            EnqueuedAndFetchedCountDto result = null;
            
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (1, :queue)", 
                    new { queue = _queue });

                result = _sut.GetEnqueuedAndFetchedCount(_queue);

                connection.Execute("delete from HANGFIRE_JOBQUEUE");
            });

            Assert.Equal(1, result.EnqueuedCount);
        }


        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            var result = _sut.GetEnqueuedJobIds(_queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            int[] result = null;
            _storage.UseConnection(connection =>
            {
                for (var i = 1; i <= 10; i++)
                {
                    connection.Execute(
                        "insert into HANGFIRE_JOBQUEUE (JobId, Queue) " +
                        "values (:jobId, :queue)", new {jobId = i, queue = _queue});
                }

                result = _sut.GetEnqueuedJobIds(_queue, 3, 2).ToArray();

                connection.Execute("delete from HANGFIRE_JOBQUEUE");
            });
            
            Assert.Equal(2, result.Length);
            Assert.Equal(4, result[0]);
            Assert.Equal(5, result[1]);
        }
    }
}
