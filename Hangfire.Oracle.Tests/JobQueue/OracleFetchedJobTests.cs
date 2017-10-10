using System;
using System.Data;
using Dapper;
using Hangfire.Oracle.JobQueue;
using Moq;
using Xunit;

namespace Hangfire.Oracle.Tests.JobQueue
{
    public class OracleFetchedJobTests : IClassFixture<TestDatabaseFixture>
    {
        private const int JobId = 1;
        private const string Queue = "queue";

        private readonly Mock<IDbConnection> _connection;
        private readonly Mock<OracleStorage> _storage;
        private readonly int _id = 0;
        private readonly FetchedJob _fetchedJob;

        public OracleFetchedJobTests()
        {
            _fetchedJob = new FetchedJob(){Id = _id, JobId = JobId, Queue = Queue};
            _connection = new Mock<IDbConnection>();
            var options = new OracleStorageOptions { PrepareSchemaIfNecessary = false };
            _storage = new Mock<OracleStorage>(ConnectionUtils.GetConnectionString(), options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleFetchedJob(null, _connection.Object, _fetchedJob));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleFetchedJob(_storage.Object, null, _fetchedJob));

            Assert.Equal("connection", exception.ParamName);
        }
        
        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new OracleFetchedJob(_storage.Object, _connection.Object, _fetchedJob);

            Assert.Equal(JobId.ToString(), fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        private OracleFetchedJob CreateFetchedJob(int jobId, string queue)
        {
            return new OracleFetchedJob(_storage.Object, _connection.Object,
                new FetchedJob() {JobId = jobId, Queue = queue, Id = _id});
        }
    }
}
