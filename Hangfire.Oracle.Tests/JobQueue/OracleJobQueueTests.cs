using System;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Oracle.JobQueue;
using Moq;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests.JobQueue
{
    public class OracleJobQueueTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private static readonly string[] DefaultQueues = { "default" };
        private readonly OracleStorage _storage;
        private readonly OracleConnection _connection;

        public OracleJobQueueTests()
        {
            _connection = ConnectionUtils.CreateConnection();
            _storage = new OracleStorage(_connection);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleJobQueue(null, new OracleStorageOptions()));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleJobQueue(_storage, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (:jobId, :queue) returning ID into :id";

            // Arrange
            _storage.UseConnection(connection =>
            {
                var param = new DynamicParameters();
                param.Add(name: "jobId", value: 1, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "queue", value: "default", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "id", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

                connection.Execute(arrangeSql, param);
                var id = (int)param.Get<int>("id");
                var queue = CreateJobQueue(connection);

                // Act
                var payload = (OracleFetchedJob)queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldDeleteAJob()
        {
            const string arrangeSql = @"
declare
    jobid number(11);    
begin 
    delete from HANGFIRE_JOBQUEUE;
    delete from HANGFIRE_JOB;    
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt) values (:invocationData, :arguments, sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (jobid, :queue);
end;";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new { invocationData = " ", arguments = " ", queue = "default" });

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                payload.RemoveFromQueue();

                // Assert
                Assert.NotNull(payload);

                var jobInQueue = connection.Query("select * from HANGFIRE_JOBQUEUE").SingleOrDefault();
                Assert.Null(jobInQueue);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            const string arrangeSql = @"
declare
    jobid number(11);    
begin 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt) values (:invocationData, :arguments, sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBQUEUE (JobId, Queue, FetchedAt) values (jobid, :queue, :fetchedAt);
end;";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new
                    {
                        queue = "default",
                        fetchedAt = DateTime.UtcNow.AddDays(-1),
                        invocationData = " ",
                        arguments = " "
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            const string arrangeSql = @"
declare
    jobid number(11);    
begin 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt) values (:invocationData, :arguments, sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (jobid, :queue);
end;";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"begin 
    delete from HANGFIRE_JOBQUEUE; 
    delete from HANGFIRE_JOB;
end;");

                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { queue = "default", invocationData = " ", arguments = " " },
                        new { queue = "default", invocationData = " ", arguments = " " }
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.Query<DateTime?>(
                    "select FetchedAt from HANGFIRE_JOBQUEUE where JobId != :id",
                    new { id = payload.JobId }).Single();

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            const string arrangeSql = @"declare
    jobid number(11);    
begin 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt) values (:invocationData, :arguments, sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (jobid, :queue);
end;";

            _storage.UseConnection(connection =>
            {
                connection.Execute(@"begin 
    delete from HANGFIRE_JOBQUEUE; 
    delete from HANGFIRE_JOB;
end;");
                var queue = CreateJobQueue(connection);

                connection.Execute(
                    arrangeSql,
                    new { queue = "critical", invocationData = " ", arguments = " " });

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            const string arrangeSql = @"
declare
    jobid number(11);    
begin 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt) values (:invocationData, :arguments, sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBQUEUE (JobId, Queue) values (jobid, :queue);
end;";

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { queue = "default", invocationData = " ", arguments = " " },
                        new { queue = "critical", invocationData = " ", arguments = " " }
                    });

                var queue = CreateJobQueue(connection);

                var critical = (OracleFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (OracleFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            _storage.UseConnection(connection =>
            {
                connection.Execute("delete from HANGFIRE_JOBQUEUE");

                var queue = CreateJobQueue(connection);

                queue.Enqueue(connection, "default", "1");

                var record = connection.Query("select jq.ID \"Id\", jq.JOBID \"JobId\", jq.QUEUE \"Queue\", jq.FETCHEDAT \"FetchedAt\", jq.FETCHTOKEN \"FetchToken\" from HANGFIRE_JOBQUEUE jq").Single();
                Assert.Equal("1", record.JobId.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        public static void Sample(string arg1, string arg2) { }

        private static OracleJobQueue CreateJobQueue(OracleConnection connection)
        {
            var storage = new OracleStorage(connection);
            return new OracleJobQueue(storage, new OracleStorageOptions());
        }
    }
}
