﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Oracle.JobQueue;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests
{
    public class OracleStorageConnectionTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly PersistentJobQueueProviderCollection _providers;

        public OracleStorageConnectionTests()
        {
            _queue = new Mock<IPersistentJobQueue>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue())
                .Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleStorageConnection(null));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var anotherProvider = new Mock<IPersistentJobQueueProvider>();
                _providers.Add(anotherProvider.Object, new[] { "critical" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "critical", "default" }, token));
            });
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var transaction = connection.CreateWriteTransaction();
                Assert.NotNull(transaction);
            });
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Job.FromExpression(() => SampleMethod("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnections((sql, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var sqlJob = sql.Query("select * from HANGFIRE_JOB").Single();
                Assert.Equal(jobId, sqlJob.ID.ToString());
                Assert.Equal(createdAt, sqlJob.CREATEDAT);
                Assert.Equal(null, (int?)sqlJob.STATEID);
                Assert.Equal(null, (string)sqlJob.STATENAME);

                var invocationData = JobHelper.FromJson<InvocationData>((string)sqlJob.INVOCATIONDATA);
                invocationData.Arguments = sqlJob.ARGUMENTS;

                var job = invocationData.Deserialize();
                Assert.Equal(typeof(OracleStorageConnectionTests), job.Type);
                Assert.Equal("SampleMethod", job.Method.Name);
                Assert.Equal("\"Hello\"", job.Arguments[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < sqlJob.EXPIREAT);
                Assert.True(sqlJob.EXPIREAT < createdAt.AddDays(1).AddMinutes(1));

                var parameters = sql.Query(
                    "select * from HANGFIRE_JOBPARAMETER where JobId = :id",
                    new { id = jobId })
                    .ToDictionary(x => (string)x.NAME, x => (string)x.VALUE);

                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobData(null)));
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection(connection =>
            {
                var result = connection.GetJobData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, STATENAME, CREATEDAT) values (:invocationData, :arguments, :stateName, sysdate) returning ID into :jobid";

            UseConnections((sql, connection) =>
            {
                var job = Job.FromExpression(() => SampleMethod("wrong"));


                var param = new DynamicParameters();
                param.Add(name: "invocationData", value: JobHelper.ToJson(InvocationData.Serialize(job)), direction: System.Data.ParameterDirection.Input);
                param.Add(name: "stateName", value: "Succeeded", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "arguments", value: "['Arguments']", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);
                var id = param.Get<int>("jobid");
                string jobId = id.ToString();
                
                var result = connection.GetJobData(jobId);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsCorrectData()
        {
            const string arrangeSql = @"
declare
    jobid number(11);
    stateId number(11);
begin
    insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, STATENAME, CREATEDAT) values (' ', ' ', ' ', sysdate) returning ID into jobid;
    insert into HANGFIRE_STATE (JobId, Name, CreatedAt) values (jobid, 'old-state', sysdate);
    insert into HANGFIRE_STATE (JobId, Name, Reason, Data, CreatedAt) values (jobid, :name, :reason, :data, sysdate) returning ID into stateId;
    update HANGFIRE_JOB set StateId = stateId;
    :jobid := jobid;
end;";

            UseConnections((sql, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "Key", "Value" }
                };

                var param = new DynamicParameters();
                param.Add("name", "Name", direction: System.Data.ParameterDirection.Input);
                param.Add("reason", "Reason", direction: System.Data.ParameterDirection.Input);
                param.Add("data", JobHelper.ToJson(data), direction: System.Data.ParameterDirection.Input);
                param.Add("jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);

                var jobId = param.Get<int>("jobid");
                
                var result = connection.GetStateData(jobId.ToString());
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsCorrectData_WhenPropertiesAreCamelcased()
        {
            const string arrangeSql = @"
declare
    jobid number(11);
    stateId number(11);
begin
    insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, STATENAME, CREATEDAT) values (' ', ' ', ' ', sysdate) returning Id into jobid;
    insert into HANGFIRE_STATE (JobId, Name, CreatedAt) values (jobId, 'old-state', sysdate);
    insert into HANGFIRE_STATE (JobId, Name, Reason, Data, CreatedAt) values (jobId, :name, :reason, :data, sysdate) returning ID into stateId;
    update HANGFIRE_JOB set StateId = stateId;
    :jobid := jobid;
end;";

            UseConnections((sql, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "key", "Value" }
                };
                var param = new DynamicParameters();
                param.Add("name", "Name", direction: System.Data.ParameterDirection.Input);
                param.Add("reason", "Reason", direction: System.Data.ParameterDirection.Input);
                param.Add("data", JobHelper.ToJson(data), direction: System.Data.ParameterDirection.Input);
                param.Add("jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);

                var jobId = param.Get<int>("jobid");

                var result = connection.GetStateData(jobId.ToString());
                Assert.NotNull(result);

                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, STATENAME, CREATEDAT) values (:invocationData, :arguments, :stateName, sysdate) returning ID into :jobid";

            UseConnections((sql, connection) =>
            {
                var param = new DynamicParameters();
                param.Add(name: "invocationData", value: JobHelper.ToJson(new InvocationData(null, null, null, null)), direction: System.Data.ParameterDirection.Input);
                param.Add(name: "stateName", value: "Succeeded", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "arguments", value: "['Arguments']", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);
                var id = param.Get<int>("jobid");
                string jobId = id.ToString();
                
                var result = connection.GetJobData(jobId);

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("1", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, CREATEDAT) values (' ', ' ', sysdate) returning ID into :jobid";

            UseConnections((sql, connection) =>
            {
                var param = new DynamicParameters();
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);
                var id = param.Get<int>("jobid");
                string jobId = id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = sql.Query(
                    "select * from HANGFIRE_JOBParameter where JobId = :id and Name = :name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("Value", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, CREATEDAT) values (' ', ' ', sysdate) returning ID into :jobid";

            UseConnections((sql, connection) =>
            {
                var param = new DynamicParameters();
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);
                var id = param.Get<int>("jobid");
                string jobId = id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = sql.Query(
                    "select * from HANGFIRE_JOBParameter where JobId = :id and Name = :name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("AnotherValue", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            const string arrangeSql = @"insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, CREATEDAT) values (' ', ' ', sysdate) returning ID into :jobid";

            UseConnections((sql, connection) =>
            {

                var param = new DynamicParameters();
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql, param);
                var id = param.Get<int>("jobid");
                string jobId = id.ToString();

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = sql.Query(
                    "select * from HANGFIRE_JOBParameter where JobId = :id and Name = :name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal((string)null, parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("1", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection(connection =>
            {
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            const string arrangeSql = @"
declare
    jobid number(11);
begin 
    insert into HANGFIRE_JOB (INVOCATIONDATA, ARGUMENTS, CREATEDAT) values (' ', ' ', sysdate) returning ID into jobid;
    insert into HANGFIRE_JOBPARAMETER (JOBID, NAME, VALUE) values (jobid, :name, :value);
    :jobid := jobid;
end;";

            UseConnections((sql, connection) =>
            {
                var param = new DynamicParameters();
                param.Add(name: "name", value: "name", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "value", value: "value", direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);
                sql.Execute(arrangeSql,param);
                var id = param.Get<int>("jobid");
                var value = connection.GetJobParameter(id.ToString(), "name");

                Assert.Equal("value", value);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            const string arrangeSql = @"
begin
    insert into HANGFIRE_SET (KEY, SCORE, VALUE) values ('key', 1.0, '1.0');
    insert into HANGFIRE_SET (KEY, SCORE, VALUE) values ('key', -1.0, '-1.0');
    insert into HANGFIRE_SET (KEY, SCORE, VALUE) values ('key', -5.0, '-5.0');
    insert into HANGFIRE_SET (KEY, SCORE, VALUE) values ('another-key', -2.0, '-2.0');
end;";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnections((sql, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = sql.Query("select * from HANGFIRE_SERVER").Single();
                Assert.Equal("server", server.ID);
                Assert.True(((string)server.DATA).StartsWith(
                    "{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":"),
                    server.Data);
                Assert.NotNull(server.LASTHEARTBEAT);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = sql.Query("select * from HANGFIRE_SERVER").Single();
                Assert.Equal("server", sameServer.ID);
                Assert.Contains("1000", sameServer.DATA);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null)));
        }

        [Fact, CleanDatabase]
        public void RemoveServer_RemovesAServerRecord()
        {
            const string arrangeSql = @"
begin
  insert into HANGFIRE_SERVER (ID, DATA, LASTHEARTBEAT) values ('Server1', ' ', sysdate);
  insert into HANGFIRE_SERVER (ID, DATA, LASTHEARTBEAT) values ('Server2', ' ', sysdate);
end;";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.RemoveServer("Server1");

                var server = sql.Query("select * from HANGFIRE_SERVER").Single();
                Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
            });
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null)));
        }

        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            const string arrangeSql = @"
begin
  insert into HANGFIRE_SERVER (ID, DATA, LASTHEARTBEAT) values ('server1', ' ', to_timestamp('2012-12-12 12:12:12',  'yyyy-MM-dd HH24:MI:SS'));
  insert into HANGFIRE_SERVER (ID, DATA, LASTHEARTBEAT) values ('server2', ' ', to_timestamp('2012-12-12 12:12:12', 'yyyy-MM-dd HH24:MI:SS'));
end;";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.Heartbeat("server1");

                var servers = sql.Query("select * from HANGFIRE_SERVER")
                    .ToDictionary(x => (string)x.ID, x => (DateTime)x.LASTHEARTBEAT);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            const string arrangeSql = @"insert into HANGFIRE_SERVER (ID, DATA, LASTHEARTBEAT) values (:id, ' ', :heartbeat)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { id = "server1", heartbeat = DateTime.UtcNow.AddDays(-1) },
                        new { id = "server2", heartbeat = DateTime.UtcNow.AddHours(-12) }
                    });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = sql.Query("select * from HANGFIRE_SERVER").Single();
                Assert.Equal("server2", liveServer.ID);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Equal(0, result.Count);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            const string arrangeSql = @"insert into HANGFIRE_SET (KEY, VALUE, SCORE) values (:Key, :Value, 0.0)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-set", value = "1" },
                    new { key = "some-set", value = "2" },
                    new { key = "another-set", value = "3" }
                });

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnections((sql, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = sql.Query(
                    "select * from HANGFIRE_HASH where Key = :key",
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.FIELD, x => (string)x.VALUE);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            const string arrangeSql = @"insert into HANGFIRE_HASH (KEY, FIELD, VALUE) values (:Key, :field, :Value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-hash", field = "Key1", value = "Value1" },
                    new { key = "some-hash", field = "Key2", value = "Value2" },
                    new { key = "another-hash", field = "Key3", value = "Value3" }
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            const string arrangeSql = @"insert into HANGFIRE_SET (KEY, VALUE, SCORE) values (:Key, :Value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new { key = "set-1", value = "value-1" },
                    new { key = "set-2", value = "value-1" },
                    new { key = "set-1", value = "value-2" }
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            const string arrangeSql = @"insert into HANGFIRE_SET (KEY, VALUE, SCORE) values (:Key, :Value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new { Key = "set-1", Value = "1" },
                    new { Key = "set-1", Value = "2" },
                    new { Key = "set-1", Value = "3" },
                    new { Key = "set-1", Value = "4" },
                    new { Key = "set-2", Value = "4" },
                    new { Key = "set-1", Value = "5" }
                });

                var result = connection.GetRangeFromSet("set-1", 2, 3);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElements2()
        {
            const string arrangeSql = @"insert into HANGFIRE_SET (KEY, VALUE, SCORE) values (:Key, :Value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new { Key = "set-1", Value = "1" },
                    new { Key = "set-1", Value = "2" },
                    new { Key = "set-0", Value = "3" },
                    new { Key = "set-1", Value = "4" },
                    new { Key = "set-2", Value = "1" },
                    new { Key = "set-1", Value = "5" },
                    new { Key = "set-2", Value = "2" },
                    new { Key = "set-1", Value = "3" },
                });

                var result = connection.GetRangeFromSet("set-1", 0, 4);

                Assert.Equal(new[] { "1", "2", "4", "5", "3" }, result);
            });
        }


        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            const string arrangeSql = @"insert into HANGFIRE_COUNTER (KEY, VALUE) values (:key, :value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "counter-1", value = 1 },
                    new { key = "counter-2", value = 1 },
                    new { key = "counter-1", value = 1 }
                });

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_IncludesValues_FromCounterAggregateTable()
        {
            const string arrangeSql = @"insert into HANGFIRE_AGGREGATEDCOUNTER (KEY, VALUE) values (:key, :value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "counter-1", value = 12 },
                    new { key = "counter-2", value = 15 }
                });

                // Act
                var result = connection.GetCounter("counter-1");

                Assert.Equal(12, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            const string arrangeSql = @"insert into HANGFIRE_HASH (KEY, FIELD) values (:key, :field)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field-1" },
                    new { key = "hash-1", field = "field-2" },
                    new { key = "hash-2", field = "field-1" }
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            const string arrangeSql = @"insert into HANGFIRE_HASH (KEY, FIELD, EXPIREAT) values (:key, :field, :expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field", expireAt = (DateTime?)DateTime.UtcNow.AddHours(1) },
                    new { key = "hash-2", field = "field", expireAt = (DateTime?) null }
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            const string arrangeSql = @"insert into HANGFIRE_LIST (KEY) values (:key)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1" },
                    new { key = "list-1" },
                    new { key = "list-2" }
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            const string arrangeSql = @"insert into HANGFIRE_LIST (KEY, EXPIREAT) values (:key, :expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", expireAt = (DateTime?) DateTime.UtcNow.AddHours(1) },
                    new { key = "list-2", expireAt = (DateTime?) null }
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            const string arrangeSql = @"insert into HANGFIRE_HASH (KEY, FIELD, VALUE) values (:key, :field, :value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field-1", value = "1" },
                    new { key = "hash-1", field = "field-2", value = "2" },
                    new { key = "hash-2", field = "field-1", value = "3" }
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            const string arrangeSql = @"insert into HANGFIRE_LIST (KEY, VALUE) values (:key, :value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "2" },
                    new { key = "list-1", value = "3" },
                    new { key = "list-1", value = "4" },
                    new { key = "list-1", value = "5" }
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "4", "3" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            const string arrangeSql = @"insert into HANGFIRE_LIST (KEY, VALUE) values (:key, :value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "2" },
                    new { key = "list-1", value = "3" }
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "3", "1" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            const string arrangeSql = @"insert into HANGFIRE_SET (KEY, VALUE, EXPIREAT, SCORE) values (:key, :value, :expireAt, 0.0)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1", expireAt = (DateTime?) DateTime.UtcNow.AddMinutes(60) },
                    new { key = "set-2", value = "2", expireAt = (DateTime?) null }
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        private void UseConnections(Action<OracleConnection, OracleStorageConnection> action)
        {
            using (var sqlConnection = ConnectionUtils.CreateConnection())
            {
                var storage = new OracleStorage(sqlConnection);
                using (var connection = new OracleStorageConnection(storage))
                {
                    action(sqlConnection, connection);
                }
            }
        }

        private void UseConnection(Action<OracleStorageConnection> action)
        {
            using (var sql = ConnectionUtils.CreateConnection())
            {
                var storage = new Mock<OracleStorage>(sql);
                storage.Setup(x => x.QueueProviders).Returns(_providers);

                using (var connection = new OracleStorageConnection(storage.Object))
                {
                    action(connection);
                }
            }
        }

        public static void SampleMethod(string arg) { }
    }
}
