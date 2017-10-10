using System;
using System.Collections.Generic;
using System.Transactions;
using Dapper;
using Hangfire.Oracle.JobQueue;
using Hangfire.Oracle.Monitoring;
using Hangfire.Storage.Monitoring;
using Moq;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests.Monitoring
{
    public class OracleMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly OracleMonitoringApi _sut;
        private readonly OracleStorage _storage;
        private readonly int? _jobListLimit = 1000;
        private readonly OracleConnection _connection;
        private readonly string _invocationData =
            "{\"Type\":\"System.Console, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089\"," +
            "\"Method\":\"WriteLine\"," +
            "\"ParameterTypes\":\"[\\\"System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089\\\"]\"," +
            "\"Arguments\":\"[\\\"\\\"test\\\"\\\"]\"}";
        private readonly string _arguments = "[\"test\"]";
        private readonly DateTime _createdAt = DateTime.UtcNow;
        private readonly DateTime _expireAt = DateTime.UtcNow.AddMinutes(1);

        public OracleMonitoringApiTests()
        {
            _connection = new OracleConnection(ConnectionUtils.GetConnectionString());
            _connection.Open();

            var persistentJobQueueMonitoringApiMock = new Mock<IPersistentJobQueueMonitoringApi>();
            persistentJobQueueMonitoringApiMock.Setup(m => m.GetQueues()).Returns(new[] { "default" });

            var defaultProviderMock = new Mock<IPersistentJobQueueProvider>();
            defaultProviderMock.Setup(m => m.GetJobQueueMonitoringApi())
                .Returns(persistentJobQueueMonitoringApiMock.Object);

            var OracleStorageMock = new Mock<OracleStorage>(_connection);
            OracleStorageMock
                .Setup(m => m.QueueProviders)
                .Returns(new PersistentJobQueueProviderCollection(defaultProviderMock.Object));

            _storage = OracleStorageMock.Object;
            _sut = new OracleMonitoringApi(_storage, _jobListLimit);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnEnqueuedCount()
        {
            const int expectedEnqueuedCount = 1;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"
insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Enqueued')");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedEnqueuedCount, result.Enqueued);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnFailedCount()
        {
            const int expectedFailedCount = 2;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"
begin
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Failed'); 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Failed');
end;");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedFailedCount, result.Failed);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnProcessingCount()
        {
            const int expectedProcessingCount = 1;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"
insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Processing')");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedProcessingCount, result.Processing);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnScheduledCount()
        {
            const int expectedScheduledCount = 3;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"
begin
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Scheduled'); 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Scheduled'); 
    insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, StateName) values (' ', ' ', sysdate,'Scheduled'); 
end;");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedScheduledCount, result.Scheduled);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnQueuesCount()
        {
            const int expectedQueuesCount = 1;

            var result = _sut.GetStatistics();

            Assert.Equal(expectedQueuesCount, result.Queues);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnServersCount()
        {
            const int expectedServersCount = 2;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"begin
insert into HANGFIRE_SERVER (Id, Data) values (1,'1'); 
insert into HANGFIRE_SERVER (Id, Data) values (2,'2'); 
end;");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedServersCount, result.Servers);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnSucceededCount()
        {
            const int expectedStatsSucceededCount = 11;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"begin
insert into HANGFIRE_COUNTER (KEY,VALUE)  values ('stats:succeeded',1);
insert into HANGFIRE_AGGREGATEDCOUNTER (KEY,VALUE) values ('stats:succeeded',10);
end;");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedStatsSucceededCount, result.Succeeded);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnDeletedCount()
        {
            const int expectedStatsDeletedCount = 7;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"begin
    INSERT INTO HANGFIRE_AGGREGATEDCOUNTER (KEY,VALUE) values ('stats:deleted',5); 
    insert into HANGFIRE_COUNTER (KEY,VALUE) values ('stats:deleted',1);
    insert into HANGFIRE_COUNTER (KEY,VALUE) values ('stats:deleted',1); 
end;");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedStatsDeletedCount, result.Deleted);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnRecurringCount()
        {
            const int expectedRecurringCount = 1;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(@"insert into HANGFIRE_SET (ID, KEY, VALUE, SCORE) values (1, 'recurring-jobs', 'test', 0)");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedRecurringCount, result.Recurring);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnJob()
        {
            JobDetailsDto result = null;
            _storage.UseConnection(connection =>
            {

                var param = new DynamicParameters();
                param.Add(name: "createdAt", value: _createdAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "invocationData", value: _invocationData, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "arguments", value: _arguments, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "expireAt", value: _expireAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

                connection.Execute(@"
declare
    jobid number(11);
begin
  insert into HANGFIRE_JOB (CreatedAt,InvocationData,Arguments,ExpireAt) values (:createdAt, :invocationData, :arguments, :expireAt) returning ID into jobid;
  :jobid := jobid;
end;", param);
                var jobId = param.Get<int>("jobid");
                result = _sut.JobDetails(jobId.ToString());
            });

            Assert.NotNull(result.Job);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnCreatedAtAndExpireAt()
        {
            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var param = new DynamicParameters();
                param.Add(name: "createdAt", value: _createdAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "invocationData", value: _invocationData, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "arguments", value: _arguments, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "expireAt", value: _expireAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

                connection.Execute(@"
declare
    jobid number(11);
begin
  insert into HANGFIRE_JOB (CreatedAt,InvocationData,Arguments,ExpireAt) values (:createdAt, :invocationData, :arguments, :expireAt) returning ID into jobid;
  :jobid := jobid;
end;", param);
                var jobId = param.Get<int>("jobid");
                result = _sut.JobDetails(jobId.ToString());
            });

            Assert.Equal(_createdAt.ToString("yyyy-MM-dd hh:mm:ss"), result.CreatedAt.Value.ToString("yyyy-MM-dd hh:mm:ss"));
            Assert.Equal(_expireAt.ToString("yyyy-MM-dd hh:mm:ss"), result.ExpireAt.Value.ToString("yyyy-MM-dd hh:mm:ss"));
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnProperties()
        {
            var properties = new Dictionary<string, string>();
            properties["CurrentUICulture"] = "en-US";
            properties["CurrentCulture"] = "lt-LT";

            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var param = new DynamicParameters();
                param.Add(name: "createdAt", value: _createdAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "invocationData", value: _invocationData, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "arguments", value: _arguments, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "expireAt", value: _expireAt, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

                connection.Execute(@"
declare 
  jobid number(11);
begin
  insert into HANGFIRE_JOB (CreatedAt,InvocationData,Arguments,ExpireAt) values (:createdAt, :invocationData, :arguments, :expireAt) returning ID into jobid; 
  insert into HANGFIRE_JOBParameter (JobId, Name, Value) values (jobId, 'CurrentUICulture', 'en-US');
  insert into HANGFIRE_JOBParameter (JobId, Name, Value) values (jobId, 'CurrentCulture', 'lt-LT');
  :jobid := jobid;
end;", param);
                var jobId = param.Get<int>("jobid");
                result = _sut.JobDetails(jobId.ToString());
            });

            Assert.Equal(properties, result.Properties);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnHistory()
        {
            const string jobStateName = "Scheduled";
            const string stateData = "{\"EnqueueAt\":\"2016-02-21T11:56:05.0561988Z\", \"ScheduledAt\":\"2016-02-21T11:55:50.0561988Z\"}";

            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {

                var param = new DynamicParameters();
                param.Add("createdAt", _createdAt, direction: System.Data.ParameterDirection.Input);
                param.Add("invocationData", _invocationData, direction: System.Data.ParameterDirection.Input);
                param.Add("arguments", _arguments, direction: System.Data.ParameterDirection.Input);
                param.Add("expireAt", _expireAt, direction: System.Data.ParameterDirection.Input);
                param.Add("jobStateName", jobStateName, direction: System.Data.ParameterDirection.Input);
                param.Add("stateData", stateData, direction: System.Data.ParameterDirection.Input);
                param.Add(name: "jobid", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

                connection.Execute(@"
declare
    jobid number(11);
begin
    insert into HANGFIRE_JOB (CreatedAt,InvocationData,Arguments,ExpireAt) values (:createdAt, :invocationData, :arguments, :expireAt) returning ID into jobid;
    insert into HANGFIRE_STATE (JobId, Name, CreatedAt, Data) values (jobId, :jobStateName, :createdAt, :stateData);
    :jobId := jobid;
end;", param);

                var jobId = param.Get<int>("jobid");

                result = _sut.JobDetails(jobId.ToString());
            });

            Assert.Equal(1, result.History.Count);
        }
    }
}
