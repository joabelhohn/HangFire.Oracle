using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Oracle.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Oracle
{
    public class OracleStorageConnection : JobStorageConnection
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly OracleStorage _storage;
        public OracleStorageConnection(OracleStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new OracleWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new OracleDistributedLock(_storage, resource, timeout).Acquire();
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException("job");
            if (parameters == null) throw new ArgumentNullException("parameters");

            var invocationData = InvocationData.Serialize(job);

            Logger.TraceFormat("CreateExpiredJob={0}", JobHelper.ToJson(invocationData));

            return _storage.UseConnection(connection =>
            {
                var param = new DynamicParameters();
                param.Add("invocationData", JobHelper.ToJson(invocationData), direction: System.Data.ParameterDirection.Input);
                param.Add("arguments", invocationData.Arguments, direction: System.Data.ParameterDirection.Input);
                param.Add("createdAt", createdAt, direction: System.Data.ParameterDirection.Input);
                param.Add("expireAt", createdAt.Add(expireIn), direction: System.Data.ParameterDirection.Input);
                param.Add("joiId", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Input);

                connection.Execute(
                    "insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, ExpireAt) " +
                    "values (:invocationData, :arguments, :createdAt, :expireAt) returning ID into :jobId",
                    param);

                var jobId = param.Get<int>("jobid").ToString();
                //string jobId = id.ToString();

                if (parameters.Count > 0)
                {
                    var parameterArray = new object[parameters.Count];
                    int parameterIndex = 0;
                    foreach (var parameter in parameters)
                    {
                        parameterArray[parameterIndex++] = new
                        {
                            jobId = jobId,
                            name = parameter.Key,
                            value = parameter.Value
                        };
                    }

                    connection.Execute(
                        "insert into HANGFIRE_JOBParameter (JobId, Name, Value) values (:jobId, :name, :value)", 
                        parameterArray);
                }

                return jobId;
            });
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException("queues");

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(String.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    String.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into HANGFIRE_JOBParameter (JobId, Name, Value) " +
                    "value (:jobId, :name, :value) " +
                    "on duplicate key update Value = :value ",
                    new { jobId = id, name, value });
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            return _storage.UseConnection(connection => 
                connection.Query<string>(
                    "select Value " +
                    "from HANGFIRE_JOBParameter " +
                    "where JobId = :id and Name = :name",
                    new {id = id, name = name}).SingleOrDefault());
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            return _storage.UseConnection(connection =>
            {
                var jobData = 
                    connection
                        .Query<SqlJob>(
                            "select InvocationData, StateName, Arguments, CreatedAt " +
                            "from HANGFIRE_JOB " +
                            "where Id = :id", 
                            new {id = jobId})
                        .SingleOrDefault();

                if (jobData == null) return null;

                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            return _storage.UseConnection(connection =>
            {
                var sqlState = 
                    connection.Query<SqlState>(
                        "select s.Name, s.Reason, s.Data " +
                        "from HANGFIRE_STATE s inner join HANGFIRE_JOB j on j.StateId = s.Id " +
                        "where j.Id = :jobId", 
                        new { jobId = jobId }).SingleOrDefault();
                if (sqlState == null)
                {
                    return null;
                }

                var data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                    StringComparer.OrdinalIgnoreCase);

                return new StateData
                {
                    Name = sqlState.Name,
                    Reason = sqlState.Reason,
                    Data = data
                };
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "INSERT INTO HANGFIRE_Server (Id, Data, LastHeartbeat) " +
                    "VALUE (:id, :data, :heartbeat) " +
                    "ON DUPLICATE KEY UPDATE Data = :data, LastHeartbeat = :heartbeat",
                    new
                    {
                        id = serverId,
                        data = JobHelper.ToJson(new ServerData
                        {
                            WorkerCount = context.WorkerCount,
                            Queues = context.Queues,
                            StartedAt = DateTime.UtcNow,
                        }),
                        heartbeat = DateTime.UtcNow
                    });
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "delete from HANGFIRE_Server where Id = :id",
                    new { id = serverId });
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "update HANGFIRE_Server set LastHeartbeat = @now where Id = :id",
                    new { now = DateTime.UtcNow, id = serverId });
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return
                _storage.UseConnection(connection =>
                    connection.Execute(
                        "delete from HANGFIRE_Server where LastHeartbeat < @timeOutAt",
                        new {timeOutAt = DateTime.UtcNow.Add(timeOut.Negate())}));
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return
                _storage.UseConnection(connection =>
                    connection.Query<int>(
                        "select count(Key) from HANGFIRE_SET where Key = :key",
                        new {key = key}).First());
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");
            
            return _storage.UseConnection(connection =>
                connection
                    .Query<string>(@"
select Value 
from (
	    select Value, @rownum := @rownum + 1 AS rank
	    from HANGFIRE_SET,
            (select @rownum := 0) r 
        where Key = :key
        order by Id
     ) ranked
where ranked.rank between @startingFrom and @endingAt",
                        new {key = key, startingFrom = startingFrom + 1, endingAt = endingAt + 1})
                    .ToList());
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return
                _storage.UseConnection(connection =>
                {
                    var result = connection.Query<string>(
                        "select Value from HANGFIRE_SET where Key = :key",
                        new {key});

                    return new HashSet<string>(result);
                });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (toScore < fromScore) 
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return
                _storage.UseConnection(connection =>
                    connection.Query<string>(
                        "select Value " +
                        "from HANGFIRE_SET " +
                        "where Key = :key and Score between :from and :to " +
                        "order by Score " +
                        "limit 1",
                        new {key, from = fromScore, to = toScore})
                        .SingleOrDefault());
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
select sum(s.Value) from (select sum(Value) as Value from HANGFIRE_COUNTER
where Key = :key
union all
select Value from HANGFIRE_AGGREGATEDCOUNTER
where Key = :key) as s";

            return 
                _storage
                    .UseConnection(connection =>
                        connection.Query<long?>(query, new { key = key }).Single() ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<long>(
                            "select count(Id) from HANGFIRE_Hash where Key = :key", 
                            new { key = key }).Single());
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<DateTime?>(
                        "select min(ExpireAt) from HANGFIRE_Hash where Key = :key", 
                        new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<long>(
                            "select count(Id) from HANGFIRE_List where Key = :key", 
                            new { key = key }).Single());
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<DateTime?>(
                        "select min(ExpireAt) from HANGFIRE_List where Key = :key", 
                        new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (name == null) throw new ArgumentNullException("name");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<string>(
                            "select Value from HANGFIRE_HASH where Key = :key and Field = :field", 
                            new { key = key, field = name }).SingleOrDefault());
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
Select x.VALUE from (select VALUE, rownum R from HANGFIRE_List
where Key = :key
order by Id desc) X
where X.R between :startingFrom and :endingAt
";

            return
                _storage
                    .UseConnection(connection =>
                        connection.Query<string>(
                            query,
                            new {key = key, startingFrom = startingFrom + 1, endingAt = endingAt + 1})
                            .ToList());
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
select Value from HANGFIRE_List
where Key = :key
order by Id desc";

            return _storage.UseConnection(connection => connection.Query<string>(query, new { key = key }).ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection
                        .Query<DateTime?>(
                            "select min(ExpireAt) from HANGFIRE_SET where Key = :key", 
                            new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            _storage.UseTransaction(connection =>
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    connection.Execute(@"
begin
	update HANGFIRE_Hash set VALUE = :value where KEY = :key and FIELD = :field;
	
	if sql%rowcount = 0 then
		insert into HANGFIRE_Hash (KEY, FIELD, VALUE) values (:key, :field, :value) 
	end if;
end;
", 
                        new { key = key, field = keyValuePair.Key, value = keyValuePair.Value });
                }
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<SqlHash>(
                    "select Field, Value from HANGFIRE_Hash where Key = :key",
                    new {key})
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            });
        }
    }
}