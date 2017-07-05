using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace SqliteMemoryTest
{
    class Program
    {
        private static DataAccess _db;

        static void Main(string[] args)
        {
            Console.WriteLine("Start DB test");

            _db = new DataAccess();

            Parallel.ForEach(
                Enumerable.Range(1, 5), 
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = 5
                }, 
                i => Run(i));
        }

        static void Run(int threadId)
        {
            var random = new Random();
            try
            {
                while (true)
                {
                    var foo = new Foo
                    {
                        Id = Guid.NewGuid(),
                        AnInt = random.Next(1000),
                        AReal = (decimal) random.NextDouble(),
                        Comment = GenerateRandomText(),
                        Timestamp = DateTime.UtcNow,
                    };

                    _db.Insert(foo);
                    Log("Insert", threadId);

                    foo.Comment = GenerateRandomText();

                    _db.Update(foo);
                    Log("Update", threadId);

                    var items = _db.Select(DateTime.UtcNow.AddMinutes(-1)).ToList();
                    Log("Select", threadId);

                    if (random.Next(100) == 1)
                    {
                        _db.Delete(items.First().Id);
                        Log("Delete", threadId);
                    }
                }
            }
            catch (Exception err)
            {
                LogError(err.ToString(), threadId);
            }
        }

        private static string GenerateRandomText()
        {
            return 
                string.Join(string.Empty,
                    Enumerable.Range(1, 25)
                    .Select(i => Guid.NewGuid().ToString()));
        }

        private static void Log(string msg, int threadId)
        {
            Console.WriteLine($"{DateTime.Now} [{threadId}] - {msg}");
        }

        private static void LogError(string msg, int threadId)
        {
            Console.Error.WriteLine($"{DateTime.Now} [{threadId}] - {msg}");
        }
    }

    class Foo
    {
        public Guid Id;
        public int AnInt;
        public decimal AReal;
        public string Comment;
        public DateTime Timestamp;
    }

    class DataAccess
    {
        private const string ConnectionString = "Data Source=storage.db";

        public DataAccess()
        {
            CreateSchemaWhenMissing();
        }

        private IDbConnection GetOpenConnection()
        {
            var connection = new SqliteConnection(ConnectionString);
            connection.Open();

            return connection;
        }

        private void CreateSchemaWhenMissing()
        {
            using (var conn = GetOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"create table if not exists Foo(
                            Id blob not null
                        ,   AnInt integer not null
                        ,   AReal real not null
                        ,   Comment text not null
                        ,   Timestamp text not null
                        ,   primary key(Id));";

                cmd.ExecuteNonQuery();

                cmd.CommandText =
                    @"create index if not exists TimestampIndex on Foo(Timestamp asc);";

                cmd.ExecuteNonQuery();
            }
        }

        public void Insert(Foo item)
        {
            using (var conn = GetOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"insert into Foo
                        (Id, AnInt, AReal, Comment, Timestamp)
                    values
                        (@id, @anint, @areal, @comment, @timestamp)";

                cmd.AddWithValue("@id", item.Id.ToByteArray());
                cmd.AddWithValue("@anint", item.AnInt);
                cmd.AddWithValue("@areal", item.AReal);
                cmd.AddWithValue("@comment", item.Comment);
                cmd.AddWithValue("@timestamp", item.Timestamp);

                cmd.ExecuteNonQuery();
            }
        }

        public IEnumerable<Foo> Select(DateTime timestamp)
        {
            using (var conn = GetOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"select
                        *
                    from
                        Foo
                    where
                       Timestamp > @timestamp";

                cmd.AddWithValue("@timestamp", timestamp);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return new Foo
                        {
                            Id = reader.GetGuid(0),
                            AnInt = reader.GetInt32(1),
                            AReal = reader.GetDecimal(2),
                            Comment = reader.GetString(3),
                            Timestamp = reader.GetDateTime(4),
                        };
                    }
                }
                cmd.ExecuteNonQuery();
            }
        }

        public void Update(Foo item)
        {
            using (var conn = GetOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"update
                        Foo
                    set
                        AnInt = @anint
                    ,   AReal = @areal
                    ,   Comment = @comment
                    ,   Timestamp = @timestamp
                    where
                       Id = @id";

                cmd.AddWithValue("@id", item.Id.ToByteArray());
                cmd.AddWithValue("@anint", item.AnInt);
                cmd.AddWithValue("@areal", item.AReal);
                cmd.AddWithValue("@comment", item.Comment);
                cmd.AddWithValue("@timestamp", item.Timestamp);

                cmd.ExecuteNonQuery();
            }
        }

        public void Delete(Guid id)
        {
            using (var conn = GetOpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"delete from
                       Foo
                    where
                       Id = @id";

                cmd.AddWithValue("@id", id.ToByteArray());

                cmd.ExecuteNonQuery();
            }
        }
    }

    public static class DbCommandExtensions
    {
        public static void AddWithValue(this IDbCommand command, string name, object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }
    }
}
