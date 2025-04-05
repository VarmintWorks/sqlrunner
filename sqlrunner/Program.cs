using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

public class DatabaseInfo
{
    public required string Server { get; set; }
    public required string Database { get; set; }
    public required string User { get; set; }
    public required string Password { get; set; }
}

public class SqlExecutor
{
    private readonly int _maxThreads;
    private readonly string _sqlQuery;
    private readonly List<DatabaseInfo> _databases;
    private readonly ConcurrentQueue<List<Dictionary<string, object>>> _resultsQueue = new();
    private readonly string _outputFilePath = "output.csv";
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly ConcurrentBag<string> _runningTasks = new();
    private int _completedCount = 0;

    public SqlExecutor(int maxThreads, string queryFilePath, string csvFilePath)
    {
        _maxThreads = maxThreads;
        _sqlQuery = File.ReadAllText(queryFilePath);
        _databases = ReadCsv(csvFilePath);
    }

    private List<DatabaseInfo> ReadCsv(string csvFilePath)
    {
        using var reader = new StreamReader(csvFilePath);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
        return csv.GetRecords<DatabaseInfo>().ToList();
    }

    public async Task ExecuteQueriesAsync()
    {
        int totalDatabases = _databases.Count;
        var semaphore = new SemaphoreSlim(_maxThreads);
        var tasks = new List<Task>();

        // Start a dedicated writer task
        var writerTask = Task.Run(() => WriteToCsvAsync(_cancellationTokenSource.Token));

        foreach (var dbInfo in _databases)
        {
            await semaphore.WaitAsync();

            var dbIdentifier = $"{dbInfo.Server}/{dbInfo.Database}";
            _runningTasks.Add(dbIdentifier);
            PrintProgress(totalDatabases);

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var result = await ExecuteSqlQueryAsync(dbInfo.Server, dbInfo.Database, dbInfo.User, dbInfo.Password);
                    if (result.Count > 0)
                    {
                        _resultsQueue.Enqueue(result); // Add results to queue
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error executing query for {dbIdentifier}: {ex.Message}");
                }
                finally
                {
                    _runningTasks.TryTake(out dbIdentifier);
                    Interlocked.Increment(ref _completedCount);
                    PrintProgress(totalDatabases);
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Stop the writer task after all queries are done
        _cancellationTokenSource.Cancel();
        await writerTask;
    }

    private async Task<List<Dictionary<string, object>>> ExecuteSqlQueryAsync(string server, string database, string user, string password)
    {
        var connectionString = $"Server={server};Database={database};User ID={user};Password={password};Integrated Security=False;TrustServerCertificate=True;";
        var results = new List<Dictionary<string, object>>();

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand(_sqlQuery, connection);
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader[i];
            }
            results.Add(row);
        }

        return results;
    }

    private async Task WriteToCsvAsync(CancellationToken cancellationToken)
    {
        using var writer = new StreamWriter(_outputFilePath);
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));

        bool headerWritten = false;

        while (!cancellationToken.IsCancellationRequested || !_resultsQueue.IsEmpty)
        {
            while (_resultsQueue.TryDequeue(out var result))
            {
                if (!headerWritten)
                {
                    var headers = result.FirstOrDefault()?.Keys.ToList();
                    if (headers != null)
                    {
                        foreach (var header in headers)
                        {
                            csv.WriteField(header);
                        }
                        csv.NextRecord();
                        headerWritten = true;
                    }
                }

                foreach (var row in result)
                {
                    foreach (var value in row.Values)
                    {
                        csv.WriteField(value);
                    }
                    csv.NextRecord();
                }

                await writer.FlushAsync();
            }

            await Task.Delay(100); // Avoid busy-waiting
        }
    }

    private void PrintProgress(int totalDatabases)
    {
        Console.Clear();
        Console.WriteLine($"Processed: {_completedCount}/{totalDatabases} databases");
        Console.WriteLine("Currently running tasks:");
        foreach (var task in _runningTasks)
        {
            Console.WriteLine($"  - {task}");
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: <MaxThreads> <QueryFilePath> <CsvFilePath>");
            return;
        }

        int maxThreads = int.Parse(args[0]);
        string queryFilePath = args[1];
        string csvFilePath = args[2];

        var executor = new SqlExecutor(maxThreads, queryFilePath, csvFilePath);
        await executor.ExecuteQueriesAsync();

        Console.WriteLine("Queries executed and results saved to output.csv.");
    }
}
